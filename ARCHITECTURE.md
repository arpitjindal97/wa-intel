# wa-intel — System Architecture & Operations

> Personal WhatsApp intelligence pipeline. Ingests messages, downloads media, builds rich self-contained kanban tasks, and lets a kanban worker (Melissa, an LLM persona) handle summarization, memory updates, and notifications asynchronously. The core design principle is **dumb pipe**: wa-intel collects, stores, and enqueues work — the brain does everything cognitive on its own side.

**Owner**: Arpit Agarwal · **Host**: vmi3334628.contaboserver.net (Contabo VPS) · **Code**: `/root/wa-intel/wa-intel.py` (single-file Python daemon)

---

## Table of contents

1. [System diagram](#system-diagram)
2. [Components](#components)
3. [Data flows](#data-flows)
4. [Storage layout](#storage-layout)
5. [HTTP endpoints](#http-endpoints)
6. [CLI write-back scripts](#cli-write-back-scripts)
7. [Brain prompt structure](#brain-prompt-structure)
8. [Configuration (env vars)](#configuration-env-vars)
9. [Failure modes & recovery](#failure-modes--recovery)
10. [Operations runbook](#operations-runbook)
11. [Schema reference](#schema-reference)
12. [History of changes](#history-of-changes)
13. [Known gaps / future work](#known-gaps--future-work)

---

## System diagram

```
                    WhatsApp (mobile + servers)
                              │ Web protocol
                              ▼
         ┌────────────────────────────────────────┐
         │  whatsapp-ha (Docker, :3000)           │
         │  github.com/gajosu/ha-whatsapp-web-rest-api  │
         │  + PR #202 (sync-history endpoint)     │
         │  Headless Chromium running whatsapp-web.js │
         └──────────────┬───────────┬─────────────┘
            webhook events │           │ REST (poll, fetch, send, download-media)
                           ▼           │
         ┌─────────────────────────────┐│
         │ event_normaliser (Docker,   ││
         │ :11997 TLS, letsencrypt)    ││
         │ - normalize event shape     ││
         │ - HMAC-sign forwarded JSON  ││
         │ - source-aware routing      ││
         └──────────────┬──────────────┘│
                        │ /webhooks/whatsapp-events
                        ▼              ▼
         ┌─────────────────────────────────────────┐
         │  wa-intel daemon (systemd, :8090)       │
         │  /root/wa-intel/wa-intel.py             │
         │                                         │
         │  HTTP server (Handler):                 │
         │    POST /events, /webhooks/...           │
         │    POST /api/summarize/<chat_id>         │
         │    POST /api/sync/<chat_id>              │
         │    GET  /health, /api/chats              │
         │    GET/POST /api/wa/*  (proxy)           │
         │                                         │
         │  Background loops (daemon threads):     │
         │    _poll_loop (cadence 60s)             │
         │    _silence_check_loop (15s)            │
         │    _wa_health_loop (30s)                │
         │                                         │
         │  Storage:                               │
         │    /root/wa-intel/data/wa-intel.db      │
         │    /root/wa-intel/data/media/<chat>/... │
         │    /root/wa-intel/data/brain_log.jsonl  │
         └─────────┬───────────────────────────────┘
                   │  hermes kanban create
                   │  --skill wa-intel
                   │  --idempotency-key wa-intel:<chat>:<last_ts>:<source>
                   ▼
         ┌─────────────────────────────────────────┐
         │  Hermes kanban dispatcher                │
         │  /root/.hermes/kanban.db                 │
         │  Spawns one worker per ready task:       │
         │    hermes -p default --skills wa-intel  │
         │           chat -q work kanban task <id>  │
         └─────────────────┬───────────────────────┘
                           ▼
         ┌─────────────────────────────────────────┐
         │  Melissa (the kanban worker)            │
         │  Default profile loads:                 │
         │    SOUL.md (persona)                    │
         │    USER.md, MEMORY.md (auto memory)     │
         │    All default skills                    │
         │  Plus: wa-intel skill (this skill)      │
         │                                         │
         │  Reads task body (the prompt). Performs:│
         │    1. media one-liners                   │
         │    2. chat summary → wa_set_context.py  │
         │    3. relationship updates →            │
         │         wa_relationship.py {add-int.,   │
         │         upsert, set-tier, delete}       │
         │       hindsight retain (her own bank)   │
         │       MEMORY.md edits (rare)            │
         │    4. Telegram notification (if any)    │
         │    Marks kanban task complete           │
         └─────────────────────────────────────────┘
                           │ writes to ↓
   ┌───────────────────────┼─────────────────────────────────────┐
   ▼                       ▼                                     ▼
┌─────────────┐  ┌────────────────────────────────┐  ┌────────────────────┐
│ chat_context│  │ social_memory.db                │  │ hindsight (:8888)  │
│ (wa-intel.db│  │ /root/.hermes/social_memory.db  │  │ vector store       │
│ table)      │  │ contacts, personas,             │  │ bank "hermes"      │
│             │  │ relationship_profiles, …        │  │ retain mission     │
│ summary,    │  │ inner_circle / known /          │  │ for life context   │
│ pending,    │  │ acquaintance tier               │  │ across all domains │
│ urgency,    │  │ recent_interactions JSON        │  │                    │
│ msgs_covered│  │                                 │  │                    │
└─────────────┘  └────────────────────────────────┘  └────────────────────┘

                  ┌────────────────────────────┐
                  │ Telegram (chat 341255489)  │
                  │ Melissa-voice notifications│
                  │ when something needs Arpit │
                  └────────────────────────────┘
```

---

## Components

### 1. `whatsapp-ha` (upstream WhatsApp HTTP API)

Docker container running [gajosu/ha-whatsapp-web-rest-api](https://github.com/gajosu/ha-whatsapp-web-rest-api), which wraps `whatsapp-web.js` over a headless Chromium that maintains the WhatsApp Web session. This deployment runs **PR #202** ([link](https://github.com/gajosu/ha-whatsapp-web-rest-api/pull/202)) which adds `POST /api/chats/<id>/sync-history` and fixes the `/messages?limit=N` 500 error when N exceeds the local cache.

- Container name: `whatsapp`
- Image: `whatsapp-ha:local`
- Port: `0.0.0.0:3000`
- Session storage: `/data/session` inside the container (chromium user-data-dir)
- Source of truth for both inbound webhooks and live REST queries

### 2. `event_normaliser`

Small Go HTTP service that normalizes events from various producers (WhatsApp HA, Gmail, Instagram, generic apps) into a stable Hermes-friendly JSON shape, signs with HMAC-SHA256, and forwards.

- Container name: `event-normaliser`
- Port: `0.0.0.0:11997` (TLS via Let's Encrypt cert at `/etc/letsencrypt/live/vmi3334628.contaboserver.net/`)
- Source: `/root/event_normaliser` (Go module)
- Forwards to `http://127.0.0.1:8090/webhooks/<route>` (i.e. wa-intel) — env var `HERMES_WEBHOOK_BASE_URL` is misleadingly named; it actually points at wa-intel, not Hermes
- Routes: `whatsapp-events`, `gmail-events`, `instagram-dms`, `instagram-events`, `app-events`

### 3. wa-intel daemon

The heart of the pipeline. Single-file Python daemon at `/root/wa-intel/wa-intel.py`.

- Systemd unit: `wa-intel.service`
- Listens on `0.0.0.0:8090`
- DB: `/root/wa-intel/data/wa-intel.db` (SQLite WAL)
- Media: `/root/wa-intel/data/media/<chat_id>/<msg_id>.<ext>` (transient — deleted after summary)
- Brain log: `/root/wa-intel/data/brain_log.jsonl`

**Three background threads:**

| Thread | Cadence | Purpose |
|--------|---------|---------|
| `_poll_loop` | first run on boot, then every `POLL_INTERVAL` (60s) | `_sync_chats` → `_resolve_ids` → `_sync_messages` → `_sync_history_active` (boot only) |
| `_silence_check_loop` | every 15s | wake brain on chats with `unread_count > 0` and silence ≥ `FEED_SILENCE` (120s) |
| `_wa_health_loop` | every 30s | detect WA-API outages and trigger `_recovery_sync` on transition back |

Plus the HTTP server thread (BaseHTTPServer on `:8090`) which handles incoming events, on-demand requests, and proxy paths.

### 4. Hermes / Melissa

The brain. Runs on the same host as a long-lived agent system. wa-intel doesn't talk to Hermes synchronously anymore (that pattern was retired this session) — it only enqueues kanban tasks. Hermes's kanban dispatcher picks them up and spawns one ephemeral worker process per task:

```
hermes -p default --accept-hooks --skills wa-intel chat -q work kanban task t_<id>
```

- `-p default` — inherits the user's default profile (skills, tools, model config)
- `--skills wa-intel` — additionally loads the wa-intel skill so the worker knows the schema and the write-back script paths
- `chat -q work` — quiet mode, work directory
- `kanban task t_<id>` — claims and processes the task, writes its output to `/root/.hermes/kanban/logs/t_<id>.log`

**What Melissa is on top of an LLM**:
- Persona spec: `/root/.hermes/SOUL.md` ("Jarvis-style girlfriend, brief, warm, opinionated")
- Always-on memories auto-loaded each session: `/root/.hermes/memories/USER.md` and `MEMORY.md`
- Long-running brain session for live conversation: `wa-intel-brain` (separate from kanban workers)

### 5. Kanban (work queue)

`/root/.hermes/kanban.db` (SQLite). Tasks are durable, claimed atomically, retried on failure, idempotent on `--idempotency-key`. wa-intel uses these keys religiously to avoid duplicating work across restarts.

- CLI: `hermes kanban {create,list,show,claim,complete,archive,...}`
- Each wa-intel task body is fully self-contained (chat metadata + transcript + prior summary + instructions); the worker doesn't need additional context beyond what's in the body and the wa-intel skill it has loaded.

### 6. Hindsight (vector memory)

Vectorize.io's hindsight at `localhost:8888`. Long-term semantic memory store keyed off a "bank" called `hermes`, with a configured `bank_retain_mission` that explicitly describes what to retain (relationships, plans, deadlines, situations, preferences, emotional context) and what NOT to retain (OTPs, transient logistics, raw message dumps).

- Config: `/root/.hermes/hindsight/config.json`
- API: `http://localhost:8888/openapi.json`, `/docs`, `/health`
- wa-intel **never** calls hindsight directly. Melissa retains via her own hindsight tools when processing kanban tasks.

### 7. social_memory.db

`/root/.hermes/social_memory.db`. Structured social graph: contacts (755 rows), personas (436), relationship_profiles (14 inner-circle people with rich profiles), contact_links, etc.

- Updated in real-time by Melissa via `wa_relationship.py` from kanban tasks
- The legacy `relationship-enrichment-weekly` cron that used to write here was deleted — wa-intel-driven updates have taken over

### 8. chat_context (per-chat live state)

Lives inside `wa-intel.db` (not social_memory). One row per chat:

- `chat_id` (PK)
- `summary` — Melissa's 1–2 sentence current-state read of the conversation
- `topics` — comma-separated tags (optional)
- `pending_action` — what Arpit owes the chat ("reply to Sofia about Friday plans")
- `urgency` — 0.0 cold to 1.0 needs-action-now
- `messages_covered_until` — unix epoch of the latest message considered last time
- `last_updated`

Read by `_enqueue_brain_task` to ground each new prompt; written back by `wa_set_context.py`.

### 9. MEMORY.md / USER.md (durable text memory)

`/root/.hermes/memories/USER.md` and `MEMORY.md`. Plain markdown, auto-loaded into every Hermes session. Intended for fundamental facts about Arpit and his world — preferences, contacts naming conventions ("Shan Bach = Shanga Gafur"), workflow rules, etc. Edited by Melissa rarely and conservatively.

---

## Data flows

### Flow A — Live WhatsApp message arrives

```
WhatsApp servers
    └─► whatsapp-ha (:3000) fires webhook
        └─► event_normaliser (:11997) normalizes + HMAC-signs + forwards
            └─► wa-intel POST /webhooks/whatsapp-events
                ├─► handle_event(payload)
                │   ├─► _extract_msg_data, _resolve_chat_id, _extract_msg_id
                │   ├─► OWN_IDS override of is_from_me (multi-device fix)
                │   ├─► store_message(...) — INSERT OR IGNORE; bumps unread_count
                │   ├─► _maybe_spawn_media_download(...) async (if hasMedia)
                │   ├─► _event_sync(chat_id, is_new_chat) async
                │   │   └─► _fetch_chat_msgs(chat_id, limit=1000 if new else 50)
                │   └─► _check_notify(chat_id)
                │       └─► if unread ≥ FEED_BATCH(3) and group cooldown ok:
                │           _feed_hermes(chat_id) → _enqueue_brain_task(...)
                │           └─► hermes kanban create ...
                ▼
       Melissa worker picks up task, processes per the body's 4 steps,
       writes back to chat_context / social_memory / hindsight, optionally
       sends Telegram notification, marks task done.
```

### Flow B — Polling (every 60s)

```
_poll_loop tick
    └─► _sync_messages
        └─► top 50 chats by last_message_at
            └─► for each: _fetch_chat_msgs(chat_id, limit=100)
                ├─► wa_get(/api/chats/<chat>/messages?limit=100) (stamps health)
                ├─► for each new row: store_message, _maybe_spawn_media_download
                └─► UPDATE chats SET last_polled_at=now

_silence_check_loop tick (15s)
    └─► find chats with unread_count > 0 AND silence ≥ FEED_SILENCE
        └─► for each (skipping group cooldown): _feed_hermes(chat_id)
            (continues into the same kanban-enqueue path as Flow A)
```

### Flow C — Boot sync (one-time on wa-intel start)

```
wa-intel start
    ├─► init_db (idempotent migrations)
    ├─► _seed_own_ids (rebuild OWN_IDS from is_from_me=1 history)
    ├─► start _poll_loop, _silence_check_loop, _wa_health_loop threads
    └─► _BOOT_SYNC_DONE event is initially CLEAR
        While clear: _check_notify and _silence_check_loop are gated off
        _poll_loop runs:
          1. _sync_chats              — refresh chat metadata from /api/chats
                                         (extracts last_message_at from API resp)
          2. _resolve_ids             — resolve LIDs → phones (id_map)
          3. _sync_messages           — top 50 × 100 normal poll
          4. _sync_history_active phase 1
                                       — top BOOT_SYNC_CHATS=500 chats × up to
                                         BOOT_SYNC_MSGS=1000 msgs each, ingest
          5. _sync_history_active phase 2
                                       — enqueue kanban tasks for top 500 chats
                                         that have ANY messages.  force=True if
                                         the chat has unprocessed media (so
                                         idempotency doesn't block recovery)
          6. finally: _BOOT_SYNC_DONE.set()
        Brain wake gates open. Normal triggers active.
```

### Flow D — On-demand summary

```
HTTP POST /api/summarize/<chat_id>?limit=N
    └─► _summarize_on_demand(chat_id, limit=N (default 2000, clamp 1..5000))
        ├─► _fetch_chat_msgs(chat_id, limit=N)
        │     refetches up to N messages from the WA API
        │     auto-spawns media downloads for any newly-stored rows
        └─► _enqueue_brain_task(chat_id, source="ondemand", limit=N)
              builds a prompt with up to N messages of transcript window
              hermes kanban create ...
        Returns JSON: {chat_id, chat_name, fetched, total_messages,
                       enqueued, limit, error}
```

### Flow E — WA-API outage detected and recovered

```
wa_get failure → _WA_LAST_FAIL stamped
…repeated failures, _WA_LAST_OK stays old…

_wa_health_loop tick
    └─► gap = now - _WA_LAST_OK; if gap > WA_OUTAGE_THRESHOLD_SEC (120s)
        AND last_fail > last_ok:
          first time:  log "WA-API OUTAGE: …"
          subsequent:  log "WA-API STILL DOWN: …" every WA_OUTAGE_REPEAT_SEC (300s)

…WA API recovers, next wa_get succeeds → _WA_LAST_OK stamped…

_wa_health_loop tick
    └─► not currently_down AND was in_outage:
        log "WA-API RECOVERED after ~Ns outage; triggering recovery sync"
        cooldown check: skip if within WA_RECOVERY_COOLDOWN of last recovery
        spawn _recovery_sync(outage_dur) thread
            ├─► phase a: sync-history POST on top RECOVERY_SYNC_HISTORY_N (100)
            │           chats — asks WhatsApp servers to backfill the WW.js
            │           in-memory cache
            ├─► phase b: sleep RECOVERY_SYNC_BACKFILL_WAIT (30s)
            ├─► phase c: top RECOVERY_FETCH_CHATS (200) × RECOVERY_FETCH_LIMIT
            │           (2000) msgs each, _fetch_chat_msgs
            └─► phase d: enqueue kanban with source="recovery", force=True for
                        every chat that received NEW rows
```

### Flow F — Image / video media handling

```
Message stored with hasMedia=true and type in {image,video,sticker,audio,ptt,document}
    └─► _maybe_spawn_media_download (async thread)
        └─► _download_media(chat_id, msg_id, mimetype)
              guard: skip if media_path already set OR media_summary_done=1
              GET /api/chats/<chat>/messages/<msg>/download-media
              save bytes to /root/wa-intel/data/media/<chat>/<msg>.<ext>
              UPDATE messages SET media_path=local_path

Next kanban task for this chat
    └─► _enqueue_brain_task includes the message as
            "[image] (path=/root/wa-intel/data/media/.../.jpg)"
        and lists it in a "Media files awaiting a one-line description" block
        Brain (worker) opens the file, generates one-liner, runs:
            wa_set_message_body.py <msg_id> "<one-liner>"
            └─► UPDATE messages SET body=<line>, media_summary_done=1, media_path=''
                os.unlink(local_path) — file deleted, source of truth is the WA API
```

---

## Storage layout

```
/root/wa-intel/
├── wa-intel.py                       # the daemon (single file)
├── wa-intel.py.bak.<event>.<ts>      # timestamped backups from each patch
├── config.yaml                       # daemon config (env-overridable)
├── docker-compose.yaml               # for the unused Go rewrite
├── main.go, internal/, go.mod        # Go rewrite, NOT running in prod
└── data/
    ├── wa-intel.db                   # SQLite WAL — primary store
    ├── wa-intel.db-wal
    ├── wa-intel.db-shm
    ├── brain_log.jsonl               # one JSON record per kanban-enqueue
    └── media/
        └── <chat_id_safe>/
            └── <msg_id_safe>.<ext>   # transient — deleted post-summary

/root/.hermes/
├── config.yaml                       # Hermes routes (whatsapp-events, etc.)
├── SOUL.md                           # Melissa's persona
├── kanban.db                         # work queue
├── social_memory.db                  # contacts, personas, relationship_profiles
├── memories/
│   ├── USER.md                       # auto-loaded user prefs
│   └── MEMORY.md                     # auto-loaded durable facts
├── hindsight/
│   └── config.json                   # bank/retain mission for vector memory
├── skills/integrations/wa-intel/
│   └── SKILL.md                      # what the kanban worker should do
└── scripts/
    ├── wa_set_context.py             # Melissa → chat_context write-back
    ├── wa_set_message_body.py        # Melissa → media one-liner write-back
    └── wa_relationship.py            # Melissa → relationship_profiles toolkit
```

---

## HTTP endpoints

### wa-intel (`:8090`)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | `{status,version,chats,messages}` |
| GET | `/api/chats` | top 100 chats from local DB |
| GET | `/api/wa/*` | proxy to whatsapp-ha at `:3000` |
| POST | `/events` | event ingestion (legacy path) |
| POST | `/webhooks/whatsapp-events` | event ingestion from event_normaliser |
| POST | `/webhooks/<route>` | proxy to Hermes gateway at `:8644` |
| POST | `/api/sync/<chat_id>` | trigger sync-history on whatsapp-ha for one chat |
| POST | `/api/summarize/<chat_id>?limit=N` | on-demand: refetch up to N msgs and enqueue summary task |
| POST | `/api/wa/*` | proxy POST to whatsapp-ha |
| POST | `/api/ask` | text-to-SQL via Ollama (legacy) |

### whatsapp-ha (`:3000`)

See [`SKILL.md`](/root/.hermes/skills/integrations/wa-intel/SKILL.md) for the full route table — chats, messages, groups, contacts, profile (`/me/*`), `/status`. Most relevant from wa-intel's POV:

- `GET /api/chats`
- `GET /api/chats/<id>/messages?limit=N`
- `GET /api/chats/<id>/messages/<msg_id>/download-media`
- `POST /api/chats/<id>/sync-history` (PR #202)
- `GET /api/contacts/<id>`

### event_normaliser (`:11997`, TLS)

Public ingress for normalized events. Reads `HERMES_WEBHOOK_BASE_URL` env var (currently `http://127.0.0.1:8090/webhooks` — i.e. wa-intel). Each route maps a source to a webhook subpath:

```
HERMES_ROUTE_WHATSAPP=whatsapp-events
HERMES_ROUTE_GMAIL=gmail-events
HERMES_ROUTE_INSTAGRAM_DM=instagram-dms
HERMES_ROUTE_INSTAGRAM_EVENT=instagram-events
```

### Hermes gateway (`:8644`)

Receives webhook deliveries for Hermes-side routes (Gmail, Instagram, the persistent `wa-intel-brain` session). wa-intel proxies `/webhooks/*` to here.

### Hindsight (`:8888`)

`/openapi.json`, `/docs`, `/health`. Vector store endpoints for retain/recall — used only by Melissa, not by wa-intel.

---

## CLI write-back scripts

All under `/root/.hermes/scripts/`. Designed so Melissa never has to construct fragile SQL — she runs a script and the script handles parameter binding, JSON merging, etc.

### `wa_set_context.py`

Upsert a row in `chat_context` (in `wa-intel.db`).

```
wa_set_context.py <chat_id> <summary> <pending_action> <urgency> <messages_covered_until>
                  [--topics <comma,sep>] [--db PATH]
```

### `wa_set_message_body.py`

Update `messages.body` for a single message, set `media_summary_done=1`, and (by default) delete the local media file.

```
wa_set_message_body.py <msg_id> <body>  [--keep-file] [--db PATH]
```

### `wa_relationship.py`

Manage `social_memory.relationship_profiles` and `contacts.tier`. `--contact` accepts a numeric `contacts.id` or a `display_name` (case-insensitive, exact preferred over LIKE).

```
wa_relationship.py upsert         --contact <id|name>
                                  [--tier T] [--personality ...] [--life-context ...]
                                  [--relationship-type ...] [--relationship-notes ...]
                                  [--interests ...] [--interaction-frequency ...]
                                  [--who-initiates ...] [--shared-activities ...]
                                  [--trajectory strengthening|stable|drifting|new]

wa_relationship.py add-interaction --contact <id|name> --what "..." --vibe "..."
                                   [--date YYYY-MM-DD]
                                   # appends to recent_interactions JSON, caps at 10

wa_relationship.py set-tier        --contact <id|name>
                                   --tier inner_circle|known|acquaintance|unknown
                                   # promoting to inner_circle creates a skeleton profile

wa_relationship.py delete-profile  --contact <id|name>
                                   # removes profile; contact row stays
```

---

## Brain prompt structure

Every kanban task body produced by `_enqueue_brain_task` follows this template:

```
WhatsApp chat: <chat_name>  |  id: <chat_id>  |  type: dm|group  [|  N members]

<prior_block>
   if has prior chat_context:
      "Earlier you processed this chat. Your stored read:
        Summary: <prior_summary>
        Pending action: <prior_pending_action or "(none)">
        Last message you covered: <ts>
       These messages have arrived since:"
   else:
      "This chat has no prior summary in chat_context. Build the initial
       summary from these messages:"

<transcript>
   <YYYY-MM-DD HH:MM>  <Sender>: <body or "[type] (path=...)" or "[type]">
   …

<media_block>  (if any pending media)
   Media files awaiting a one-line description:
     - msg_id=… type=image path=/root/wa-intel/data/media/…
     - …

Your tasks:

  1. Media one-liners — for each pending file, view it and persist:
       wa_set_message_body.py "<msg_id>" "<one-line description>"

  2. Chat summary — incorporate any media descriptions you wrote in step 1:
       wa_set_context.py "<chat_id>" "<summary>" "<pending>" <urgency> <last_ts>

  3. Update social memory + hindsight (DM chats only — skip for groups).
       a) wa_relationship.py add-interaction (if substantive content)
       b) wa_relationship.py upsert (if learned new durable info)
       c) wa_relationship.py set-tier (promote/demote when warranted)
       d) hindsight retain (durable life facts per bank_retain_mission)
       e) optionally edit MEMORY.md (rare, fundamental Arpit-facts only)

  4. Decide whether to notify Arpit. If yes, send a brief Melissa-voice note
     to Telegram chat 341255489. Otherwise, do nothing.

If you need more context (older messages, related chats, contacts), use the
wa-intel skill to query ~/wa-intel/data/wa-intel.db, social_memory.db, or
hindsight directly.

Mark this kanban task complete when all relevant steps are done.
```

The prompt's transcript window is sized by the optional `limit` parameter (default 100; on-demand defaults to 2000). For chats with prior chat_context, only messages with `timestamp > messages_covered_until` are included (capped at `limit`).

---

## Configuration (env vars)

All set via systemd unit `Environment=` lines in `/etc/systemd/system/wa-intel.service`, or `.env` files / shell defaults.

### Identity

| Var | Default | Purpose |
|-----|---------|---------|
| `OWN_PHONE` | `""` | Arpit's phone (e.g. `31684337120@c.us`); used to override is_from_me when WA reports false on multi-device sends |
| `OWN_NAME` | `"Arpit Agarwal"` (set in unit) | same, for `pushName` matching |

### Cadence

| Var | Default | Purpose |
|-----|---------|---------|
| `POLL_INTERVAL` | `60` | seconds between `_sync_messages` ticks |
| `FEED_BATCH` | `3` | unread threshold to wake brain via `_check_notify` |
| `FEED_SILENCE` | `120` | seconds of silence before `_silence_check_loop` wakes brain |
| `NOTIFY_COOLDOWN` | `1800` | applied **only to groups** (DMs ignore it) |

### Boot sync

| Var | Default | Purpose |
|-----|---------|---------|
| `BOOT_SYNC_CHATS` | `500` | top-N chats to ingest at boot |
| `BOOT_SYNC_MSGS` | `1000` | per-chat ingest limit at boot |

### Media

| Var | Default | Purpose |
|-----|---------|---------|
| `WA_MEDIA_DIR` | `/root/wa-intel/data/media` | base dir for downloaded media |

### Outage detection / recovery

| Var | Default | Purpose |
|-----|---------|---------|
| `WA_OUTAGE_THRESHOLD_SEC` | `120` | gap-since-success threshold to declare outage |
| `WA_OUTAGE_REPEAT_SEC` | `300` | seconds between repeated "STILL DOWN" warnings |
| `WA_RECOVERY_COOLDOWN` | `60` | min seconds between recovery_sync runs (anti-flap) |
| `RECOVERY_SYNC_HISTORY_N` | `100` | top-N chats to ask WA to sync-history backfill |
| `RECOVERY_SYNC_BACKFILL_WAIT` | `30` | seconds to wait after sync-history before fetching |
| `RECOVERY_FETCH_CHATS` | `200` | top-N chats to elevated-fetch in recovery |
| `RECOVERY_FETCH_LIMIT` | `2000` | per-chat fetch limit in recovery |

### Connections

| Var | Default | Purpose |
|-----|---------|---------|
| `WA_API` | `http://localhost:3000` | upstream whatsapp-ha base URL |
| `LISTEN_PORT` | `8090` | wa-intel HTTP server bind port |
| `DB_PATH` | `/root/wa-intel/data/wa-intel.db` | wa-intel DB |
| `HERMES_SESSION` | `wa-intel-brain` | (legacy; not used by kanban path) |
| `HERMES_LOG_PATH` | `/root/wa-intel/data/brain_log.jsonl` | per-call audit log |

---

## Failure modes & recovery

### A. WA API down

- **Detection**: `_wa_health_loop` watches `_WA_LAST_OK` vs `_WA_LAST_FAIL`. After `WA_OUTAGE_THRESHOLD_SEC` (120s) of no successes, logs `WA-API OUTAGE`. Repeats every `WA_OUTAGE_REPEAT_SEC` (300s).
- **Effect during**: `wa_get` returns `None`; polling no-ops; webhooks deliver nothing because the WA container isn't pushing. wa-intel keeps running.
- **Recovery**: when next `wa_get` succeeds, the loop logs `WA-API RECOVERED` and spawns `_recovery_sync`: sync-history → backfill wait → top-200 × 2000-msg fetch → force-enqueue kanban for affected chats.

### B. wa-intel crash

- systemd `Restart=always`. On boot, the boot-sync gate (`_BOOT_SYNC_DONE`) prevents brain wakes until ingest + enqueue done. Boot sync is comprehensive (top 500 × 1000 msgs).
- Pending media (`media_summary_done=0`) triggers `force=True` enqueues so the brain re-processes any media that was downloaded but not yet summarized when the crash happened.

### C. WhatsApp Web cache eviction

- `download-media` returns 404 on messages that have aged out of WW.js's in-memory store.
- Workaround: `POST /api/chats/<id>/sync-history` to ask WhatsApp servers to backfill. Then retry download.
- This is exactly what `_recovery_sync` does at scale.

### D. Idempotency-driven dedup masquerading as "no work"

- `hermes kanban create` with the same `--idempotency-key` returns the existing task ID with rc=0 — no new task is created. Logged as `kanban=` (vs `kanban+` for genuinely new tasks).
- Boot sync re-runs are mostly `kanban=` events. This is correct — same `(chat_id, last_ts, source)` shouldn't produce duplicate work.

### E. LID / phone canonical-id drift (HISTORICAL — fixed)

- **Was**: `_fetch_chat_msgs` resolved `@lid → @c.us` for the API URL **and** for storage. Result: messages landed under a phantom `@c.us` chat row while the canonical `@lid` chat stayed empty. ~270 chats affected, ~1,464 messages misrouted.
- **Now**: store under canonical `chat_id` (whatever was returned by `/api/chats`); `fetch_id` is only used for the API URL.

### F. is_from_me false on multi-device sends (HISTORICAL — fixed)

- **Was**: WhatsApp's multi-device sync reports `fromMe=false` for messages sent from a non-API device. Brain saw your own messages as third-party content. Notable consequence: a Charlotte conversation where Melissa returned NO_ACTION because the prompt looked like two strangers chatting.
- **Now**: `OWN_IDS` set seeded from `OWN_PHONE`/`OWN_NAME` env + auto-cached from any `is_from_me=1` message. Override applied in `handle_event`.

### G. Persistent session prompt staleness (HISTORICAL — retired)

- **Was**: `_feed_hermes` did `hermes -z --continue wa-intel-brain <prompt>`. The session cached an old system prompt that didn't include chat_context write-back instructions. Result: brain processed but never wrote summaries.
- **Now**: kanban-based architecture. Each task is a fresh worker spawn with the full instruction set in the task body. No persistent-session caching to fight.

---

## Operations runbook

### Quick health check

```bash
# is wa-intel running?
systemctl is-active wa-intel

# health endpoint (returns chat + message counts)
curl -s http://localhost:8090/health | jq

# upstream WA API reachable?
curl -s http://localhost:3000/api/status | jq
```

### Live tailing

```bash
# everything wa-intel is doing
journalctl -u wa-intel -f --no-pager

# just milestones (boot sync phases, kanban events, outages, recovery)
journalctl -u wa-intel -f --no-pager | \
  grep --line-buffered -E \
    'boot-sync|kanban[+=]|kanban (FAIL|error)|event-sync|poller: ready|WA-API|recovery-sync'

# brain calls (full prompt + response per task)
tail -f /root/wa-intel/data/brain_log.jsonl | jq
```

### Restart (graceful)

```bash
systemctl restart wa-intel
# wait ~5min for boot sync to finish
journalctl -u wa-intel -f | grep "poller: ready"
```

### Trigger manual recovery for one chat

```bash
# refetch up to 2000 msgs from the WA API and enqueue a fresh summary task
curl -s -X POST 'http://localhost:8090/api/summarize/<chat_id>?limit=2000' | jq

# trigger sync-history (backfill from WhatsApp servers) for one chat
curl -s -X POST 'http://localhost:8090/api/sync/<chat_id>' | jq
```

### Inspect kanban queue

```bash
# counts by status
for s in todo ready running done blocked review; do
  printf '%-10s ' "$s"
  hermes kanban list --status "$s" --json 2>/dev/null | jq 'length'
done

# what Melissa is working on right now
hermes kanban list --status running --json | jq '.[] | {id, title, started_at}'

# inspect a task body
hermes kanban show <task_id>
```

### Inspect a chat's state

```bash
# chat metadata + last_polled / last_notified
sqlite3 -line /root/wa-intel/data/wa-intel.db \
  "SELECT * FROM chats WHERE id='<chat_id>';"

# chat_context summary
sqlite3 -line /root/wa-intel/data/wa-intel.db \
  "SELECT * FROM chat_context WHERE chat_id='<chat_id>';"

# relationship profile (if inner_circle)
sqlite3 -line /root/.hermes/social_memory.db \
  "SELECT rp.*, c.display_name FROM relationship_profiles rp
   JOIN contacts c ON c.id = rp.contact_id
   WHERE lower(c.display_name) LIKE lower('%<name>%');"
```

### Cross-DB join (wa-intel + social_memory)

```bash
sqlite3 ~/.hermes/social_memory.db <<'SQL'
ATTACH DATABASE '/root/wa-intel/data/wa-intel.db' AS wa;
SELECT c.display_name, rp.trajectory, cc.summary
FROM contacts c
LEFT JOIN relationship_profiles rp ON rp.contact_id = c.id
LEFT JOIN wa.chats wc ON lower(wc.name) = lower(c.display_name)
LEFT JOIN wa.chat_context cc ON cc.chat_id = wc.id
WHERE c.tier = 'inner_circle'
ORDER BY rp.last_updated DESC;
SQL
```

### Backups

The "source of truth" tier:

- `/root/wa-intel/data/wa-intel.db` (messages, chats, chat_context — irreplaceable from upstream API beyond ~50 msgs/chat)
- `/root/.hermes/social_memory.db` (relationship_profiles)
- `/root/.hermes/memories/USER.md` and `MEMORY.md`
- `/root/.hermes/hindsight/config.json` (config; the bank's actual data lives in the hindsight container)

The `media/` dir is **transient** by design — files are deleted after Melissa writes the one-liner, and the upstream API is the source of truth for the bytes.

### Common debugging

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `journalctl` shows `kanban=` for every chat, no new tasks | Idempotency dedup — same `(chat_id, last_ts, source)` as prior boot. Working as designed. | None — `kanban+` will fire when last_ts advances |
| chat_context never updates for a chat | wa-intel-brain persistent session caching old prompt (ONLY if you reverted to legacy synchronous path) | Use kanban path (default) |
| Charlotte / DM brain returns NO_ACTION inappropriately | is_from_me bug on multi-device sync | Already fixed via OWN_IDS |
| download-media returns 404 | message aged out of WW.js cache | `POST /api/sync/<chat_id>` then retry |
| `WA-API STILL DOWN` for hours | container dead or chromium stuck | `docker restart whatsapp` |
| Brain queue draining slowly | one Melissa worker per task, expensive model | `hermes kanban list --status running` to see how many in flight |

---

## Schema reference

### `wa-intel.db`

```sql
CREATE TABLE contacts (
    phone TEXT PRIMARY KEY,
    name TEXT, relationship TEXT, notes TEXT,
    first_seen INTEGER, last_seen INTEGER
);

CREATE TABLE chats (
    id TEXT PRIMARY KEY,                -- canonical: @c.us / @g.us / @lid
    name TEXT,
    type TEXT,                          -- 'dm' | 'group'
    participant_count INTEGER,
    last_message_at INTEGER,
    last_notified_at INTEGER,
    last_polled_at INTEGER,
    unread_count INTEGER,
    is_muted BOOLEAN
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,                -- WA _serialized form
    chat_id TEXT NOT NULL,
    sender_name TEXT, sender_phone TEXT,
    timestamp INTEGER NOT NULL,
    body TEXT,
    type TEXT,                          -- chat / image / video / sticker / …
    media_path TEXT,                    -- local file when downloaded; cleared after summary
    is_from_me BOOLEAN,                 -- corrected via OWN_IDS heuristic
    reply_to_id TEXT,
    media_summary_done INTEGER          -- 1 = brain has written body
);

CREATE TABLE reactions (
    message_id TEXT, chat_id TEXT,
    reactor_name TEXT, reactor_phone TEXT,
    emoji TEXT, timestamp INTEGER
);

CREATE TABLE chat_context (
    chat_id TEXT PRIMARY KEY,
    summary TEXT,
    topics TEXT,
    pending_action TEXT,
    urgency REAL,
    last_updated INTEGER,
    messages_covered_until INTEGER
);

CREATE TABLE id_map (
    lid TEXT PRIMARY KEY,
    phone TEXT, name TEXT
);
```

### `social_memory.db` (wa-intel-relevant tables only)

```sql
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY,
    display_name TEXT,
    tier TEXT DEFAULT 'unknown',        -- inner_circle / known / acquaintance / unknown
    notes TEXT,
    created_at INTEGER
);

CREATE TABLE relationship_profiles (
    id INTEGER PRIMARY KEY,
    contact_id INTEGER UNIQUE REFERENCES contacts(id),
    display_name TEXT,
    tier TEXT,                          -- mirror of contacts.tier
    personality TEXT,
    interests TEXT,
    life_context TEXT,
    relationship_type TEXT,             -- friend / family / romantic / colleague / …
    relationship_notes TEXT,
    interaction_frequency TEXT,
    who_initiates TEXT,                 -- mostly_me / mostly_them / balanced
    shared_activities TEXT,
    recent_interactions TEXT,           -- JSON: [{date, what, vibe}, …] cap 10
    trajectory TEXT,                    -- strengthening / stable / drifting / new / dormant
    last_interaction_date INTEGER,
    last_updated INTEGER,
    created_at INTEGER
);

-- Also: personas, contact_links, chats, messages, accumulator_state
-- (legacy / lighter representations; not currently driven by wa-intel)
```

---

## History of changes

This system has been incrementally rebuilt during a single working session. Major shifts, in order:

1. **is_from_me bug fix** — added `OWN_IDS` heuristic; multi-device sends now correctly tagged.
2. **DM cooldown disabled** — only groups respect `NOTIFY_COOLDOWN=1800s`. DMs gated by `FEED_BATCH=3` and `FEED_SILENCE=120s` only.
3. **Brain log JSONL** — `/root/wa-intel/data/brain_log.jsonl` records every kanban-enqueue with full prompt and response.
4. **Aggressive boot sync** — top 500 chats × up to 1000 msgs ingest at boot, env-tunable.
5. **Event-triggered fetch** — `_event_sync` rate-limited to 30s/chat; fetches 1000 msgs for new chats, 50 for existing.
6. **Kanban-based brain pipeline** — replaced synchronous `hermes -z --continue` with `hermes kanban create`. Each task is fresh, self-contained, processed async by Melissa workers.
7. **Boot-sync gate** — `_BOOT_SYNC_DONE` event prevents brain wakes mid-boot; releases via try/finally in `_poll_loop`.
8. **chat_context wiring** — `_enqueue_brain_task` reads prior summary; brain writes via `wa_set_context.py`.
9. **On-demand summary endpoint** — `POST /api/summarize/<chat_id>?limit=N`, default 2000, clamped 1–5000.
10. **LID/phone unification** — `_fetch_chat_msgs` and `_sync_messages` now store under canonical `chat_id`; one-shot migration moved 1,464 messages out of phantom `@c.us` rows; 122 phantom chats deleted.
11. **Stale kanban tasks archived** — 431 ready tasks created with old (wrong) chat_ids removed.
12. **Media handling** — download to `/root/wa-intel/data/media/`, brain writes 1-line via `wa_set_message_body.py`, file deleted after.
13. **`media_summary_done` column** added with idempotent `ALTER TABLE`.
14. **Kanban log clarity** — distinguish `kanban+` (new task) from `kanban=` (idempotency dedup hit).
15. **Social memory write-back** — `wa_relationship.py` (upsert / add-interaction / set-tier / delete-profile), step 3 in the kanban prompt explicitly grants Melissa promotion / demotion / hindsight authority.
16. **Cron deletion** — the legacy `relationship-enrichment-weekly` cron (Sunday 3am) was removed; wa-intel-driven updates have taken over.
17. **WA-API outage detection** — `_wa_health_loop` watches `_WA_LAST_OK` / `_WA_LAST_FAIL`; logs OUTAGE / STILL DOWN / RECOVERED.
18. **Recovery sync** — auto-fires on outage→recovery transition: sync-history POST on top 100 → 30s backfill wait → top-200 × 2000-msg fetch → force-enqueue kanban for affected chats.

Backups of every patch live at `/root/wa-intel/wa-intel.py.bak.<event>.<timestamp>`.

---

## Known gaps / future work

None are blockers; all are "would be nicer if."

- **`personas` vs `relationship_profiles` overlap** — two parallel tables for "who is this person"; the lighter one (personas, 436 rows) is mostly historical, the deeper one (relationship_profiles, 14 inner-circle) is what wa-intel writes to. Could be merged into one `people` table with a tier column gating which fields apply.
- **Duplicate contacts table rows** — e.g. `Shan Bach` exists at `id=2988` and `id=3050`; `Akash Shanker` similarly duplicated. `wa_relationship.py` resolves by preferring the row with an existing profile, but a one-shot dedup migration would make queries cleaner.
- **Per-chat staleness polling** ("option 3" from the recovery design discussion) — currently long-tail chats outside the top 50 are only refreshed via webhooks (which can fail) or restarts. A 5-min staleness-driven sweep of cold chats (10 chats, oldest `last_polled_at`) would cover this. **Decided not to ship; restart cadence handles it adequately for now.**
- **Reactions ingest** — only ~1 reaction stored despite many in real chats. `_handle_reaction_event` likely isn't being fed the right webhook events. Worth a 30-min investigation.
- **Telegram alert on outage** — currently outage detection is journal-only. Could route through Hermes's existing Telegram delivery (`chat_id 341255489`) for push alerts when `WA-API OUTAGE` fires for >10 min.
- **Automatic MEMORY.md edits** — currently allowed in the prompt but no script wrapper. Melissa would have to write to the file directly. A `mem_set.py <key> <value>` wrapper with audit-log + size cap would make it safer.
- **Schema introspection in SKILL.md** — the SKILL describes the schema in markdown text; it could just `cat schema_<db>.sql` at build time so it's never out of date.
- **Go rewrite under `/root/wa-intel/{main.go,internal/}`** — built but not running. Either finish migrating or remove to reduce cognitive load.

---

*Last updated: this document was written in one pass at the end of a multi-hour rebuild session. Diff against `wa-intel.py.bak.*` files for the per-change forensic trail.*
