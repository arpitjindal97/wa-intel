#!/usr/bin/env python3
"""wa-intel v4 — WhatsApp Intelligence collector
Stores messages in clean schema optimized for LLM querying.
Triggers Hermes for autonomous notifications.
Dumb pipe: collect, store, trigger. No LLM calls.
"""

import sqlite3, json, time, threading, subprocess, os, sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError
from urllib.parse import unquote, parse_qs

# === Config ===
WA_API = os.getenv("WA_API", "http://localhost:3000")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "8090"))
DB_PATH = os.getenv("DB_PATH", "/root/wa-intel/data/wa-intel.db")
HERMES_SESSION = os.getenv("HERMES_SESSION", "wa-intel-brain")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
FEED_BATCH = int(os.getenv("FEED_BATCH", "3"))
FEED_SILENCE = int(os.getenv("FEED_SILENCE", "120"))  # seconds
NOTIFY_COOLDOWN = int(os.getenv("NOTIFY_COOLDOWN", "1800"))  # 30 min per chat

# Own identity used to fix is_from_me when WhatsApp multi-device sync
# falsely reports fromMe=False for messages sent from a non-API device.
OWN_PHONE = os.getenv("OWN_PHONE", "")
OWN_NAME = os.getenv("OWN_NAME", "")
OWN_IDS = set(x for x in (OWN_PHONE, OWN_NAME) if x)

HERMES_LOG_PATH = os.getenv("HERMES_LOG_PATH", "/root/wa-intel/data/brain_log.jsonl")

# === Boot sync gate ===
# Cleared on module load; set in _poll_loop's finally clause once boot-sync
# (ingest + sequential brain wakes) is complete. _check_notify and
# _silence_check_loop both no-op while it's clear.
_BOOT_SYNC_DONE = threading.Event()

# === WA-API health state ===
# Stamped by wa_get on each success/failure. Watched by _wa_health_loop.
_WA_STATE_LOCK = threading.Lock()
_WA_LAST_OK = 0          # epoch of last successful wa_get; 0 = never yet
_WA_LAST_FAIL = 0        # epoch of last failed wa_get
_WA_LAST_RECOVERY_AT = 0 # cooldown for _recovery_sync (avoid flapping)
WA_OUTAGE_THRESHOLD_SEC = int(os.getenv("WA_OUTAGE_THRESHOLD_SEC", "120"))
WA_OUTAGE_REPEAT_SEC    = int(os.getenv("WA_OUTAGE_REPEAT_SEC",    "300"))
WA_RECOVERY_COOLDOWN    = int(os.getenv("WA_RECOVERY_COOLDOWN",    "60"))

# === Media storage ===
MEDIA_DIR = os.getenv("WA_MEDIA_DIR", "/root/wa-intel/data/media")
_MEDIA_LOCK = threading.Lock()
_MEDIA_INFLIGHT = set()  # (chat_id, msg_id) tuples currently being downloaded

# === Database ===
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.executescript("""
    CREATE TABLE IF NOT EXISTS contacts (
        phone TEXT PRIMARY KEY,
        name TEXT DEFAULT '',
        relationship TEXT DEFAULT '',
        notes TEXT DEFAULT '',
        first_seen INTEGER DEFAULT 0,
        last_seen INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS chats (
        id TEXT PRIMARY KEY,
        name TEXT DEFAULT '',
        type TEXT DEFAULT 'dm',
        participant_count INTEGER DEFAULT 0,
        last_message_at INTEGER DEFAULT 0,
        last_notified_at INTEGER DEFAULT 0,
        last_polled_at INTEGER DEFAULT 0,
        unread_count INTEGER DEFAULT 0,
        is_muted BOOLEAN DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        chat_id TEXT NOT NULL,
        sender_name TEXT DEFAULT '',
        sender_phone TEXT DEFAULT '',
        timestamp INTEGER NOT NULL,
        body TEXT DEFAULT '',
        type TEXT DEFAULT 'chat',
        media_path TEXT DEFAULT '',
        is_from_me BOOLEAN DEFAULT 0,
        reply_to_id TEXT DEFAULT '',
        media_summary_done INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_msg_chat_ts ON messages(chat_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_msg_ts ON messages(timestamp);
    CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        reactor_name TEXT DEFAULT '',
        reactor_phone TEXT DEFAULT '',
        emoji TEXT DEFAULT '',
        timestamp INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_react_msg ON reactions(message_id);
    CREATE TABLE IF NOT EXISTS chat_context (
        chat_id TEXT PRIMARY KEY,
        summary TEXT DEFAULT '',
        topics TEXT DEFAULT '',
        pending_action TEXT DEFAULT '',
        urgency REAL DEFAULT 0,
        last_updated INTEGER DEFAULT 0,
        messages_covered_until INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS id_map (
        lid TEXT PRIMARY KEY,
        phone TEXT NOT NULL,
        name TEXT DEFAULT ''
    );
    """)
    # idempotent migrations for older DBs
    cols = {row[1] for row in db.execute("PRAGMA table_info(messages)").fetchall()}
    if "media_summary_done" not in cols:
        db.execute("ALTER TABLE messages ADD COLUMN media_summary_done INTEGER DEFAULT 0")
    db.commit()
    return db

DB = init_db()
LOCK = threading.Lock()

def q(sql, params=()):
    with LOCK:
        return DB.execute(sql, params).fetchall()

def qone(sql, params=()):
    with LOCK:
        r = DB.execute(sql, params).fetchone()
        return r[0] if r else None

def ex(sql, params=()):
    with LOCK:
        DB.execute(sql, params)
        DB.commit()

def ex_many(statements):
    with LOCK:
        for sql, params in statements:
            DB.execute(sql, params)
        DB.commit()

# === ID Resolution ===
def resolve_lid(lid):
    return qone("SELECT phone FROM id_map WHERE lid=?", (lid,)) or ""

def set_mapping(lid, phone, name=""):
    ex("INSERT OR REPLACE INTO id_map (lid,phone,name) VALUES (?,?,?)", (lid, phone, name))

def _seed_own_ids():
    """Seed OWN_IDS from existing is_from_me=1 messages (self-bootstrap)."""
    for (p,) in q("SELECT DISTINCT sender_phone FROM messages WHERE is_from_me=1 AND sender_phone!=''"):
        OWN_IDS.add(p)
    for (n,) in q("SELECT DISTINCT sender_name FROM messages WHERE is_from_me=1 AND sender_name!=''"):
        OWN_IDS.add(n)
    if OWN_IDS:
        print(f"  own_ids: seeded {len(OWN_IDS)} identifiers")

# === Core: Store Message ===
def store_message(msg_id, chat_id, sender_name, sender_phone, ts, body, msg_type="chat", is_from_me=False, media_path="", reply_to=""):
    """Store a message. Returns True if new (inserted), False if duplicate."""
    ex("INSERT OR IGNORE INTO chats (id) VALUES (?)", (chat_id,))
    with LOCK:
        c = DB.execute(
            "INSERT OR IGNORE INTO messages (id,chat_id,sender_name,sender_phone,timestamp,body,type,is_from_me,media_path,reply_to_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (msg_id, chat_id, sender_name, sender_phone, ts, body, msg_type, is_from_me, media_path, reply_to))
        if c.rowcount > 0:
            DB.execute("UPDATE chats SET last_message_at=MAX(last_message_at,?), unread_count=unread_count+1 WHERE id=?", (ts, chat_id))
            # Update contact last_seen
            if sender_phone and not is_from_me:
                DB.execute("INSERT OR IGNORE INTO contacts (phone,name,first_seen,last_seen) VALUES (?,?,?,?)", (sender_phone, sender_name, ts, ts))
                DB.execute("UPDATE contacts SET last_seen=MAX(last_seen,?), name=CASE WHEN ?!='' THEN ? ELSE name END WHERE phone=?", (ts, sender_name, sender_name, sender_phone))
        DB.commit()
        return c.rowcount > 0

def store_reaction(message_id, chat_id, reactor_name, reactor_phone, emoji, ts):
    """Store a reaction. Only if original message exists in DB."""
    orig = qone("SELECT body FROM messages WHERE id=?", (message_id,))
    if not orig:
        return False  # skip reaction if we don't have the original message
    ex("INSERT INTO reactions (message_id,chat_id,reactor_name,reactor_phone,emoji,timestamp) VALUES (?,?,?,?,?,?)",
       (message_id, chat_id, reactor_name, reactor_phone, emoji, ts))
    ex("UPDATE chats SET unread_count=unread_count+1 WHERE id=?", (chat_id,))
    return True

# === Event Ingester ===
def handle_event(data):
    try:
        evt = json.loads(data) if isinstance(data, (str, bytes)) else data
    except:
        return

    event_type = evt.get("event_type", "")
    if event_type in ("whatsapp_authenticated", "whatsapp_disconnected", "whatsapp_message_ack"):
        return
    if event_type == "whatsapp_message_reaction":
        _handle_reaction_event(evt)
        return

    # Extract message data
    msg_data = _extract_msg_data(evt)
    if not msg_data:
        return

    body = msg_data.get("body", "") or ""
    is_from_me = msg_data.get("fromMe", False)
    msg_type = msg_data.get("type", "chat") or "chat"
    ts = int(msg_data.get("t", msg_data.get("timestamp", time.time())))
    sender_phone = msg_data.get("author", msg_data.get("from", ""))
    sender_name = msg_data.get("notifyName", msg_data.get("pushName", ""))
    has_media = msg_data.get("hasMedia", False)

    # Resolve chat ID from id.remote
    chat_id = _resolve_chat_id(msg_data)
    if not chat_id:
        return

    # Extract message ID
    msg_id = _extract_msg_id(msg_data)
    if not msg_id:
        msg_id = f"{chat_id}_{ts}"

    # Resolve sender phone if LID
    if sender_phone and "@lid" in sender_phone:
        resolved = resolve_lid(sender_phone)
        if resolved:
            sender_phone = resolved

    # Multi-device sync bug fix: WhatsApp reports fromMe=False for messages
    # sent from a non-API device. Override when sender matches our own identity.
    if not is_from_me and (sender_phone in OWN_IDS or sender_name in OWN_IDS):
        is_from_me = True

    # Detect new-to-DB chat BEFORE storing the event message so _event_sync
    # knows whether to fetch deep history (1000) or shallow refresh (50).
    prior_count = qone("SELECT COUNT(*) FROM messages WHERE chat_id=?", (chat_id,)) or 0
    is_new_chat = prior_count == 0

    if store_message(msg_id, chat_id, sender_name, sender_phone, ts, body, msg_type, is_from_me):
        if is_from_me:
            if sender_phone: OWN_IDS.add(sender_phone)
            if sender_name: OWN_IDS.add(sender_name)
        print(f"  [{chat_id}] {sender_name or 'me'}: {body[:50]}")
        _maybe_spawn_media_download(msg_data, chat_id, msg_id)
        threading.Thread(target=_event_sync, args=(chat_id, is_new_chat), daemon=True).start()
        _check_notify(chat_id)

def _handle_reaction_event(evt):
    payload = evt.get("payload", evt.get("payload_json", {}))
    if not payload:
        return
    emoji = payload.get("reaction", "")
    if not emoji:
        return  # reaction removed
    msg_id_obj = payload.get("msgId", {})
    if not isinstance(msg_id_obj, dict):
        return
    orig_id = msg_id_obj.get("_serialized", "")
    if not orig_id:
        return

    remote = msg_id_obj.get("remote", "")
    chat_id = ""
    if remote:
        chat_id = resolve_lid(remote) if "@lid" in remote else remote
    if not chat_id:
        return

    reactor_phone = payload.get("senderId", "")
    if reactor_phone and "@lid" in reactor_phone:
        reactor_phone = resolve_lid(reactor_phone) or reactor_phone
    reactor_name = ""  # reactions don't include name
    ts = int(payload.get("timestamp", time.time()))

    if store_reaction(orig_id, chat_id, reactor_name, reactor_phone, emoji, ts):
        orig_body = qone("SELECT body FROM messages WHERE id=?", (orig_id,)) or ""
        print(f"  [{chat_id}] reacted {emoji} to: {orig_body[:30]}")
        _check_notify(chat_id)

def _extract_msg_data(evt):
    for path in [("payload", "message"), ("payload_json", "message"), ("payload",), ("data",)]:
        obj = evt
        for key in path:
            obj = obj.get(key, {}) if isinstance(obj, dict) else {}
        if obj and (obj.get("body") is not None or obj.get("t") or obj.get("id")):
            return obj
    return None

def _resolve_chat_id(msg_data):
    # Best: id.remote
    id_obj = msg_data.get("id", {})
    if isinstance(id_obj, dict):
        remote = id_obj.get("remote", "")
        if remote:
            if "@c.us" in remote or "@g.us" in remote:
                return remote
            return resolve_lid(remote) or remote
    # Fallback: chatId field
    cid = msg_data.get("chatId", "")
    if cid:
        if "@c.us" in cid or "@g.us" in cid:
            return cid
        return resolve_lid(cid) or cid
    return ""

def _extract_msg_id(msg_data):
    id_obj = msg_data.get("id", {})
    if isinstance(id_obj, dict):
        return id_obj.get("_serialized", id_obj.get("id", ""))
    return ""

# === Brain call logging ===
def _log_hermes_call(record):
    """Append a JSON record per hermes invocation for replay/audit."""
    try:
        record["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        record["epoch"] = int(time.time())
        with open(HERMES_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"  brain_log error: {e}")


# === Hermes Notification ===
def _check_notify(chat_id):
    """Decide whether to trigger hermes for this chat.

    During boot sync this is a no-op — brain wakes happen sequentially in
    _sync_history_active so context can build one chat at a time.
    """
    if not _BOOT_SYNC_DONE.is_set():
        return
    row = q("SELECT unread_count, last_notified_at, last_message_at, is_muted FROM chats WHERE id=?", (chat_id,))
    if not row:
        return
    unread, last_notified, last_msg, muted = row[0]
    if muted:
        return
    now = int(time.time())

    # NOTIFY_COOLDOWN applies only to groups. DMs are always eligible — gated
    # only by FEED_BATCH (3 msgs) and FEED_SILENCE (2 min).
    chat_type = qone("SELECT type FROM chats WHERE id=?", (chat_id,)) or "dm"
    if chat_type == "group" and last_notified and (now - last_notified) < NOTIFY_COOLDOWN:
        return

    if unread >= FEED_BATCH:
        threading.Thread(target=_feed_hermes, args=(chat_id,), daemon=True).start()

def _silence_check_loop():
    """Periodically check for chats with pending messages after silence.

    Suppressed during boot sync. Cooldown applies to groups only.
    """
    while True:
        time.sleep(15)
        if not _BOOT_SYNC_DONE.is_set():
            continue
        try:
            now = int(time.time())
            rows = q("SELECT id, last_notified_at, type FROM chats WHERE unread_count>0 AND is_muted=0 AND (?-last_message_at)>=?", (now, FEED_SILENCE))
            for chat_id, last_notified, chat_type in rows:
                if chat_type == "group" and last_notified and (now - last_notified) < NOTIFY_COOLDOWN:
                    continue
                _feed_hermes(chat_id)
        except Exception as e:
            print(f"  silence-check error: {e}")

def _enqueue_brain_task(chat_id, source="event", limit=None, force=False):
    """Enqueue a self-contained kanban task to process this chat.

    Reads chat_context to decide whether this is an initial summary
    (no prior row -> boot-style prompt with last 100 msgs) or an
    incremental update (prior row -> include only msgs newer than
    chat_context.messages_covered_until, capped at 100).

    `source` is purely for telemetry/idempotency bucketing; the prompt
    structure adapts to the chat_context state, not this argument.
    """
    chat_name = qone("SELECT name FROM chats WHERE id=?", (chat_id,)) or chat_id
    chat_type = qone("SELECT type FROM chats WHERE id=?", (chat_id,)) or "dm"
    participants = qone("SELECT participant_count FROM chats WHERE id=?", (chat_id,)) or 0

    ctx_rows = q(
        "SELECT summary, pending_action, urgency, messages_covered_until "
        "FROM chat_context WHERE chat_id=?", (chat_id,)
    )
    has_prior = bool(ctx_rows)
    if has_prior:
        prev_summary, prev_pending, _prev_urgency, prev_covered = ctx_rows[0]
    else:
        prev_summary, prev_pending, prev_covered = "", "", 0

    if has_prior:
        # limit defaults to 100 for boot/event; on-demand can pass higher.
        eff_limit = limit if limit is not None else 100
        msgs = q(
            "SELECT timestamp, sender_name, body, type, is_from_me, "
            "id, media_path, media_summary_done "
            "FROM messages WHERE chat_id=? AND timestamp > ? "
            "ORDER BY timestamp ASC LIMIT ?",
            (chat_id, prev_covered, eff_limit),
        )
    else:
        eff_limit = limit if limit is not None else 100
        msgs = q(
            "SELECT timestamp, sender_name, body, type, is_from_me, "
            "id, media_path, media_summary_done "
            "FROM messages WHERE chat_id=? "
            "ORDER BY timestamp DESC LIMIT ?",
            (chat_id, eff_limit),
        )[::-1]

    if not msgs:
        return  # nothing to process

    last_ts = max(m[0] for m in msgs)

    transcript_lines = []
    pending_media = []  # list of (msg_id, mtype, path) for the brain to summarize
    for ts, sn, body, mtype, ifm, mid, mpath, mdone in msgs:
        sender = "Arpit" if ifm else (sn or "?")
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
        if mpath and not mdone:
            text_part = f"[{mtype}] (path={mpath})"
            pending_media.append((mid, mtype, mpath))
        elif body:
            text_part = body
        elif mpath:
            # downloaded and summarized — but body was empty; rare
            text_part = f"[{mtype}]"
        else:
            text_part = f"[{mtype}]"
        transcript_lines.append(f"{when}  {sender}: {text_part}")
    transcript = "\n".join(transcript_lines)

    if pending_media:
        media_block_lines = ["", "Media files awaiting a one-line description:"]
        for mid, mtype, mpath in pending_media:
            media_block_lines.append(f"  - msg_id={mid} type={mtype} path={mpath}")
        media_block = "\n".join(media_block_lines)
    else:
        media_block = ""

    if has_prior:
        prior_block = (
            "Earlier you processed this chat. Your stored read:\n"
            f"  Summary: {prev_summary}\n"
            f"  Pending action: {prev_pending or '(none)'}\n"
            f"  Last message you covered: "
            f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(prev_covered)) if prev_covered else '(none)'}\n\n"
            "These messages have arrived since:"
        )
    else:
        prior_block = (
            "This chat has no prior summary in chat_context. "
            "Build the initial summary from these messages:"
        )

    header = f"WhatsApp chat: {chat_name}  |  id: {chat_id}  |  type: {chat_type}"
    if chat_type == "group" and participants:
        header += f"  |  {participants} members"

    body_text = f"""{header}

{prior_block}

{transcript}
{media_block}

Your tasks:

1. For each line in "Media files awaiting a one-line description" (if any),
   open the file at the given path, look at it, and persist a one-line
   description (max ~120 chars) for that message:

     python3 ~/.hermes/scripts/wa_set_message_body.py "<msg_id>" "<one-line description>"

   The description replaces the placeholder body for that message and marks
   media_summary_done=1 so future prompts read the description instead of the
   raw [image]/[video] tag.

2. Build (or update) the chat summary. Persist via:

     python3 ~/.hermes/scripts/wa_set_context.py "{chat_id}" "<1-2 sentence summary>" "<pending action or empty>" <urgency 0.0-1.0> {last_ts}

   Incorporate any media descriptions you wrote in step 1.

3. Update social memory + hindsight (DM chats only — skip for groups).
   You have full read/write access to ~/.hermes/social_memory.db and to
   hindsight (banks: hermes; bank_retain_mission already configured).

   For the contact behind this DM (resolve by chat name "{chat_name}"):

   a) Log this conversation as an interaction (always, when there's
      substantive content):

        python3 ~/.hermes/scripts/wa_relationship.py add-interaction \
          --contact "{chat_name}" \
          --what "<one-line description of the substantive thing in this batch>" \
          --vibe "<warm|romantic|caring|flirty|playful|practical|distant|tense|...>"

      This appends to recent_interactions and caps the JSON list at 10
      most-recent. If no profile exists yet, a skeleton is created. Skip
      this if nothing substantive happened (greetings only, ack-only, etc).

   b) If you learned something new and durable about their personality,
      life context, or how the relationship is evolving, refine the profile:

        python3 ~/.hermes/scripts/wa_relationship.py upsert \
          --contact "{chat_name}" \
          [--personality ...] [--life-context ...] \
          [--relationship-type ...] [--relationship-notes ...] \
          [--interaction-frequency ...] [--who-initiates ...] \
          [--shared-activities ...] \
          [--trajectory strengthening|stable|drifting|new]

      Only set fields that genuinely changed. Don't rewrite stable fields.

   c) Promote tier when interaction patterns warrant — meaningful sustained
      engagement, intimate or substantive content, mutual investment:

        python3 ~/.hermes/scripts/wa_relationship.py set-tier \
          --contact "{chat_name}" --tier inner_circle

      Demote when the relationship has clearly gone cold (weeks of silence,
      trajectory drifting, no engagement, contact no longer matters):

        python3 ~/.hermes/scripts/wa_relationship.py set-tier \
          --contact "{chat_name}" --tier known

   d) For durable life context that should outlast this single chat — life
      events, decisions, plans, important dates, situational changes,
      preferences — retain via your hindsight tools (per the
      bank_retain_mission already configured for the hermes bank).
      Do NOT retain: OTPs, transient logistics, one-off greetings, or
      anything stale within a week.

   e) Optionally edit ~/.hermes/memories/MEMORY.md if you discover a
      fundamental fact about Arpit's life that every future Hermes session
      should know. Be conservative — these notes load into every session.

   Skip step 3 entirely if this is a group chat or nothing in this batch is
   durable. None of (a)-(e) is mandatory; do what fits.

4. Decide whether to notify Arpit. If something needs his attention or
   would interest him, send a brief Melissa-voice note to Telegram chat
   341255489. If not, do nothing.

If you need more context (older messages, related chats, contacts), use the
wa-intel skill to query ~/wa-intel/data/wa-intel.db, social_memory.db, or
hindsight directly.

Mark this kanban task complete when all relevant steps are done.
"""

    # Idempotency: dedupe identical work (same chat at same last_ts) within the
    # same source+minute bucket. If the same chat fires twice in 60s with no new
    # messages between, the second create returns the existing task id.
    idem = f"wa-intel:{chat_id}:{last_ts}:{source}"
    title = f"WA[{source}] {(chat_name or chat_id)[:40]}"

    t0 = time.time()
    response = ""
    returncode = -1
    stderr_text = ""
    err = ""
    cmd = [
        "hermes", "kanban", "create", title,
        "--body", body_text,
        "--skill", "wa-intel",
        "--created-by", "wa-intel",
        "--max-runtime", "300",
        "--json",
    ]
    if not force:
        cmd.extend(["--idempotency-key", idem])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "HOME": "/root",
                 "PATH": "/root/.local/bin:/usr/local/bin:/usr/bin:/bin"}
        )
        response = result.stdout.strip()
        returncode = result.returncode
        stderr_text = (result.stderr or "").strip()
    except Exception as e:
        err = str(e)

    duration_ms = int((time.time() - t0) * 1000)
    enqueued = (returncode == 0) and not err

    # Distinguish a freshly-created task from an idempotency dedup hit.
    task_id = ""
    task_created_at = 0
    if response:
        try:
            parsed = json.loads(response)
            if isinstance(parsed, dict):
                task_id = parsed.get("id", "") or ""
                task_created_at = int(parsed.get("created_at") or 0)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    is_new_task = task_created_at >= int(t0) - 1  # within ~1s of when we called

    if err:
        print(f"  kanban error ({chat_name}): {err}")
    elif not enqueued:
        print(f"  kanban FAIL {chat_name}: rc={returncode} stderr={stderr_text[:120]}")
    elif is_new_task:
        print(f"  kanban+ {chat_name}: {source}, msgs={len(msgs)}, task={task_id} (new, {duration_ms}ms)")
    else:
        print(f"  kanban= {chat_name}: {source}, msgs={len(msgs)}, task={task_id} (existing dedup, {duration_ms}ms)")

    _log_hermes_call({
        "chat_id": chat_id,
        "chat_name": chat_name,
        "trigger": f"_enqueue_brain_task({source})",
        "msg_count": len(msgs),
        "prompt": body_text,
        "response": response,
        "returncode": returncode,
        "stderr": stderr_text,
        "error": err,
        "duration_ms": duration_ms,
        "suppressed": not enqueued,
        "kanban_idempotency_key": idem,
        "has_prior_summary": has_prior,
    })

    # Reset unread/notif state so triggers don't re-fire on the same batch.
    ex(
        "UPDATE chats SET unread_count=0, last_notified_at=? WHERE id=?",
        (int(time.time()), chat_id),
    )


def _feed_hermes(chat_id):
    """Backwards-compat entry point — now enqueues a kanban task."""
    _enqueue_brain_task(chat_id, source="event")



# === Poller ===
def wa_get(path):
    """GET <path> from WA_API. Returns parsed JSON or None on any failure.
    Stamps success/failure into the global health state for outage detection.
    """
    try:
        data = json.loads(urlopen(Request(f"{WA_API}{path}"), timeout=30).read())
    except Exception:
        with _WA_STATE_LOCK:
            global _WA_LAST_FAIL
            _WA_LAST_FAIL = int(time.time())
        return None
    with _WA_STATE_LOCK:
        global _WA_LAST_OK
        _WA_LAST_OK = int(time.time())
    return data

def _media_ext_from_mime(mt):
    """Pick a sane extension for a mimetype. Falls back to 'bin'."""
    if not mt:
        return "bin"
    main = mt.split(";", 1)[0].strip().lower()
    table = {
        "image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png",
        "image/webp": "webp", "image/gif": "gif", "image/heic": "heic",
        "video/mp4": "mp4", "video/webm": "webm", "video/quicktime": "mov",
        "audio/ogg": "ogg", "audio/mpeg": "mp3", "audio/mp4": "m4a",
        "audio/aac": "aac", "audio/amr": "amr",
        "application/pdf": "pdf",
    }
    if main in table:
        return table[main]
    if "/" in main:
        return main.split("/", 1)[1].split("+", 1)[0][:8] or "bin"
    return "bin"


def _safe_path_segment(s):
    """Sanitize an id for use as a filename component."""
    if not s:
        return "unknown"
    keep = []
    for ch in s:
        if ch.isalnum() or ch in "._-@":
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep)[:200]


def _download_media(chat_id, msg_id, mimetype):
    """Download media bytes for a message via the WA-HA download-media route.
    Saves to MEDIA_DIR/<chat>/<msg>.<ext> and stamps messages.media_path.
    Idempotent: skips if media_path already set or another thread is downloading.
    """
    key = (chat_id, msg_id)
    with _MEDIA_LOCK:
        if key in _MEDIA_INFLIGHT:
            return
        _MEDIA_INFLIGHT.add(key)
    try:
        # Skip if already on disk OR already summarized.
        # AND media_summary_done=1, no re-download — once the brain has
        # written the one-liner and we deleted the file, we don't pull it
        # again on the next event/poll. Re-fetching is a manual operation
        # via the /api/summarize endpoint.
        existing = q(
            "SELECT media_path, media_summary_done FROM messages WHERE id=?",
            (msg_id,),
        )
        if existing:
            mpath_now, mdone_now = existing[0]
            if mpath_now:
                return  # already downloaded
            if mdone_now:
                return  # already summarized; bytes were cleaned up

        fetch_chat = chat_id
        if "@lid" in chat_id:
            fetch_chat = resolve_lid(chat_id) or chat_id

        url = f"{WA_API}/api/chats/{fetch_chat}/messages/{msg_id}/download-media"
        try:
            resp = urlopen(Request(url), timeout=60)
            ct = resp.headers.get("Content-Type", "") or mimetype or ""
            data = resp.read()
        except URLError as e:
            print(f"  media-download FAIL [{chat_id}/{msg_id}]: {e}")
            return
        except Exception as e:
            print(f"  media-download FAIL [{chat_id}/{msg_id}]: {e}")
            return

        if not data:
            return
        ext = _media_ext_from_mime(ct)
        chat_dir = os.path.join(MEDIA_DIR, _safe_path_segment(chat_id))
        os.makedirs(chat_dir, exist_ok=True)
        local_path = os.path.join(chat_dir, _safe_path_segment(msg_id) + "." + ext)
        with open(local_path, "wb") as f:
            f.write(data)
        ex("UPDATE messages SET media_path=? WHERE id=?", (local_path, msg_id))
        print(f"  media+ [{chat_id}] {msg_id[:30]}... -> {local_path} ({len(data)} bytes)")
    finally:
        with _MEDIA_LOCK:
            _MEDIA_INFLIGHT.discard(key)


def _maybe_spawn_media_download(m, chat_id, msg_id):
    """If a message dict from the WA API indicates media, spawn an async
    download. Idempotent — _download_media re-checks DB.
    """
    if not m or not msg_id:
        return
    has_media = bool(m.get("hasMedia"))
    mt = m.get("mimetype") or m.get("_data", {}).get("mimetype") or ""
    mtype = m.get("type", "chat")
    if not has_media and mtype not in ("image", "video", "sticker", "audio", "ptt", "document"):
        return
    threading.Thread(
        target=_download_media,
        args=(chat_id, msg_id, mt),
        daemon=True,
    ).start()


def _fetch_chat_msgs(chat_id, limit=50):
    """Fetch up to `limit` messages from the WA API and upsert into messages.
    Returns count of new (non-duplicate) inserts.
    """
    fetch_id = chat_id
    if "@lid" in chat_id:
        fetch_id = resolve_lid(chat_id) or chat_id
    data = wa_get(f"/api/chats/{fetch_id}/messages?limit={limit}")
    if not data:
        return 0
    n_new = 0
    for m in data:
        mid = ""
        id_obj = m.get("id", m.get("_data", {}).get("id", {}))
        if isinstance(id_obj, dict):
            mid = id_obj.get("_serialized", "")
        if not mid:
            continue
        ts = int(m.get("timestamp", 0))
        sp = m.get("from", "") or m.get("author", "")
        sn = m.get("notifyName", m.get("pushName", ""))
        ifm = bool(m.get("fromMe", False))
        if not ifm and (sp in OWN_IDS or sn in OWN_IDS):
            ifm = True
        # LID_FIX_v1: store under canonical chat_id, not fetch_id.
        # fetch_id is only used to build the API URL.
        if store_message(mid, chat_id, sn, sp, ts, m.get("body", ""),
                         m.get("type", "chat"), ifm):
            n_new += 1
            if ifm:
                if sp: OWN_IDS.add(sp)
                if sn: OWN_IDS.add(sn)
            _maybe_spawn_media_download(m, chat_id, mid)
    ex("UPDATE chats SET last_polled_at=? WHERE id=?", (int(time.time()), chat_id))
    return n_new


def _event_sync(chat_id, is_new_chat):
    """On message event, refresh chat context from the WA API.

    - is_new_chat=True  -> fetch BOOT_SYNC_MSGS (default 1000) for full history.
    - is_new_chat=False -> fetch 50 to catch anything missed between events.

    Skipped during boot sync. Rate-limited per chat to once per 30s so a burst
    of events doesn't hammer the WA API.
    """
    if not _BOOT_SYNC_DONE.is_set():
        return
    last_polled = qone("SELECT last_polled_at FROM chats WHERE id=?", (chat_id,)) or 0
    if int(time.time()) - last_polled < 30:
        return
    limit = int(os.getenv("BOOT_SYNC_MSGS", "1000")) if is_new_chat else 50
    try:
        n = _fetch_chat_msgs(chat_id, limit=limit)
        if n:
            print(f"  event-sync [{chat_id}]: +{n} msgs (limit={limit}, new={is_new_chat})")
    except Exception as e:
        print(f"  event-sync error [{chat_id}]: {e}")


def _wa_health_loop():
    """Detect WA-API outages and trigger _recovery_sync on transition back.

    Cold-start safe: stays quiet until at least one successful wa_get has
    been observed. Re-emits a warning every WA_OUTAGE_REPEAT_SEC so a long
    outage is visible in the journal.
    """
    in_outage = False
    outage_start = 0
    last_warn = 0
    while True:
        time.sleep(30)
        now = int(time.time())
        with _WA_STATE_LOCK:
            last_ok = _WA_LAST_OK
            last_fail = _WA_LAST_FAIL
        if last_ok == 0:
            # haven't seen a success yet — could be cold start with WA-API
            # already down; skip outage logic until first success
            continue

        gap_since_ok = now - last_ok
        # Outage = at least one recent failure AND gap to last success exceeds threshold
        currently_down = (
            last_fail > last_ok
            and gap_since_ok > WA_OUTAGE_THRESHOLD_SEC
        )

        if currently_down and not in_outage:
            in_outage = True
            outage_start = last_ok  # last known good moment
            print(f"  WA-API OUTAGE: no successful response in {gap_since_ok}s; "
                  f"last ok at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_ok))}")
            last_warn = now
        elif currently_down and in_outage:
            if now - last_warn >= WA_OUTAGE_REPEAT_SEC:
                print(f"  WA-API STILL DOWN: {gap_since_ok}s since last success "
                      f"(outage started ~{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(outage_start))})")
                last_warn = now
        elif not currently_down and in_outage:
            outage_dur = last_ok - outage_start if outage_start else 0
            print(f"  WA-API RECOVERED after ~{outage_dur}s outage; triggering recovery sync")
            in_outage = False
            # Cooldown: avoid re-firing recovery on flapping connections
            global _WA_LAST_RECOVERY_AT
            with _WA_STATE_LOCK:
                if now - _WA_LAST_RECOVERY_AT < WA_RECOVERY_COOLDOWN:
                    print(f"  WA-API recovery sync skipped (within {WA_RECOVERY_COOLDOWN}s cooldown)")
                    outage_start = 0
                    continue
                _WA_LAST_RECOVERY_AT = now
            outage_start = 0
            threading.Thread(target=_recovery_sync, args=(outage_dur,), daemon=True).start()


def _recovery_sync(outage_dur):
    """Catch up on messages missed during a WA-API outage.

    Phase a) sync-history POST on top SYNC_HISTORY_N chats — asks WhatsApp
            servers to backfill the WW.js client's in-memory cache.
    Phase b) sleep BACKFILL_WAIT seconds for the cache to populate.
    Phase c) elevated fetch: top FETCH_CHATS × FETCH_LIMIT messages each.
    Phase d) for each chat with new inserts, enqueue a kanban task with
            source="recovery", force=True (bypass idempotency).
    """
    SYNC_HISTORY_N    = int(os.getenv("RECOVERY_SYNC_HISTORY_N", "100"))
    BACKFILL_WAIT     = int(os.getenv("RECOVERY_SYNC_BACKFILL_WAIT", "30"))
    FETCH_CHATS       = int(os.getenv("RECOVERY_FETCH_CHATS", "200"))
    FETCH_LIMIT       = int(os.getenv("RECOVERY_FETCH_LIMIT", "2000"))

    print(f"  recovery-sync: outage was ~{outage_dur}s; phase a — "
          f"sync-history on top {SYNC_HISTORY_N} chats")
    chats = q(
        "SELECT id FROM chats ORDER BY last_message_at DESC, rowid LIMIT ?",
        (SYNC_HISTORY_N,),
    )
    n_sh = 0
    for (cid,) in chats:
        fetch_id = cid
        if "@lid" in cid:
            fetch_id = resolve_lid(cid) or cid
        try:
            urlopen(
                Request(f"{WA_API}/api/chats/{fetch_id}/sync-history", method="POST"),
                timeout=10,
            ).read()
            n_sh += 1
        except Exception:
            pass
        time.sleep(0.1)
    print(f"  recovery-sync: phase a done — {n_sh}/{len(chats)} sync-history calls accepted; "
          f"sleeping {BACKFILL_WAIT}s for backfill")

    time.sleep(BACKFILL_WAIT)

    print(f"  recovery-sync: phase c — fetching top {FETCH_CHATS} chats x up to {FETCH_LIMIT} msgs")
    chats = q(
        "SELECT id FROM chats ORDER BY last_message_at DESC, rowid LIMIT ?",
        (FETCH_CHATS,),
    )
    affected = []
    t0 = time.time()
    for i, (cid,) in enumerate(chats, 1):
        try:
            n_new = _fetch_chat_msgs(cid, limit=FETCH_LIMIT)
            if n_new > 0:
                affected.append(cid)
        except Exception as e:
            print(f"  recovery-sync ingest error on {cid}: {e}")
        if i % 50 == 0 or i == len(chats):
            print(f"  recovery-sync progress: {i}/{len(chats)} chats, "
                  f"{len(affected)} with new msgs ({int(time.time()-t0)}s)")
        time.sleep(0.05)
    print(f"  recovery-sync: phase c done — {len(affected)} chats had new messages")

    print(f"  recovery-sync: phase d — enqueueing brain tasks (force=True) for {len(affected)} chats")
    n_enq = 0
    for cid in affected:
        try:
            _enqueue_brain_task(cid, source="recovery", force=True)
            n_enq += 1
        except Exception as e:
            print(f"  recovery-sync enqueue error for {cid}: {e}")
    print(f"  recovery-sync complete: {n_enq} kanban tasks created")


def _poll_loop():
    try:
        _sync_chats()
        _resolve_ids()
        _sync_messages()
        _sync_history_active()
    except Exception as e:
        print(f"  boot-sync fatal: {e}")
    finally:
        # Always release the gate so normal triggers can run, even on partial fail.
        _BOOT_SYNC_DONE.set()
    print(f"  poller: ready (interval={POLL_INTERVAL}s)")
    while True:
        time.sleep(POLL_INTERVAL)
        try:
            _sync_messages()
        except Exception as e:
            print(f"  poller error: {e}")

def _sync_chats():
    data = wa_get("/api/chats")
    if not data:
        return
    count = 0
    for c in data:
        cid = ""
        id_obj = c.get("id", {})
        if isinstance(id_obj, dict):
            cid = id_obj.get("_serialized", "")
        elif isinstance(id_obj, str):
            cid = id_obj
        if not cid:
            continue
        name = c.get("name", "")
        ctype = "group" if "@g.us" in cid else "dm"
        participants = 0
        gm = c.get("groupMetadata")
        if gm:
            participants = len(gm.get("participants", []))
            name = name or gm.get("subject", "")
        ex("INSERT OR IGNORE INTO chats (id) VALUES (?)", (cid,))
        # Extract last-message timestamp from the API response so ORDER BY
        # last_message_at works on a freshly-populated DB.
        last_t = 0
        try:
            lm = c.get("lastMessage")
            if isinstance(lm, dict):
                last_t = int(lm.get("t") or lm.get("timestamp") or 0)
            elif "t" in c:
                last_t = int(c.get("t") or 0)
        except (TypeError, ValueError):
            last_t = 0
        if last_t:
            ex("UPDATE chats SET last_message_at=MAX(last_message_at,?) WHERE id=?", (last_t, cid))
        if name:
            ex("UPDATE chats SET name=?, type=?, participant_count=? WHERE id=? AND name=''", (name, ctype, participants, cid))
            ex("UPDATE chats SET name=?, type=?, participant_count=? WHERE id=?", (name, ctype, participants, cid))
        count += 1
    print(f"  poller: {count} chats synced")

def _resolve_ids():
    lids = q("SELECT id FROM chats WHERE id LIKE '%@lid' LIMIT 200")
    resolved = 0
    for (lid,) in lids:
        if resolve_lid(lid):
            continue
        contact = wa_get(f"/api/contacts/{lid}")
        if contact and isinstance(contact.get("id"), dict):
            phone = contact["id"].get("_serialized", "")
            if phone and "@c.us" in phone:
                set_mapping(lid, phone, contact.get("name", ""))
                resolved += 1
        time.sleep(0.1)
    if resolved:
        print(f"  poller: resolved {resolved} LID mappings")

def _sync_messages():
    now = int(time.time())
    chats = q("SELECT id, last_polled_at FROM chats ORDER BY last_message_at DESC LIMIT 50")
    synced = 0
    for chat_id, last_polled in chats:
        if now - (last_polled or 0) < 60:
            continue
        fetch_id = chat_id
        if "@lid" in chat_id:
            fetch_id = resolve_lid(chat_id) or chat_id
        data = wa_get(f"/api/chats/{fetch_id}/messages?limit=100")
        if not data:
            continue
        for m in data:
            mid = ""
            id_obj = m.get("id", m.get("_data", {}).get("id", {}))
            if isinstance(id_obj, dict):
                mid = id_obj.get("_serialized", "")
            if not mid:
                continue
            # LID_FIX_v1: same — store under chat_id, not fetch_id.
            store_message(mid, chat_id, m.get("notifyName", m.get("pushName", "")),
                         m.get("from", ""), int(m.get("timestamp", 0)),
                         m.get("body", ""), m.get("type", "chat"),
                         m.get("fromMe", False))
        ex("UPDATE chats SET last_polled_at=? WHERE id=?", (now, chat_id))
        synced += 1
        time.sleep(0.2)
    if synced:
        print(f"  poller: messages synced for {synced} chats")

def _sync_history_active():
    """Boot-time aggressive sync.

    Phase 1 — ingest: top BOOT_SYNC_CHATS chats (default 500), up to
      BOOT_SYNC_MSGS each (default 1000). Brain wakes are gated off during
      this phase by _BOOT_SYNC_DONE.

    Phase 2 — sequential brain wakes: for each chat that received new messages
      this boot, call _feed_hermes synchronously. Context builds one chat at a
      time. After this phase _poll_loop sets _BOOT_SYNC_DONE and normal
      event-driven and silence-driven triggers resume.
    """
    boot_n_chats = int(os.getenv("BOOT_SYNC_CHATS", "500"))
    boot_n_msgs  = int(os.getenv("BOOT_SYNC_MSGS",  "1000"))
    chats = q("SELECT id FROM chats ORDER BY last_message_at DESC, rowid LIMIT ?", (boot_n_chats,))
    print(f"  boot-sync ingest: {len(chats)} chats x up to {boot_n_msgs} msgs")
    affected = []
    t0 = time.time()
    for i, (chat_id,) in enumerate(chats, 1):
        try:
            n_new = _fetch_chat_msgs(chat_id, limit=boot_n_msgs)
            if n_new > 0:
                affected.append(chat_id)
        except Exception as e:
            print(f"  boot-sync ingest error on {chat_id}: {e}")
        if i % 25 == 0 or i == len(chats):
            print(f"  boot-sync ingest progress: {i}/{len(chats)} chats, "
                  f"{len(affected)} with new msgs ({int(time.time()-t0)}s)")
        time.sleep(0.05)  # gentle throttle on the WA API
    print(f"  boot-sync ingest done in {int(time.time()-t0)}s; "
          f"{len(affected)} chats had new messages")

    # Phase 2 — enqueue a kanban task per chat with messages. Idempotent on
    # (chat_id, last_ts) so chats with no new content are deduped against prior
    # boot runs. Fast: ~100ms per enqueue, no LLM blocking here.
    boot_targets = q(
        "SELECT c.id FROM chats c "
        "WHERE EXISTS (SELECT 1 FROM messages WHERE chat_id=c.id) "
        "ORDER BY c.last_message_at DESC, c.rowid LIMIT ?",
        (boot_n_chats,),
    )
    print(f"  boot-sync enqueue: {len(boot_targets)} chats with messages")
    n_enqueued = 0
    for j, (chat_id,) in enumerate(boot_targets, 1):
        try:
            unproc_media = qone(
                "SELECT COUNT(*) FROM messages WHERE chat_id=? "
                "AND media_path!='' AND media_summary_done=0",
                (chat_id,),
            ) or 0
            _enqueue_brain_task(
                chat_id, source="boot",
                force=(unproc_media > 0),  # bypass idempotency for media recovery
            )
            n_enqueued += 1
        except Exception as e:
            print(f"  boot-sync enqueue error for {chat_id}: {e}")
        if j % 50 == 0 or j == len(boot_targets):
            print(f"  boot-sync enqueue progress: {j}/{len(boot_targets)}")
    print(f"  boot-sync enqueue done; {n_enqueued} kanban tasks created")

# === Ollama Ask (text-to-SQL + answer) ===
SCHEMA = """Tables in wa-intel.db:
- contacts(phone, name, relationship, notes, first_seen, last_seen)
- chats(id, name, type[dm/group], participant_count, last_message_at, unread_count)
- messages(id, chat_id, sender_name, sender_phone, timestamp, body, type[chat/image/video/reaction], is_from_me, media_path, reply_to_id)
- reactions(message_id, chat_id, reactor_name, reactor_phone, emoji, timestamp)
- chat_context(chat_id, summary, topics, pending_action, urgency)
Note: timestamps are unix epoch. Use datetime(timestamp,'unixepoch','localtime') for readable times.
Chat IDs: DMs are phone@c.us, groups are number@g.us."""

def ask_ollama(question):
    """Ask a question about WhatsApp data. Uses Ollama to generate SQL, executes it, then formulates answer."""
    if not question:
        return "No question provided."
    
    # Step 1: Generate SQL
    sql_prompt = f"""{SCHEMA}

User question: {question}

Write a SQLite query to answer this question. Return ONLY the SQL, nothing else. No markdown, no explanation."""
    
    try:
        import urllib.request
        req = urllib.request.Request("http://localhost:11434/api/generate",
            data=json.dumps({"model": "qwen2.5:7b", "prompt": sql_prompt, "stream": False}).encode(),
            headers={"Content-Type": "application/json"})
        resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
        sql = resp.get("response", "").strip()
        # Clean markdown fences if present
        sql = sql.replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        return f"LLM error: {e}"

    if not sql or not sql.upper().startswith("SELECT"):
        return f"Could not generate a valid query. LLM said: {sql[:200]}"

    # Step 2: Execute SQL
    try:
        rows = q(sql)
        if not rows:
            results_text = "No results found."
        else:
            results_text = "\n".join([str(r) for r in rows[:20]])
    except Exception as e:
        return f"SQL error: {e}. Query was: {sql}"

    # Step 3: Generate natural language answer
    answer_prompt = f"""User asked: {question}

SQL executed: {sql}

Results:
{results_text}

Based on these results, answer the user's question naturally and concisely. Be direct."""

    try:
        req = urllib.request.Request("http://localhost:11434/api/generate",
            data=json.dumps({"model": "qwen2.5:7b", "prompt": answer_prompt, "stream": False}).encode(),
            headers={"Content-Type": "application/json"})
        resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
        return resp.get("response", "").strip()
    except Exception as e:
        return f"Results: {results_text} (answer generation failed: {e})"

# === HTTP Server ===
def _summarize_on_demand(chat_id, limit=2000):
    """On-demand summary: refetch messages from the WA API (up to `limit`),
    then enqueue a kanban task with the same `limit` driving the transcript
    window.

    Used by POST /api/summarize/<chat_id>. Lets the brain build (or rebuild)
    a summary for any chat regardless of whether it was caught by boot sync.

    Returns a dict with what happened.
    """
    fetched = 0
    err = None
    try:
        fetched = _fetch_chat_msgs(chat_id, limit=limit)
    except Exception as e:
        err = str(e)

    total = qone("SELECT COUNT(*) FROM messages WHERE chat_id=?", (chat_id,)) or 0
    chat_name = qone("SELECT name FROM chats WHERE id=?", (chat_id,)) or chat_id

    enqueued = False
    if total > 0:
        try:
            _enqueue_brain_task(chat_id, source="ondemand", limit=limit)
            enqueued = True
        except Exception as e:
            err = (err + "; " if err else "") + f"enqueue: {e}"

    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "fetched": fetched,
        "total_messages": total,
        "enqueued": enqueued,
        "limit": limit,
        "error": err,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        if self.path == "/health":
            chats = qone("SELECT count(*) FROM chats") or 0
            msgs = qone("SELECT count(*) FROM messages") or 0
            self._json(200, {"status": "ok", "version": "4.0.0", "chats": chats, "messages": msgs})
        elif self.path == "/api/chats":
            rows = q("SELECT id,name,type,last_message_at,unread_count FROM chats ORDER BY last_message_at DESC LIMIT 100")
            self._json(200, [{"id":r[0],"name":r[1],"type":r[2],"last_message_at":r[3],"unread":r[4]} for r in rows])
        elif self.path.startswith("/api/wa/"):
            self._proxy_wa("GET")
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path in ("/events", "/webhooks/whatsapp-events"):
            threading.Thread(target=handle_event, args=(body,), daemon=True).start()
            self._json(200, {"ok": True})
        elif self.path.startswith("/api/summarize/"):
            # POST /api/summarize/<chat_id>?limit=N — fetch up to N messages
            # for this chat from the WA API, then enqueue a kanban task to
            # build/update the summary in chat_context.
            tail = self.path[len("/api/summarize/"):]
            chat_part, _, qs = tail.partition("?")
            chat_id = unquote(chat_part)
            try:
                lim = int(parse_qs(qs).get("limit", ["2000"])[0])
            except (TypeError, ValueError):
                lim = 2000
            lim = max(1, min(lim, 5000))  # clamp [1, 5000]
            result = _summarize_on_demand(chat_id, limit=lim)
            self._json(200, result)
        elif self.path.startswith("/api/sync/"):
            cid = self.path.split("/api/sync/")[1]
            threading.Thread(target=lambda: wa_get(f"/api/chats/{cid}/sync-history"), daemon=True).start()
            self._json(200, {"status": "syncing"})
        elif self.path.startswith("/webhooks/"):
            self._proxy_hermes(body)
        elif self.path == "/api/ask":
            result = ask_ollama(json.loads(body).get("question", ""))
            self._json(200, {"answer": result})
        elif self.path.startswith("/api/wa/"):
            self._proxy_wa("POST", body)
        else:
            self._json(404, {"error": "not found"})

    def _proxy_wa(self, method, body=None):
        path = self.path[len("/api/wa"):]
        url = f"{WA_API}/api{path}"
        try:
            req = Request(url, data=body, method=method)
            req.add_header("Content-Type", self.headers.get("Content-Type", "application/json"))
            r = urlopen(req, timeout=30)
            self.send_response(r.status)
            self.send_header("Content-Type", r.headers.get("Content-Type", "application/json"))
            self.end_headers()
            self.wfile.write(r.read())
        except URLError as e:
            self._json(502, {"error": str(e)})

    def _proxy_hermes(self, body):
        route = self.path.split("/webhooks/")[1]
        try:
            req = Request(f"http://localhost:8644/webhooks/{route}", data=body, method="POST")
            for h in ("Content-Type", "X-Webhook-Signature"):
                if self.headers.get(h):
                    req.add_header(h, self.headers[h])
            r = urlopen(req, timeout=10)
            self.send_response(r.status)
            self.end_headers()
            self.wfile.write(r.read())
        except Exception as e:
            self._json(502, {"error": str(e)})

    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

# === Main ===
if __name__ == "__main__":
    print(f"wa-intel v4 starting on :{LISTEN_PORT}")
    _seed_own_ids()
    threading.Thread(target=_poll_loop, daemon=True).start()
    threading.Thread(target=_silence_check_loop, daemon=True).start()
    threading.Thread(target=_wa_health_loop, daemon=True).start()
    server = HTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print(f"wa-intel v4 listening on :{LISTEN_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("shutting down")
