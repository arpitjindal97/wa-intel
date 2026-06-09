package db

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

type Chat struct {
	ID               string
	Name             string
	Type             string
	ParticipantCount int
	IsInnerCircle    bool
	LastSyncedAt     int64
	LastMessageAt    int64
	LastFedAt        int64
	UnanalyzedCount  int
	IsMuted          bool
}

type Message struct {
	ID         string
	ChatID     string
	SenderID   string
	SenderName string
	Timestamp  int64
	Body       string
	Type       string
	IsFromMe   bool
	HasMedia   bool
	FedToHermes bool
}

func Open(path string) (*Store, error) {
	d, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	if err := migrate(d); err != nil {
		return nil, err
	}
	return &Store{db: d}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func migrate(d *sql.DB) error {
	_, err := d.Exec(`
	-- Add created_at column to messages if missing
	ALTER TABLE messages ADD COLUMN created_at INTEGER DEFAULT 0;

	-- Views for easy querying
	CREATE VIEW IF NOT EXISTS v_recent_chats AS
	SELECT 
		c.id,
		COALESCE(c.name, m.name, c.id) as display_name,
		c.type,
		c.last_message_at,
		c.unanalyzed_count,
		(SELECT body FROM messages WHERE chat_id = c.id ORDER BY timestamp DESC LIMIT 1) as last_message,
		(SELECT count(*) FROM messages WHERE chat_id = c.id) as message_count
	FROM chats c
	LEFT JOIN id_map m ON c.id = m.lid OR c.id = m.phone
	WHERE c.last_message_at > 0
	ORDER BY c.last_message_at DESC;

	CREATE VIEW IF NOT EXISTS v_chat_messages AS
	SELECT 
		m.chat_id,
		COALESCE(c.name, '') as chat_name,
		m.sender_name,
		m.body,
		m.timestamp,
		m.is_from_me,
		datetime(m.timestamp, 'unixepoch') as time_str
	FROM messages m
	LEFT JOIN chats c ON m.chat_id = c.id
	ORDER BY m.timestamp DESC;
	`)
	if err != nil {
		// Views might fail on first run if tables don't exist yet, ignore
		_ = err
	}

	_, err = d.Exec(`
	CREATE TABLE IF NOT EXISTS chats (
		id TEXT PRIMARY KEY,
		name TEXT DEFAULT '',
		type TEXT DEFAULT 'dm',
		participant_count INTEGER DEFAULT 0,
		is_inner_circle BOOLEAN DEFAULT 0,
		last_synced_at INTEGER DEFAULT 0,
		last_message_at INTEGER DEFAULT 0,
		last_fed_at INTEGER DEFAULT 0,
		unanalyzed_count INTEGER DEFAULT 0,
		is_muted BOOLEAN DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		chat_id TEXT NOT NULL,
		sender_id TEXT DEFAULT '',
		sender_name TEXT DEFAULT '',
		timestamp INTEGER NOT NULL,
		body TEXT DEFAULT '',
		type TEXT DEFAULT 'chat',
		is_from_me BOOLEAN DEFAULT 0,
		has_media BOOLEAN DEFAULT 0,
		fed_to_hermes BOOLEAN DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_msg_chat_ts ON messages(chat_id, timestamp);
	CREATE INDEX IF NOT EXISTS idx_msg_unfed ON messages(fed_to_hermes, timestamp);
	CREATE TABLE IF NOT EXISTS id_map (
		lid TEXT PRIMARY KEY,
		phone TEXT NOT NULL,
		name TEXT DEFAULT '',
		updated_at INTEGER DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_idmap_phone ON id_map(phone);
	CREATE TABLE IF NOT EXISTS sent_messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		chat_id TEXT,
		instruction TEXT,
		generated_message TEXT,
		sent_at INTEGER,
		wa_message_id TEXT
	);`)
	return err
}

// --- ID Map ---

func (s *Store) SetIDMapping(lid, phone, name string) {
	s.db.Exec(`INSERT INTO id_map (lid, phone, name, updated_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(lid) DO UPDATE SET phone=excluded.phone, name=excluded.name, updated_at=excluded.updated_at`,
		lid, phone, name, time.Now().Unix())
}

func (s *Store) ResolveLID(lid string) string {
	var phone string
	s.db.QueryRow(`SELECT phone FROM id_map WHERE lid = ?`, lid).Scan(&phone)
	return phone
}

func (s *Store) ResolvePhone(phone string) string {
	// Returns the phone itself — it's already canonical
	return phone
}

// --- Chats ---

func (s *Store) UpsertChat(id, name, chatType string, participants int) {
	s.db.Exec(`INSERT INTO chats (id, name, type, participant_count)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name=CASE WHEN excluded.name != '' THEN excluded.name ELSE chats.name END,
			type=excluded.type,
			participant_count=excluded.participant_count`,
		id, name, chatType, participants)
}

func (s *Store) InsertMessage(m Message) bool {
	res, err := s.db.Exec(`INSERT OR IGNORE INTO messages (id, chat_id, sender_id, sender_name, timestamp, body, type, is_from_me, has_media, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ChatID, m.SenderID, m.SenderName, m.Timestamp, m.Body, m.Type, m.IsFromMe, m.HasMedia, time.Now().Unix())
	if err != nil {
		return false
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		s.db.Exec(`UPDATE chats SET last_message_at = MAX(last_message_at, ?), unanalyzed_count = unanalyzed_count + 1 WHERE id = ?`, m.Timestamp, m.ChatID)
		return true
	}
	return false
}

func (s *Store) GetChat(id string) *Chat {
	c := &Chat{}
	err := s.db.QueryRow(`SELECT id, name, type, participant_count, is_inner_circle, last_synced_at, last_message_at, last_fed_at, unanalyzed_count, is_muted FROM chats WHERE id = ?`, id).
		Scan(&c.ID, &c.Name, &c.Type, &c.ParticipantCount, &c.IsInnerCircle, &c.LastSyncedAt, &c.LastMessageAt, &c.LastFedAt, &c.UnanalyzedCount, &c.IsMuted)
	if err != nil {
		return nil
	}
	return c
}

func (s *Store) GetActiveChats(since int64) []Chat {
	rows, _ := s.db.Query(`SELECT id, name, type, participant_count, is_inner_circle, last_synced_at, last_message_at, last_fed_at, unanalyzed_count, is_muted
		FROM chats WHERE last_message_at >= ? AND is_muted = 0 ORDER BY last_message_at DESC`, since)
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var chats []Chat
	for rows.Next() {
		var c Chat
		rows.Scan(&c.ID, &c.Name, &c.Type, &c.ParticipantCount, &c.IsInnerCircle, &c.LastSyncedAt, &c.LastMessageAt, &c.LastFedAt, &c.UnanalyzedCount, &c.IsMuted)
		chats = append(chats, c)
	}
	return chats
}

func (s *Store) GetUnfedMessages(chatID string, limit int) []Message {
	rows, _ := s.db.Query(`SELECT id, chat_id, sender_id, sender_name, timestamp, body, type, is_from_me, has_media
		FROM messages WHERE chat_id = ? AND fed_to_hermes = 0 ORDER BY timestamp ASC LIMIT ?`, chatID, limit)
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var msgs []Message
	for rows.Next() {
		var m Message
		rows.Scan(&m.ID, &m.ChatID, &m.SenderID, &m.SenderName, &m.Timestamp, &m.Body, &m.Type, &m.IsFromMe, &m.HasMedia)
		msgs = append(msgs, m)
	}
	return msgs
}

func (s *Store) GetRecentMessages(chatID string, limit int) []Message {
	rows, _ := s.db.Query(`SELECT id, chat_id, sender_id, sender_name, timestamp, body, type, is_from_me, has_media
		FROM messages WHERE chat_id = ? ORDER BY timestamp DESC LIMIT ?`, chatID, limit)
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var msgs []Message
	for rows.Next() {
		var m Message
		rows.Scan(&m.ID, &m.ChatID, &m.SenderID, &m.SenderName, &m.Timestamp, &m.Body, &m.Type, &m.IsFromMe, &m.HasMedia)
		msgs = append(msgs, m)
	}
	// Reverse to chronological
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs
}

func (s *Store) MarkFed(chatID string) {
	now := time.Now().Unix()
	s.db.Exec(`UPDATE messages SET fed_to_hermes = 1 WHERE chat_id = ? AND fed_to_hermes = 0`, chatID)
	s.db.Exec(`UPDATE chats SET last_fed_at = ?, unanalyzed_count = 0 WHERE id = ?`, now, chatID)
}

func (s *Store) UpdateSynced(chatID string) {
	s.db.Exec(`UPDATE chats SET last_synced_at = ? WHERE id = ?`, time.Now().Unix(), chatID)
}

func (s *Store) ListChats() []Chat {
	rows, _ := s.db.Query(`SELECT id, name, type, participant_count, is_inner_circle, last_synced_at, last_message_at, last_fed_at, unanalyzed_count, is_muted FROM chats ORDER BY last_message_at DESC`)
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var chats []Chat
	for rows.Next() {
		var c Chat
		rows.Scan(&c.ID, &c.Name, &c.Type, &c.ParticipantCount, &c.IsInnerCircle, &c.LastSyncedAt, &c.LastMessageAt, &c.LastFedAt, &c.UnanalyzedCount, &c.IsMuted)
		chats = append(chats, c)
	}
	return chats
}

func (s *Store) GetChatsNeedingFeed(batchThreshold int, silenceSeconds int64) []Chat {
	now := time.Now().Unix()
	rows, _ := s.db.Query(`SELECT id, name, type, participant_count, is_inner_circle, last_synced_at, last_message_at, last_fed_at, unanalyzed_count, is_muted
		FROM chats WHERE is_muted = 0 AND unanalyzed_count > 0
		AND (unanalyzed_count >= ? OR (? - last_message_at) >= ?)
		ORDER BY last_message_at DESC`, batchThreshold, now, silenceSeconds)
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var chats []Chat
	for rows.Next() {
		var c Chat
		rows.Scan(&c.ID, &c.Name, &c.Type, &c.ParticipantCount, &c.IsInnerCircle, &c.LastSyncedAt, &c.LastMessageAt, &c.LastFedAt, &c.UnanalyzedCount, &c.IsMuted)
		chats = append(chats, c)
	}
	return chats
}

func (s *Store) LogSent(chatID, instruction, message, waID string) {
	s.db.Exec(`INSERT INTO sent_messages (chat_id, instruction, generated_message, sent_at, wa_message_id) VALUES (?, ?, ?, ?, ?)`,
		chatID, instruction, message, time.Now().Unix(), waID)
}

func (s *Store) GetMessageByID(id string) string {
	var body string
	s.db.QueryRow("SELECT body FROM messages WHERE id = ?", id).Scan(&body)
	return body
}

func (s *Store) Stats() (chats, messages int) {
	s.db.QueryRow(`SELECT count(*) FROM chats`).Scan(&chats)
	s.db.QueryRow(`SELECT count(*) FROM messages`).Scan(&messages)
	return
}

func init() {
	log.SetFlags(log.Ltime)
}
