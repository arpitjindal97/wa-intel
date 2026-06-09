package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"wa-intel/internal/config"
	"wa-intel/internal/db"
)

type Poller struct {
	store  *db.Store
	cfg    *config.Config
	client *http.Client
}

func New(store *db.Store, cfg *config.Config) *Poller {
	return &Poller{
		store:  store,
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *Poller) Start(ctx context.Context) {
	// Initial sync
	log.Println("poller: initial chat sync + ID map build")
	p.syncChats()

	// Resolve LID→phone mappings
	log.Println("poller: resolving LID→phone mappings via contacts API")
	p.resolveIDMappings()

	// Immediate first message sync
	log.Println("poller: starting initial message sync")
	p.syncMessages()

	if p.cfg.Poller.SyncHistoryOnStart {
		p.syncHistoryForActiveChats()
	}

	// Periodic sync
	log.Printf("poller: starting periodic sync (chats=%s, msgs=%s)", p.cfg.Poller.ChatSyncInterval, p.cfg.Poller.MessageSyncInterval)
	chatTicker := time.NewTicker(p.cfg.Poller.ChatSyncInterval)
	msgTicker := time.NewTicker(p.cfg.Poller.MessageSyncInterval)
	defer chatTicker.Stop()
	defer msgTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-chatTicker.C:
			p.syncChats()
		case <-msgTicker.C:
			p.syncMessages()
		}
	}
}

func (p *Poller) syncChats() {
	resp, err := p.client.Get(p.cfg.WhatsAppAPI + "/api/chats")
	if err != nil {
		log.Printf("poller: chats fetch failed: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var chats []map[string]interface{}
	if err := json.Unmarshal(body, &chats); err != nil {
		log.Printf("poller: chats parse failed: %v", err)
		return
	}

	log.Printf("poller: synced %d chats from API", len(chats))

	for _, c := range chats {
		id := p.extractChatID(c)
		if id == "" {
			continue
		}

		name := getStr(c, "name")
		chatType := "dm"
		if strings.Contains(id, "@g.us") {
			chatType = "group"
		}

		participants := 0
		if gm, ok := c["groupMetadata"].(map[string]interface{}); ok {
			if ps, ok := gm["participants"].([]interface{}); ok {
				participants = len(ps)
			}
			if name == "" {
				name = getStr(gm, "subject")
			}
		}

		p.store.UpsertChat(id, name, chatType, participants)

		// Build ID map from chat data
		p.learnIDMapping(c, id)
	}
}

func (p *Poller) syncMessages() {
	cutoff := time.Now().Add(-p.cfg.Poller.ActiveChatWindow).Unix()
	chats := p.store.GetActiveChats(cutoff)

	// On first run or if no active chats found, sync top 50 chats by name
	if len(chats) == 0 {
		chats = p.store.ListChats()
		if len(chats) > 50 {
			chats = chats[:50]
		}
	}

	now := time.Now().Unix()
	synced := 0
	for _, c := range chats {
		// Only sync if not synced in last minute
		if now-c.LastSyncedAt < 60 {
			continue
		}
		// If chat is @lid, try to sync using the @c.us equivalent
		chatIDToFetch := c.ID
		if strings.Contains(c.ID, "@lid") {
			if resolved := p.store.ResolveLID(c.ID); resolved != "" {
				chatIDToFetch = resolved
			}
		}
		p.syncChatMessages(chatIDToFetch)
		p.store.UpdateSynced(c.ID)
		synced++
		time.Sleep(200 * time.Millisecond)
	}
	if synced > 0 {
		log.Printf("poller: synced messages for %d chats", synced)
	}
}

func (p *Poller) syncChatMessages(chatID string) {
	url := fmt.Sprintf("%s/api/chats/%s/messages?limit=50", p.cfg.WhatsAppAPI, chatID)
	resp, err := p.client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var msgs []map[string]interface{}
	if err := json.Unmarshal(body, &msgs); err != nil {
		return
	}

	stored := 0
	for _, m := range msgs {
		msgID := p.extractMsgID(m)
		if msgID == "" {
			continue
		}

		msg := db.Message{
			ID:         msgID,
			ChatID:     chatID,
			SenderID:   getStr(m, "from"),
			SenderName: getStr(m, "notifyName"),
			Body:       getStr(m, "body"),
			Type:       getStr(m, "type"),
			IsFromMe:   getBool(m, "fromMe"),
			HasMedia:   getBool(m, "hasMedia"),
		}
		if ts, ok := m["timestamp"].(float64); ok {
			msg.Timestamp = int64(ts)
		}
		if msg.SenderName == "" {
			msg.SenderName = getStr(m, "pushName")
		}

		if p.store.InsertMessage(msg) {
			stored++
		}
	}
}

func (p *Poller) syncHistoryForActiveChats() {
	cutoff := time.Now().Add(-p.cfg.Poller.ActiveChatWindow).Unix()
	chats := p.store.GetActiveChats(cutoff)

	limit := 20
	if len(chats) < limit {
		limit = len(chats)
	}

	for i := 0; i < limit; i++ {
		url := fmt.Sprintf("%s/api/chats/%s/sync-history", p.cfg.WhatsAppAPI, chats[i].ID)
		resp, err := p.client.Post(url, "application/json", nil)
		if err != nil {
			continue
		}
		resp.Body.Close()
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf("poller: triggered sync-history for %d chats", limit)
}

// SyncChat triggers a sync for a specific chat (called from API)
func (p *Poller) SyncChat(chatID string) {
	url := fmt.Sprintf("%s/api/chats/%s/sync-history", p.cfg.WhatsAppAPI, chatID)
	resp, err := p.client.Post(url, "application/json", nil)
	if err == nil {
		resp.Body.Close()
	}
	// Also fetch messages immediately
	p.syncChatMessages(chatID)
	p.store.UpdateSynced(chatID)
}

func (p *Poller) extractChatID(c map[string]interface{}) string {
	// Try id._serialized
	if idObj, ok := c["id"].(map[string]interface{}); ok {
		if ser := getStr(idObj, "_serialized"); ser != "" {
			return ser
		}
	}
	// Try flat id (string)
	if id := getStr(c, "id"); id != "" {
		return id
	}
	return ""
}

func (p *Poller) extractMsgID(m map[string]interface{}) string {
	if idObj, ok := m["id"].(map[string]interface{}); ok {
		if ser := getStr(idObj, "_serialized"); ser != "" {
			return ser
		}
	}
	// Nested in _data
	if data, ok := m["_data"].(map[string]interface{}); ok {
		if idObj, ok := data["id"].(map[string]interface{}); ok {
			if ser := getStr(idObj, "_serialized"); ser != "" {
				return ser
			}
		}
	}
	return getStr(m, "id")
}

func (p *Poller) learnIDMapping(chatData map[string]interface{}, phoneID string) {
	// The chat list returns both formats — learn the mapping
	if idObj, ok := chatData["id"].(map[string]interface{}); ok {
		remote := getStr(idObj, "_serialized")
		if remote != "" && strings.Contains(remote, "@lid") && strings.Contains(phoneID, "@c.us") {
			p.store.SetIDMapping(remote, phoneID, getStr(chatData, "name"))
		}
	}
}

func (p *Poller) resolveIDMappings() {
	chats := p.store.ListChats()
	resolved := 0
	for _, c := range chats {
		if !strings.Contains(c.ID, "@lid") {
			continue
		}
		// Already resolved?
		if existing := p.store.ResolveLID(c.ID); existing != "" {
			continue
		}
		// Call /api/contacts/{lid} to get the @c.us ID
		resp, err := p.client.Get(fmt.Sprintf("%s/api/contacts/%s", p.cfg.WhatsAppAPI, c.ID))
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var contact map[string]interface{}
		if err := json.Unmarshal(body, &contact); err != nil {
			continue
		}

		// Extract id._serialized which has the @c.us format
		if idObj, ok := contact["id"].(map[string]interface{}); ok {
			phone := getStr(idObj, "_serialized")
			if phone != "" && strings.Contains(phone, "@c.us") {
				name := getStr(contact, "name")
				p.store.SetIDMapping(c.ID, phone, name)
				resolved++
			}
		}
		time.Sleep(100 * time.Millisecond) // rate limit
	}
	if resolved > 0 {
		log.Printf("poller: resolved %d LID→phone mappings", resolved)
	}
}

func getStr(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	v, _ := m[key].(bool)
	return v
}
