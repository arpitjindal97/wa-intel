package ingester

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"wa-intel/internal/db"
)

type Ingester struct {
	store      *db.Store
	onNewMsg   func(chatID string) // callback when new message stored
}

func New(store *db.Store, onNewMsg func(string)) *Ingester {
	return &Ingester{store: store, onNewMsg: onNewMsg}
}

func (ing *Ingester) HandleEvent(data []byte) {
	var evt map[string]interface{}
	if err := json.Unmarshal(data, &evt); err != nil {
		log.Printf("ingester: json parse error: %v", err)
		return
	}

	eventType, _ := evt["event_type"].(string)

	// Skip non-message events
	switch eventType {
	case "whatsapp_authenticated", "whatsapp_disconnected":
		return
	case "whatsapp_message_ack":
		// ACKs are duplicates of sent messages, skip
		return
	case "whatsapp_message_reaction":
		ing.handleReaction(evt)
		return
	}

	// Extract message data from event_normaliser format
	// Format: {event_type, payload: {message: {body, from, to, id: {_serialized}, t, ...}}}
	msgData := ing.extractMessageData(evt)
	if msgData == nil {
		log.Printf("ingester: no message data in event_type=%s", eventType)
		return
	}

	// Extract fields
	body := getString(msgData, "body")
	fromMe := getBool(msgData, "fromMe")
	msgType := getString(msgData, "type")
	if msgType == "" {
		msgType = "chat"
	}

	// Resolve chat ID (always try to get @c.us format)
	chatID := ing.resolveChatID(msgData, fromMe)
	if chatID == "" {
		log.Printf("ingester: could not resolve chatID for event_type=%s", eventType)
		return
	}

	// Extract message ID
	msgID := ing.extractMsgID(msgData)
	if msgID == "" {
		msgID = fmt.Sprintf("%s_%d", chatID, time.Now().UnixNano())
	}

	// Extract timestamp
	var ts int64
	if t, ok := msgData["t"].(float64); ok {
		ts = int64(t)
	} else if t, ok := msgData["timestamp"].(float64); ok {
		ts = int64(t)
	} else {
		ts = time.Now().Unix()
	}

	// Extract sender
	senderID := getString(msgData, "author")
	if senderID == "" {
		senderID = getString(msgData, "from")
	}
	senderName := getString(msgData, "notifyName")
	if senderName == "" {
		senderName = getString(msgData, "pushName")
	}

	msg := db.Message{
		ID:         msgID,
		ChatID:     chatID,
		SenderID:   senderID,
		SenderName: senderName,
		Timestamp:  ts,
		Body:       body,
		Type:       msgType,
		IsFromMe:   fromMe,
		HasMedia:   getBool(msgData, "hasMedia"),
	}

	// Ensure chat exists
	ing.store.UpsertChat(chatID, "", "", 0)

	if ing.store.InsertMessage(msg) {
		log.Printf("ingester: stored [%s] %s: %s", chatID, senderName, truncate(body, 50))
		if ing.onNewMsg != nil {
			ing.onNewMsg(chatID)
		}
	}
}

func (ing *Ingester) extractMessageData(evt map[string]interface{}) map[string]interface{} {
	// Try payload.message (event_normaliser whatsapp_message_sent/received)
	if payload, ok := evt["payload"].(map[string]interface{}); ok {
		if msg, ok := payload["message"].(map[string]interface{}); ok {
			return msg
		}
		// payload might be the message itself (for received events)
		if payload["body"] != nil || payload["chatId"] != nil {
			return payload
		}
	}
	// Try payload_json.message
	if pj, ok := evt["payload_json"].(map[string]interface{}); ok {
		if msg, ok := pj["message"].(map[string]interface{}); ok {
			return msg
		}
		if pj["body"] != nil || pj["chatId"] != nil {
			return pj
		}
	}
	// Try data wrapper
	if d, ok := evt["data"].(map[string]interface{}); ok {
		return d
	}
	// Try flat (direct event)
	if evt["body"] != nil || evt["chatId"] != nil {
		return evt
	}
	return nil
}

func (ing *Ingester) resolveChatID(msgData map[string]interface{}, fromMe bool) string {
	// 1. Best source: id.remote — this is always the CHAT identifier
	//    (the other person in DMs, the group in groups)
	if idObj, ok := msgData["id"].(map[string]interface{}); ok {
		if remote := getString(idObj, "remote"); remote != "" {
			// Resolve if LID
			if strings.Contains(remote, "@c.us") || strings.Contains(remote, "@g.us") {
				return remote
			}
			if resolved := ing.store.ResolveLID(remote); resolved != "" {
				return resolved
			}
			return remote // store under LID, poller will fix
		}
	}

	// 2. Explicit chatId field (from direct events / non-event-normaliser)
	if cid := getString(msgData, "chatId"); cid != "" {
		if strings.Contains(cid, "@c.us") || strings.Contains(cid, "@g.us") {
			return cid
		}
		if resolved := ing.store.ResolveLID(cid); resolved != "" {
			return resolved
		}
		return cid
	}

	// 3. Fallback: use to (for sent) or from (for received)
	var target string
	if fromMe {
		target = getString(msgData, "to")
	} else {
		target = getString(msgData, "from")
	}
	if target == "" {
		target = getString(msgData, "to")
	}
	if target == "" {
		target = getString(msgData, "from")
	}
	if target == "" {
		return ""
	}

	if strings.Contains(target, "@c.us") || strings.Contains(target, "@g.us") {
		return target
	}
	if resolved := ing.store.ResolveLID(target); resolved != "" {
		return resolved
	}
	return target
}

func (ing *Ingester) extractMsgID(msgData map[string]interface{}) string {
	// Try id._serialized (nested object)
	if idObj, ok := msgData["id"].(map[string]interface{}); ok {
		if ser := getString(idObj, "_serialized"); ser != "" {
			return ser
		}
		if id := getString(idObj, "id"); id != "" {
			return id
		}
	}
	// Try flat id field
	if id := getString(msgData, "id"); id != "" {
		return id
	}
	return getString(msgData, "_serialized")
}

func (ing *Ingester) handleReaction(evt map[string]interface{}) {
	// Extract reaction data from payload
	payload, ok := evt["payload"].(map[string]interface{})
	if !ok {
		if pj, ok := evt["payload_json"].(map[string]interface{}); ok {
			payload = pj
		}
	}
	if payload == nil {
		return
	}

	reaction := getString(payload, "reaction")
	if reaction == "" {
		return // reaction removed, ignore
	}

	senderID := getString(payload, "senderId")
	timestamp := int64(0)
	if ts, ok := payload["timestamp"].(float64); ok {
		timestamp = int64(ts)
	}
	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	// Get the chat ID from msgId.remote
	chatID := ""
	if msgId, ok := payload["msgId"].(map[string]interface{}); ok {
		remote := getString(msgId, "remote")
		if remote != "" {
			if strings.Contains(remote, "@c.us") || strings.Contains(remote, "@g.us") {
				chatID = remote
			} else if resolved := ing.store.ResolveLID(remote); resolved != "" {
				chatID = resolved
			} else {
				chatID = remote
			}
		}
	}
	if chatID == "" {
		return
	}

	// Resolve sender name
	senderName := ""
	if senderID != "" && ing.store.ResolveLID(senderID) != "" {
		senderName = ing.store.ResolveLID(senderID)
	}

	// Look up the original message to provide context
	originalBody := ""
	if msgId, ok := payload["msgId"].(map[string]interface{}); ok {
		if ser := getString(msgId, "_serialized"); ser != "" {
			// Look up in our DB
			origMsgs := ing.store.GetMessageByID(ser)
			if origMsgs != "" {
				originalBody = origMsgs
			}
		}
	}

	// Skip reaction if we don't know what it's reacting to
	if originalBody == "" {
		return
	}

	// Create a message representing the reaction with context
	msgID := fmt.Sprintf("reaction_%s_%d", senderID, timestamp)
	body := fmt.Sprintf("reacted %s to: %q", reaction, originalBody)

	msg := db.Message{
		ID:         msgID,
		ChatID:     chatID,
		SenderID:   senderID,
		SenderName: senderName,
		Timestamp:  timestamp,
		Body:       body,
		Type:       "reaction",
		IsFromMe:   false,
		HasMedia:   false,
	}

	ing.store.UpsertChat(chatID, "", "", 0)
	if ing.store.InsertMessage(msg) {
		log.Printf("ingester: stored [%s] %s %s", chatID, senderName, body)
		if ing.onNewMsg != nil {
			ing.onNewMsg(chatID)
		}
	}
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
