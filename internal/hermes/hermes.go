package hermes

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"wa-intel/internal/config"
	"wa-intel/internal/db"
)

type Session struct {
	store *db.Store
	cfg   *config.Config
}

func New(store *db.Store, cfg *config.Config) *Session {
	return &Session{store: store, cfg: cfg}
}

// StartFeedLoop checks for chats needing analysis and feeds them to Hermes
func (s *Session) StartFeedLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.feedPendingChats()
		}
	}
}

func (s *Session) feedPendingChats() {
	silenceSec := int64(s.cfg.Analysis.FeedSilenceTrigger.Seconds())
	chats := s.store.GetChatsNeedingFeed(s.cfg.Analysis.FeedBatchThreshold, silenceSec)

	for _, chat := range chats {
		msgs := s.store.GetUnfedMessages(chat.ID, 20)
		if len(msgs) == 0 {
			continue
		}

		prompt := s.formatFeedPrompt(chat, msgs)
		response := s.callHermes(prompt)
		s.store.MarkFed(chat.ID)

		if response != "" && !strings.Contains(strings.ToUpper(response), "NO_ACTION") {
			log.Printf("hermes: notified about %s: %s", chat.Name, truncate(response, 80))
		} else {
			log.Printf("hermes: no action for %s (%d msgs)", chat.Name, len(msgs))
		}
	}
}

// OnNewMessage is called by ingester when a new message arrives
func (s *Session) OnNewMessage(chatID string) {
	chat := s.store.GetChat(chatID)
	if chat == nil || chat.IsMuted {
		return
	}

	now := time.Now().Unix()

	// Fresh message after long silence — feed immediately
	if chat.LastFedAt > 0 && (now-chat.LastFedAt) > int64(s.cfg.Analysis.FeedFreshThreshold.Seconds()) {
		msgs := s.store.GetUnfedMessages(chatID, 20)
		if len(msgs) > 0 {
			prompt := s.formatFeedPrompt(*chat, msgs)
			go func() {
				s.callHermes(prompt)
				s.store.MarkFed(chatID)
			}()
		}
		return
	}

	// Batch threshold reached
	if chat.UnanalyzedCount >= s.cfg.Analysis.FeedBatchThreshold {
		msgs := s.store.GetUnfedMessages(chatID, 20)
		if len(msgs) > 0 {
			prompt := s.formatFeedPrompt(*chat, msgs)
			go func() {
				s.callHermes(prompt)
				s.store.MarkFed(chatID)
			}()
		}
	}
}

func (s *Session) callHermes(prompt string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "hermes", "-z", "--continue", "wa-intel-brain", prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("hermes: cli error: %v (stderr: %s)", err, truncate(stderr.String(), 100))
		return ""
	}

	response := strings.TrimSpace(stdout.String())
	return response
}

func (s *Session) formatFeedPrompt(chat db.Chat, msgs []db.Message) string {
	var b strings.Builder

	chatName := chat.Name
	if chatName == "" {
		chatName = chat.ID
	}

	if chat.Type == "group" {
		b.WriteString(fmt.Sprintf("[WhatsApp — %s (Group, %d new)]\n", chatName, len(msgs)))
	} else {
		b.WriteString(fmt.Sprintf("[WhatsApp — %s (DM)]\n", chatName))
	}

	for _, m := range msgs {
		sender := m.SenderName
		if sender == "" {
			sender = m.SenderID
		}
		if m.IsFromMe {
			sender = "Arpit"
		}
		ts := time.Unix(m.Timestamp, 0).Format("15:04")
		body := m.Body
		if m.HasMedia && body == "" {
			body = fmt.Sprintf("[%s media]", m.Type)
		}
		if body == "" {
			continue
		}
		b.WriteString(fmt.Sprintf("%s %s: %s\n", ts, sender, body))
	}

	b.WriteString("---\n")
	lastMsg := msgs[len(msgs)-1]
	elapsed := time.Since(time.Unix(lastMsg.Timestamp, 0))
	if elapsed < time.Minute {
		b.WriteString("Just now")
	} else {
		b.WriteString(fmt.Sprintf("%s ago", elapsed.Round(time.Minute)))
	}
	hasReply := false
	for _, m := range msgs {
		if m.IsFromMe {
			hasReply = true
		}
	}
	if !hasReply && !msgs[len(msgs)-1].IsFromMe {
		b.WriteString(" | You haven't replied")
	}

	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
