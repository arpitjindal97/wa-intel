package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type PollerConfig struct {
	ChatSyncInterval    time.Duration `yaml:"chat_sync_interval"`
	MessageSyncInterval time.Duration `yaml:"message_sync_interval"`
	SyncHistoryOnStart  bool          `yaml:"sync_history_on_start"`
	ActiveChatWindow    time.Duration `yaml:"active_chat_window"`
}

type AnalysisConfig struct {
	FeedBatchThreshold int           `yaml:"feed_batch_threshold"`
	FeedSilenceTrigger time.Duration `yaml:"feed_silence_trigger"`
	FeedFreshThreshold time.Duration `yaml:"feed_fresh_threshold"`
}

type Config struct {
	WhatsAppAPI      string         `yaml:"whatsapp_api"`
	HermesGateway    string         `yaml:"hermes_gateway"`
	HermesBrainRoute string         `yaml:"hermes_brain_route"`
	HermesWebhookSecret string      `yaml:"hermes_webhook_secret"`
	Listen           string         `yaml:"listen"`
	DBPath           string         `yaml:"db_path"`
	Poller           PollerConfig   `yaml:"poller"`
	Analysis         AnalysisConfig `yaml:"analysis"`
	InnerCircle      []string       `yaml:"inner_circle"`
	MutedChats        []string       `yaml:"muted_chats"`
	Timezone         string         `yaml:"timezone"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		WhatsAppAPI:      "http://localhost:3000",
		HermesGateway:    "http://localhost:8644",
		HermesBrainRoute: "wa-intel-brain",
		Listen:           ":8090",
		DBPath:           "./data/wa-intel.db",
		Timezone:         "Europe/Amsterdam",
		Poller: PollerConfig{
			ChatSyncInterval:    5 * time.Minute,
			MessageSyncInterval: 1 * time.Minute,
			SyncHistoryOnStart:  true,
			ActiveChatWindow:    24 * time.Hour,
		},
		Analysis: AnalysisConfig{
			FeedBatchThreshold: 3,
			FeedSilenceTrigger: 2 * time.Minute,
			FeedFreshThreshold: 30 * time.Minute,
		},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
