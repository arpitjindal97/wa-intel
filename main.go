package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"wa-intel/internal/api"
	"wa-intel/internal/config"
	"wa-intel/internal/db"
	"wa-intel/internal/hermes"
	"wa-intel/internal/ingester"
	"wa-intel/internal/poller"
)

func main() {
	log.SetFlags(log.Ltime)

	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	store, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer store.Close()

	// Hermes session manager
	hermesSession := hermes.New(store, cfg)

	// Ingester with callback to Hermes
	ing := ingester.New(store, hermesSession.OnNewMessage)

	// Poller
	pol := poller.New(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background workers
	go pol.Start(ctx)
	go hermesSession.StartFeedLoop(ctx)

	// Start HTTP server
	srv := api.New(store, ing, pol, cfg)
	go func() {
		log.Printf("wa-intel v2 listening on %s", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("http: %v", err)
		}
	}()

	// Wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	cancel()
}
