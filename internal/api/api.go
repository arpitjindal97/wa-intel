package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"wa-intel/internal/config"
	"wa-intel/internal/db"
	"wa-intel/internal/ingester"
	"wa-intel/internal/poller"
)

type Server struct {
	store    *db.Store
	ingester *ingester.Ingester
	poller   *poller.Poller
	cfg      *config.Config
	srv      *http.Server
}

func New(store *db.Store, ing *ingester.Ingester, pol *poller.Poller, cfg *config.Config) *Server {
	mux := http.NewServeMux()
	s := &Server{store: store, ingester: ing, poller: pol, cfg: cfg}

	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /api/chats", s.handleListChats)
	mux.HandleFunc("GET /api/chats/{chatID}/messages", s.handleGetMessages)
	mux.HandleFunc("POST /api/sync/{chatID}", s.handleSync)
	mux.HandleFunc("POST /webhooks/{route}", s.handleWebhook)
	mux.HandleFunc("POST /events", s.handleEvents)
	mux.HandleFunc("/api/wa/{path...}", s.handleWAProxy)

	s.srv = &http.Server{Addr: cfg.Listen, Handler: mux}
	return s
}

func (s *Server) ListenAndServe() error { return s.srv.ListenAndServe() }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	chats, msgs := s.store.Stats()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"version":  "2.0.0",
		"chats":    chats,
		"messages": msgs,
	})
}

func (s *Server) handleListChats(w http.ResponseWriter, r *http.Request) {
	chats := s.store.ListChats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chats)
}

func (s *Server) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	chatID := r.PathValue("chatID")
	msgs := s.store.GetRecentMessages(chatID, 50)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	chatID := r.PathValue("chatID")
	go s.poller.SyncChat(chatID)
	json.NewEncoder(w).Encode(map[string]string{"status": "syncing", "chat_id": chatID})
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	route := r.PathValue("route")
	body, _ := io.ReadAll(r.Body)

	if route == "whatsapp-events" {
		go s.ingester.HandleEvent(body)
		w.WriteHeader(200)
		w.Write([]byte(`{"ok":true}`))
		return
	}

	// Proxy non-WA routes to Hermes
	hermesURL := fmt.Sprintf("%s/webhooks/%s", s.cfg.HermesGateway, route)
	req, _ := http.NewRequest("POST", hermesURL, bytes.NewReader(body))
	for k, v := range r.Header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	go s.ingester.HandleEvent(body)
	w.Write([]byte(`{"ok":true}`))
}

func (s *Server) handleWAProxy(w http.ResponseWriter, r *http.Request) {
	path := r.PathValue("path")
	targetURL := s.cfg.WhatsAppAPI + "/api/" + path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	var bodyReader io.Reader
	if r.Body != nil {
		bodyData, _ := io.ReadAll(r.Body)
		bodyReader = bytes.NewReader(bodyData)
	}

	proxyReq, err := http.NewRequest(r.Method, targetURL, bodyReader)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		proxyReq.Header.Set("Content-Type", ct)
	}

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("WhatsApp API error: %v", err), 502)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func init() {
	log.SetFlags(log.Ltime)
}
