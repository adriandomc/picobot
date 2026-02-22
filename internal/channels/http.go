package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/local/picobot/internal/chat"
)

// chatRequest is the JSON body expected by POST /chat.
type chatRequest struct {
	Message string `json:"message"`
	ChatID  string `json:"chat_id"`
}

// chatResponse is the JSON body returned by POST /chat.
type chatResponse struct {
	Reply string `json:"reply"`
}

// httpChannel manages pending HTTP requests waiting for agent replies.
type httpChannel struct {
	hub    *chat.Hub
	mu     sync.Mutex
	waiters map[string]chan string
}

// register stores a response channel for the given chat ID.
func (h *httpChannel) register(chatID string) chan string {
	ch := make(chan string, 1)
	h.mu.Lock()
	h.waiters[chatID] = ch
	h.mu.Unlock()
	return ch
}

// unregister removes the response channel for the given chat ID.
func (h *httpChannel) unregister(chatID string) {
	h.mu.Lock()
	delete(h.waiters, chatID)
	h.mu.Unlock()
}

// deliver sends a reply to the waiter for the given chat ID, if present.
func (h *httpChannel) deliver(chatID, reply string) {
	h.mu.Lock()
	ch, ok := h.waiters[chatID]
	h.mu.Unlock()
	if ok {
		select {
		case ch <- reply:
		default:
		}
	}
}

// runOutbound reads from the hub's "http" subscription and delivers replies to
// the waiting HTTP handlers.
func (h *httpChannel) runOutbound(ctx context.Context, outCh <-chan chat.Outbound) {
	for {
		select {
		case <-ctx.Done():
			return
		case out, ok := <-outCh:
			if !ok {
				return
			}
			h.deliver(out.ChatID, out.Content)
		}
	}
}

// ServeHTTP handles POST /chat requests.
func (h *httpChannel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Message == "" {
		http.Error(w, `"message" field is required`, http.StatusBadRequest)
		return
	}

	// Use provided chat_id or generate a unique one for this request.
	chatID := req.ChatID
	if chatID == "" {
		chatID = fmt.Sprintf("http-%d", time.Now().UnixNano())
	}

	replyCh := h.register(chatID)
	defer h.unregister(chatID)

	h.hub.In <- chat.Inbound{
		Channel:   "http",
		SenderID:  "http",
		ChatID:    chatID,
		Content:   req.Message,
		Timestamp: time.Now(),
	}

	// Wait up to 120 seconds for the agent to reply.
	select {
	case reply := <-replyCh:
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(chatResponse{Reply: reply}); err != nil {
			log.Printf("http: failed to encode response: %v", err)
		}
	case <-time.After(120 * time.Second):
		http.Error(w, "timeout waiting for agent reply", http.StatusGatewayTimeout)
	case <-r.Context().Done():
		// Client disconnected.
	}
}

// StartHTTP starts an HTTP server that exposes a POST /chat endpoint.
// Requests are forwarded to the agent via the chat hub and the reply is
// returned synchronously in the HTTP response.
//
// addr is the listen address, e.g. ":8080".
func StartHTTP(ctx context.Context, hub *chat.Hub, addr string) error {
	if addr == "" {
		addr = ":8080"
	}

	ch := &httpChannel{
		hub:     hub,
		waiters: make(map[string]chan string),
	}

	outCh := hub.Subscribe("http")
	go ch.runOutbound(ctx, outCh)

	mux := http.NewServeMux()
	mux.Handle("/chat", ch)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		log.Println("http: shutting down server")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()

	go func() {
		log.Printf("http: listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http: server error: %v", err)
		}
	}()

	return nil
}
