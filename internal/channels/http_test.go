package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/local/picobot/internal/chat"
)

// simulateAgent reads one inbound message from the hub and writes back a reply.
func simulateAgent(hub *chat.Hub) {
	go func() {
		msg := <-hub.In
		hub.Out <- chat.Outbound{
			Channel: msg.Channel,
			ChatID:  msg.ChatID,
			Content: "hello back",
		}
	}()
}

func TestHTTPChannel_PostChat(t *testing.T) {
	hub := chat.NewHub(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := StartHTTP(ctx, hub, ""); err != nil {
		t.Fatalf("StartHTTP failed: %v", err)
	}
	hub.StartRouter(ctx)

	simulateAgent(hub)

	body, _ := json.Marshal(chatRequest{Message: "hi"})
	req := httptest.NewRequest(http.MethodPost, "/chat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	hc := &httpChannel{hub: hub, waiters: make(map[string]chan string)}
	outCh := hub.Subscribe("http-test")
	go hc.runOutbound(ctx, outCh)

	// We test ServeHTTP directly against an httpChannel that is wired to the hub.
	// Re-create so the subscription matches the channel name used inside ServeHTTP.
	hc2 := &httpChannel{hub: hub, waiters: make(map[string]chan string)}
	// Subscribe before the handler runs so the reply is not lost.
	httpOutCh := hub.Subscribe("http")
	go hc2.runOutbound(ctx, httpOutCh)

	hc2.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp chatResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Reply != "hello back" {
		t.Fatalf("unexpected reply: %q", resp.Reply)
	}
}

func TestHTTPChannel_MethodNotAllowed(t *testing.T) {
	hub := chat.NewHub(10)
	hc := &httpChannel{hub: hub, waiters: make(map[string]chan string)}

	req := httptest.NewRequest(http.MethodGet, "/chat", nil)
	rec := httptest.NewRecorder()
	hc.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHTTPChannel_MissingMessage(t *testing.T) {
	hub := chat.NewHub(10)
	hc := &httpChannel{hub: hub, waiters: make(map[string]chan string)}

	body := strings.NewReader(`{"chat_id":"abc"}`)
	req := httptest.NewRequest(http.MethodPost, "/chat", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	hc.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHTTPChannel_InvalidJSON(t *testing.T) {
	hub := chat.NewHub(10)
	hc := &httpChannel{hub: hub, waiters: make(map[string]chan string)}

	req := httptest.NewRequest(http.MethodPost, "/chat", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	hc.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHTTPChannel_ProvidesChatID(t *testing.T) {
	hub := chat.NewHub(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hc := &httpChannel{hub: hub, waiters: make(map[string]chan string)}
	outCh := hub.Subscribe("http")
	go hc.runOutbound(ctx, outCh)
	hub.StartRouter(ctx)

	// Agent echoes back the provided chat_id.
	go func() {
		msg := <-hub.In
		hub.Out <- chat.Outbound{
			Channel: "http",
			ChatID:  msg.ChatID,
			Content: "pong",
		}
	}()

	body, _ := json.Marshal(chatRequest{Message: "ping", ChatID: "my-chat-42"})
	req := httptest.NewRequest(http.MethodPost, "/chat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		hc.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for handler")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp chatResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Reply != "pong" {
		t.Fatalf("unexpected reply: %q", resp.Reply)
	}
}
