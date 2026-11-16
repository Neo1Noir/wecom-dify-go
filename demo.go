package wecomdify

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Demo struct {
	mu       sync.Mutex
	Replies  []map[string]string
	sequence int
}

func (*Demo) Sync(context.Context, string, string, string) (SyncPage, error) { return SyncPage{}, nil }
func (*Demo) State(context.Context, string, string) (int, error)             { return 1, nil }
func (*Demo) Assistant(context.Context, string, string) error                { return nil }
func (d *Demo) Send(_ context.Context, account, customer, id, text string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Replies = append(d.Replies, map[string]string{"customer": customer, "text": text, "message_id": id})
	return nil
}
func (*Demo) Ask(_ context.Context, user, query, conversation string) (Answer, error) {
	return Answer{Text: "[Demo, no model call] " + query, Conversation: "demo-conversation-" + digest(user)[:8]}, nil
}

func DemoConfig(path string) Config {
	return Config{Listen: "127.0.0.1:8080", StatePath: path, CorpID: "demo-corp", Secret: "demo-secret", CallbackToken: "demo-token", AESKey: base64.RawStdEncoding.EncodeToString(make([]byte, 32)), DifyBase: "http://127.0.0.1/v1", DifyKey: "demo-key", UserHashKey: "demo-only-hash-key", Workers: 4, IdleTimeout: time.Hour, RequestTimeout: time.Second, UnsupportedReply: "Please send text.", HandoffReply: "Contact support.", ErrorReply: "Assistant unavailable.", HumanKeywords: []string{"human"}}
}

// Handler exposes test-only routes; the command binds them to loopback in demo mode.
func (d *Demo) Handler(server *Server) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", server.Handler())
	mux.HandleFunc("POST /demo/send", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Customer string `json:"customer"`
			Text     string `json:"text"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Text == "" {
			http.Error(w, "text required", 400)
			return
		}
		if body.Customer == "" {
			body.Customer = "demo-customer"
		}
		d.mu.Lock()
		d.sequence++
		id := strconv.Itoa(d.sequence) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		d.mu.Unlock()
		message := Message{ID: id, Account: "demo-account", Customer: body.Customer, Origin: 3, Type: "text", Time: time.Now().Unix()}
		message.Text.Content = body.Text
		if err := server.Store.Ingest(Notification{Account: message.Account}, SyncPage{Messages: []Message{message}}); err != nil {
			http.Error(w, "store unavailable", 503)
			return
		}
		w.WriteHeader(202)
		json.NewEncoder(w).Encode(map[string]string{"message_id": id})
	})
	mux.HandleFunc("GET /demo/messages", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.Replies)
	})
	return mux
}
