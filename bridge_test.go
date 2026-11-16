package wecomdify

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func encrypted(c Config, clear string) string {
	key, _ := base64.RawStdEncoding.DecodeString(c.AESKey)
	buffer := make([]byte, 20)
	binary.BigEndian.PutUint32(buffer[16:], uint32(len(clear)))
	buffer = append(buffer, []byte(clear)...)
	buffer = append(buffer, []byte(c.CorpID)...)
	pad := 32 - len(buffer)%32
	buffer = append(buffer, bytes.Repeat([]byte{byte(pad)}, pad)...)
	block, _ := aes.NewCipher(key)
	out := make([]byte, len(buffer))
	cipher.NewCBCEncrypter(block, key[:16]).CryptBlocks(out, buffer)
	return base64.StdEncoding.EncodeToString(out)
}

func callbackRequest(c Config, token string) *http.Request {
	clear := `<xml><ToUserName>` + c.CorpID + `</ToUserName><MsgType>event</MsgType><Event>kf_msg_or_event</Event><Token>` + token + `</Token><OpenKfId>account</OpenKfId></xml>`
	value := encrypted(c, clear)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	q := url.Values{"timestamp": {timestamp}, "nonce": {"nonce"}, "msg_signature": {signature(c.CallbackToken, timestamp, "nonce", value)}}
	return httptest.NewRequest("POST", "/callback?"+q.Encode(), strings.NewReader("<xml><Encrypt>"+value+"</Encrypt></xml>"))
}

func incoming(id, customer, text string) Message {
	m := Message{ID: id, Account: "account", Customer: customer, Type: "text", Origin: 3, Time: time.Now().Unix()}
	m.Text.Content = text
	return m
}

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DB.Close() })
	return s
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for state")
}

func runServer(t *testing.T, s *Server) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return cancel
}

func TestCallbackSignatureRecipientAndVerification(t *testing.T) {
	c := DemoConfig("")
	store := newStore(t)
	s := &Server{Config: c, Store: store}
	value := encrypted(c, "challenge")
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	q := url.Values{"echostr": {value}, "timestamp": {timestamp}, "nonce": {"n"}, "msg_signature": {signature(c.CallbackToken, timestamp, "n", value)}}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/callback?"+q.Encode(), nil))
	if w.Code != 200 || w.Body.String() != "challenge" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	q.Set("msg_signature", "wrong")
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/callback?"+q.Encode(), nil))
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	other := c
	other.CorpID = "different"
	value = encrypted(other, "challenge")
	if _, err := decryptCallback(c, value, signature(c.CallbackToken, timestamp, "n", value), timestamp, "n"); err == nil {
		t.Fatal("recipient mismatch accepted")
	}
}

func TestEncryptedCallbackToDifyToWeComAndDeduplication(t *testing.T) {
	c := DemoConfig("")
	store := newStore(t)
	var mu sync.Mutex
	var sends, queries []string
	var conversations []string
	syncCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			fmt.Fprint(w, `{"access_token":"fixture-token","expires_in":7200}`)
		case "/cgi-bin/kf/sync_msg":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			syncCalls++
			mu.Unlock()
			messages := []Message{}
			if body["cursor"] == "" {
				messages = []Message{incoming("one", "customer", "first"), incoming("one", "customer", "first"), incoming("two", "customer", "second")}
			}
			json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "next_cursor": "cursor-1", "has_more": 0, "msg_list": messages})
		case "/cgi-bin/kf/service_state/get":
			fmt.Fprint(w, `{"errcode":0,"service_state":1}`)
		case "/v1/chat-messages":
			if r.Header.Get("Authorization") != "Bearer fixture-app-key" {
				t.Error("missing Dify credential")
			}
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if len(body["user"].(string)) != 64 || strings.Contains(body["user"].(string), "customer") {
				t.Error("user ID was not hashed")
			}
			mu.Lock()
			queries = append(queries, body["query"].(string))
			conversations = append(conversations, body["conversation_id"].(string))
			mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"answer": "<think>private reasoning</think>Answer: " + body["query"].(string), "conversation_id": "conversation-1"})
		case "/cgi-bin/kf/send_msg":
			var body struct {
				ID   string `json:"msgid"`
				Text struct {
					Content string `json:"content"`
				} `json:"text"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			if len(body.ID) != 32 {
				t.Error("invalid outbound ID")
			}
			mu.Lock()
			sends = append(sends, body.Text.Content)
			mu.Unlock()
			fmt.Fprint(w, `{"errcode":0}`)
		default:
			t.Error("unexpected API route", r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer upstream.Close()
	c.DifyBase = upstream.URL + "/v1"
	c.DifyKey = "fixture-app-key"
	wc := NewWeCom(c)
	wc.Base = upstream.URL
	s := &Server{Config: c, Store: store, Platform: wc, Chat: NewDify(c), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	runServer(t, s)
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, callbackRequest(c, "pull-token"))
		if w.Code != 200 {
			t.Fatalf("callback %d", w.Code)
		}
	}
	waitFor(t, func() bool { counts, _ := store.Counts(); return counts["sent"] == 2 })
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(queries) != "[first second]" || fmt.Sprint(sends) != "[Answer: first Answer: second]" {
		t.Fatalf("queries=%v sends=%v", queries, sends)
	}
	if conversations[0] != "" || conversations[1] != "conversation-1" {
		t.Fatalf("conversations=%v", conversations)
	}
	if syncCalls != 1 {
		t.Fatal("duplicate callback was not deduplicated", syncCalls)
	}
}

type fakePlatform struct {
	mu        sync.Mutex
	sent      []string
	sendError error
	state     int
}

func (*fakePlatform) Sync(context.Context, string, string, string) (SyncPage, error) {
	return SyncPage{}, nil
}
func (f *fakePlatform) State(context.Context, string, string) (int, error) { return f.state, nil }
func (*fakePlatform) Assistant(context.Context, string, string) error      { return nil }
func (f *fakePlatform) Send(_ context.Context, account, customer, id, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, text)
	return f.sendError
}

type fakeChat struct {
	mu      sync.Mutex
	calls   int
	entered chan string
	release chan struct{}
}

func (f *fakeChat) Ask(ctx context.Context, user, query, conversation string) (Answer, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- user
		select {
		case <-f.release:
		case <-ctx.Done():
			return Answer{}, ctx.Err()
		}
	}
	return Answer{Text: "reply " + query, Conversation: "conv"}, nil
}

func TestUncertainSendBlocksCustomerUntilReconciled(t *testing.T) {
	store := newStore(t)
	c := DemoConfig("")
	platform := &fakePlatform{state: 1, sendError: ErrUncertain}
	chat := &fakeChat{}
	if err := store.Ingest(Notification{Account: "account"}, SyncPage{Messages: []Message{incoming("one", "user", "first"), incoming("two", "user", "second")}}); err != nil {
		t.Fatal(err)
	}
	s := &Server{Config: c, Store: store, Platform: platform, Chat: chat}
	runServer(t, s)
	waitFor(t, func() bool { counts, _ := store.Counts(); return counts["uncertain"] == 1 })
	time.Sleep(200 * time.Millisecond)
	chat.mu.Lock()
	calls := chat.calls
	chat.mu.Unlock()
	if calls != 1 {
		t.Fatal("later message overtook uncertain send")
	}
	platform.mu.Lock()
	platform.sendError = nil
	platform.mu.Unlock()
	if err := store.Resolve(1, "sent"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { counts, _ := store.Counts(); return counts["sent"] == 2 })
	platform.mu.Lock()
	defer platform.mu.Unlock()
	if len(platform.sent) != 2 {
		t.Fatal("uncertain response was resent")
	}
}

func TestDifferentCustomersCanRunConcurrently(t *testing.T) {
	store := newStore(t)
	c := DemoConfig("")
	c.Workers = 2
	chat := &fakeChat{entered: make(chan string, 2), release: make(chan struct{})}
	platform := &fakePlatform{state: 1}
	store.Ingest(Notification{Account: "account"}, SyncPage{Messages: []Message{incoming("one", "a", "first"), incoming("two", "b", "second")}})
	runServer(t, &Server{Config: c, Store: store, Platform: platform, Chat: chat})
	for i := 0; i < 2; i++ {
		select {
		case <-chat.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("customers were serialized globally")
		}
	}
	close(chat.release)
	waitFor(t, func() bool { counts, _ := store.Counts(); return counts["sent"] == 2 })
}

func TestRestartRecoveryPreservesAnswerAndUnknownOutcome(t *testing.T) {
	store := newStore(t)
	store.Ingest(Notification{Account: "account"}, SyncPage{Messages: []Message{incoming("a", "one", "first"), incoming("b", "two", "second")}})
	store.DB.Exec(`UPDATE messages SET state='processing',answer='saved reply' WHERE id=1`)
	store.DB.Exec(`UPDATE messages SET state='sending' WHERE id=2`)
	if err := store.Recover(); err != nil {
		t.Fatal(err)
	}
	counts, _ := store.Counts()
	if counts["pending"] != 1 || counts["uncertain"] != 1 {
		t.Fatal(counts)
	}
	chat := &fakeChat{}
	p := &fakePlatform{state: 1}
	runServer(t, &Server{Config: DemoConfig(""), Store: store, Platform: p, Chat: chat})
	waitFor(t, func() bool { counts, _ := store.Counts(); return counts["sent"] == 1 })
	chat.mu.Lock()
	defer chat.mu.Unlock()
	if chat.calls != 0 {
		t.Fatal("persisted answer was regenerated")
	}
}

func TestHumanStateAndUnsupportedTypesDoNotCallDify(t *testing.T) {
	for _, state := range []int{2, 3, 4} {
		store := newStore(t)
		m := incoming("one", "user", "question")
		store.Ingest(Notification{Account: "account"}, SyncPage{Messages: []Message{m}})
		job, _ := store.Claim(map[string]bool{})
		chat := &fakeChat{}
		s := &Server{Config: DemoConfig(""), Store: store, Platform: &fakePlatform{state: state}, Chat: chat, Log: slog.Default()}
		s.process(context.Background(), job)
		if chat.calls != 0 {
			t.Fatal("human session was interrupted")
		}
	}
}

func TestReplySplittingIsUTF8SafeAndHidesReasoning(t *testing.T) {
	for _, text := range []string{"🙂", strings.Repeat("🙂", 2000), "<think>secret</think>Visible", "<think>unfinished"} {
		parts := replyParts(text)
		if len(parts) > 3 {
			t.Fatal("too many replies")
		}
		for _, part := range parts {
			if !utf8.ValidString(part) || len(part) > 1800 || strings.Contains(part, "secret") {
				t.Fatalf("invalid reply %q", part)
			}
		}
	}
	if got := replyParts("🙂"); len(got) != 1 || got[0] != "🙂" {
		t.Fatal(got)
	}
	if len(replyParts("<think>unfinished")) != 0 {
		t.Fatal("reasoning leaked")
	}
}

func TestUnknownHTTPResponseIsNotRetriedAsASend(t *testing.T) {
	sends := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cgi-bin/gettoken" {
			fmt.Fprint(w, `{"access_token":"token","expires_in":7200}`)
			return
		}
		sends++
		http.Error(w, "ambiguous gateway failure", 502)
	}))
	defer up.Close()
	w := NewWeCom(DemoConfig(""))
	w.Base = up.URL
	if err := w.Send(context.Background(), "account", "user", "id", "answer"); err != ErrUncertain {
		t.Fatal(err)
	}
	if sends != 1 {
		t.Fatal("ambiguous send retried")
	}
}

func TestNotificationIdentityDoesNotAssumeUniquePullTokens(t *testing.T) {
	store := newStore(t)
	if err := store.Notify("account", "same-pull-token", "event-1"); err != nil {
		t.Fatal(err)
	}
	store.Notify("account", "same-pull-token", "event-1")
	store.Notify("account", "same-pull-token", "event-2")
	var n int
	if err := store.DB.QueryRow(`SELECT count(*) FROM notifications`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected two distinct callbacks and one deduplicated retry, got %d", n)
	}
}

func TestUnsupportedAndHumanCommandsBypassModel(t *testing.T) {
	for _, kind := range []string{"image", "human"} {
		store := newStore(t)
		message := incoming("one", "user", "human")
		if kind == "image" {
			message.Type = "image"
		}
		store.Ingest(Notification{Account: "account"}, SyncPage{Messages: []Message{message}})
		job, err := store.Claim(map[string]bool{})
		if err != nil {
			t.Fatal(err)
		}
		chat := &fakeChat{}
		platform := &fakePlatform{state: 1}
		server := &Server{Config: DemoConfig(""), Store: store, Platform: platform, Chat: chat, Log: slog.Default()}
		server.process(context.Background(), job)
		if chat.calls != 0 || len(platform.sent) != 1 {
			t.Fatalf("%s called the model or failed to reply", kind)
		}
	}
}
