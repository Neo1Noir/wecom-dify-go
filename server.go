package wecomdify

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Server struct {
	Config   Config
	Store    *Store
	Platform Platform
	Chat     Chat
	Log      *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.DB.PingContext(r.Context()); err != nil {
			http.Error(w, "not ready", 503)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	mux.HandleFunc("/callback", s.callback)
	return mux
}

func (s *Server) callback(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	query := r.URL.Query()
	timestamp, err := strconv.ParseInt(query.Get("timestamp"), 10, 64)
	if err != nil || time.Since(time.Unix(timestamp, 0)) > 10*time.Minute || time.Until(time.Unix(timestamp, 0)) > 10*time.Minute {
		http.Error(w, "invalid callback", 403)
		return
	}
	encrypted := query.Get("echostr")
	if r.Method == "POST" {
		var body struct {
			Encrypt string `xml:"Encrypt"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
		if xml.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, "invalid callback", 400)
			return
		}
		encrypted = body.Encrypt
	}
	clear, err := decryptCallback(s.Config, encrypted, query.Get("msg_signature"), query.Get("timestamp"), query.Get("nonce"))
	if err != nil {
		http.Error(w, "invalid callback", 403)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if r.Method == "GET" {
		w.Write(clear)
		return
	}
	var event struct {
		To      string `xml:"ToUserName"`
		Type    string `xml:"MsgType"`
		Event   string `xml:"Event"`
		Token   string `xml:"Token"`
		Account string `xml:"OpenKfId"`
	}
	if xml.Unmarshal(clear, &event) != nil || event.To != s.Config.CorpID {
		http.Error(w, "invalid event", 400)
		return
	}
	if event.Type == "event" && event.Event == "kf_msg_or_event" {
		if event.Account == "" || event.Token == "" || len(event.Account) > 256 || len(event.Token) > 8192 {
			http.Error(w, "incomplete event", 400)
			return
		}
		// Persist before acknowledgement; network/model work is asynchronous.
		if err = s.Store.Notify(event.Account, event.Token, digest(encrypted+"\x00"+query.Get("timestamp")+"\x00"+query.Get("nonce"))); err != nil {
			http.Error(w, "temporarily unavailable", 503)
			return
		}
	}
	fmt.Fprint(w, "success")
}

func (s *Server) syncNotification(ctx context.Context, n Notification) error {
	cursor, err := s.Store.Cursor(n.Account)
	if err != nil {
		return err
	}
	for pageNumber := 0; pageNumber < 100; pageNumber++ {
		page, err := s.Platform.Sync(ctx, n.Account, n.Token, cursor)
		if err != nil {
			return err
		}
		if (page.More != 0 && page.More != 1) || (page.More == 1 && (page.Cursor == "" || page.Cursor == cursor)) {
			return fmt.Errorf("invalid sync pagination")
		}
		if err = s.Store.Ingest(n, page); err != nil {
			return err
		}
		if page.More == 0 {
			return nil
		}
		cursor = page.Cursor
	}
	return fmt.Errorf("sync page limit reached")
}

func (s *Server) Run(ctx context.Context) error {
	if s.Config.Workers < 1 || s.Config.Workers > 32 {
		return fmt.Errorf("invalid worker limit")
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if err := s.Store.Recover(); err != nil {
		return err
	}
	var syncWG sync.WaitGroup
	syncWG.Add(1)
	go func() {
		defer syncWG.Done()
		timer := time.NewTicker(300 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				n, err := s.Store.NextNotification()
				if err == sql.ErrNoRows {
					continue
				}
				if err != nil {
					s.Log.Error("notification store unavailable")
					continue
				}
				if err = s.syncNotification(ctx, n); err != nil {
					s.Log.Warn("message sync incomplete", "notification", n.ID[:12])
					if e := s.Store.FailNotification(n); e != nil {
						s.Log.Error("cannot persist sync failure")
					}
				}
			}
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	done := make(chan string, s.Config.Workers)
	active := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			for len(active) > 0 {
				delete(active, <-done)
			}
			syncWG.Wait()
			return nil
		case key := <-done:
			delete(active, key)
		case <-ticker.C:
		}
		for len(active) < s.Config.Workers && ctx.Err() == nil {
			job, err := s.Store.Claim(active)
			if err == sql.ErrNoRows {
				break
			}
			if err != nil {
				s.Log.Error("cannot claim message")
				break
			}
			key := customerKey(job.Message)
			active[key] = true
			go func() { defer func() { done <- key }(); s.process(ctx, job) }()
		}
	}
}

func (s *Server) mark(job Job, state, reason string) bool {
	if err := s.Store.Mark(job.Row, state, reason); err != nil {
		s.Log.Error("cannot persist message state", "row", job.Row)
		return false
	}
	return true
}
func (s *Server) assistantState(ctx context.Context, m Message) (bool, error) {
	state, err := s.Platform.State(ctx, m.Account, m.Customer)
	if err != nil {
		return false, err
	}
	if state == 0 {
		if err = s.Platform.Assistant(ctx, m.Account, m.Customer); err != nil {
			return false, err
		}
		return true, nil
	}
	return state == 1, nil
}

var thoughts = regexp.MustCompile(`(?is)<think>.*?</think>`)
var unclosedThought = regexp.MustCompile(`(?is)<think>.*$`)

func replyParts(text string) []string {
	text = strings.TrimSpace(unclosedThought.ReplaceAllString(thoughts.ReplaceAllString(text, ""), ""))
	if text == "" {
		return nil
	}
	parts := []string{}
	for text != "" && len(parts) < 3 {
		n := min(1800, len(text))
		for n > 0 && !utf8.ValidString(text[:n]) {
			n--
		}
		if n == 0 {
			return nil
		}
		part := text[:n]
		text = text[n:]
		if len(parts) == 2 && text != "" {
			suffix := "\n[Answer truncated; please ask a narrower question.]"
			limit := 1800 - len(suffix)
			for !utf8.ValidString(part[:limit]) {
				limit--
			}
			part = part[:limit] + suffix
			text = ""
		}
		parts = append(parts, part)
	}
	return parts
}

func (s *Server) process(ctx context.Context, job Job) {
	m := job.Message
	if m.Time <= 0 || time.Since(time.Unix(m.Time, 0)) >= 48*time.Hour {
		s.mark(job, "skipped", "expired_message")
		return
	}
	ok, err := s.assistantState(ctx, m)
	if err != nil {
		if ctx.Err() != nil {
			s.mark(job, "pending", "shutdown_before_send")
		} else {
			s.mark(job, "failed", "service_state_unavailable")
		}
		return
	}
	if !ok {
		s.mark(job, "skipped", "human_or_ended_session")
		return
	}
	if job.Answer == "" {
		answer := Answer{}
		query := strings.TrimSpace(m.Text.Content)
		if m.Type != "text" || query == "" {
			answer.Text = s.Config.UnsupportedReply
		} else {
			human := false
			for _, word := range s.Config.HumanKeywords {
				if strings.EqualFold(query, strings.TrimSpace(word)) {
					human = true
				}
			}
			if human {
				answer.Text = s.Config.HandoffReply
			} else {
				conversation, e := s.Store.Conversation(customerKey(m), s.Config.IdleTimeout)
				if e != nil {
					s.mark(job, "failed", "conversation_store_unavailable")
					return
				}
				answer, err = s.Chat.Ask(ctx, customerKey(m), query, conversation)
				if err != nil {
					if ctx.Err() != nil {
						s.mark(job, "pending", "shutdown_during_model")
						return
					}
					answer = Answer{Text: s.Config.ErrorReply}
				}
			}
		}
		if len(replyParts(answer.Text)) == 0 {
			answer.Text = s.Config.ErrorReply
		}
		if err = s.Store.SaveAnswer(job, answer); err != nil {
			s.Log.Error("cannot persist generated answer", "row", job.Row)
			return
		}
		job.Answer = answer.Text
	}
	parts := replyParts(job.Answer)
	if len(parts) == 0 {
		s.mark(job, "failed", "empty_answer")
		return
	}
	for i := job.Sent; i < len(parts); i++ {
		ok, err = s.assistantState(ctx, m)
		if err != nil {
			s.mark(job, "failed", "service_state_unavailable")
			return
		}
		if !ok {
			s.mark(job, "skipped", "human_took_over")
			return
		}
		if !s.mark(job, "sending", "") {
			return
		}
		id := digest(m.Account + "\x00" + m.ID + "\x00" + strconv.Itoa(i))[:32]
		if err = s.Platform.Send(ctx, m.Account, m.Customer, id, parts[i]); err != nil {
			if errors.Is(err, ErrUncertain) {
				s.mark(job, "uncertain", "send_outcome_unknown")
			} else {
				s.mark(job, "failed", "send_rejected")
			}
			return
		}
		if err = s.Store.SentPart(job.Row, i+1); err != nil {
			s.Log.Error("send completed but receipt was not persisted", "row", job.Row)
			return
		}
	}
	s.mark(job, "sent", "")
}
