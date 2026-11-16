package wecomdify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var ErrUncertain = errors.New("send outcome is uncertain; automatic resend disabled")

type Message struct {
	ID       string `json:"msgid"`
	Account  string `json:"open_kfid"`
	Customer string `json:"external_userid"`
	Type     string `json:"msgtype"`
	Origin   int    `json:"origin"`
	Time     int64  `json:"send_time"`
	Text     struct {
		Content string `json:"content"`
	} `json:"text"`
}

type SyncPage struct {
	Cursor   string    `json:"next_cursor"`
	More     int       `json:"has_more"`
	Messages []Message `json:"msg_list"`
}

type Platform interface {
	Sync(context.Context, string, string, string) (SyncPage, error)
	State(context.Context, string, string) (int, error)
	Assistant(context.Context, string, string) error
	Send(context.Context, string, string, string, string) error
}

type Answer struct{ Text, Conversation string }
type Chat interface {
	Ask(context.Context, string, string, string) (Answer, error)
}

func httpClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func readResponse(response *http.Response, out any) error {
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("upstream HTTP status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024+1))
	if err != nil || len(raw) > 8*1024*1024 {
		return fmt.Errorf("invalid or oversized upstream response")
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("invalid upstream JSON")
	}
	return nil
}

type WeCom struct {
	Base, CorpID, Secret string
	HTTP                 *http.Client
	mu                   sync.Mutex
	token                string
	expires              time.Time
}

func NewWeCom(c Config) *WeCom {
	return &WeCom{Base: "https://qyapi.weixin.qq.com", CorpID: c.CorpID, Secret: c.Secret, HTTP: httpClient(c.RequestTimeout)}
}

func (w *WeCom) accessToken(ctx context.Context, refresh bool) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !refresh && w.token != "" && time.Now().Before(w.expires) {
		return w.token, nil
	}
	q := url.Values{"corpid": {w.CorpID}, "corpsecret": {w.Secret}}
	req, err := http.NewRequestWithContext(ctx, "GET", w.Base+"/cgi-bin/gettoken?"+q.Encode(), nil)
	if err != nil {
		return "", fmt.Errorf("invalid token endpoint")
	}
	response, err := w.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed")
	}
	var body struct {
		Token   string `json:"access_token"`
		Expires int    `json:"expires_in"`
		Code    int    `json:"errcode"`
	}
	if err = readResponse(response, &body); err != nil {
		return "", err
	}
	if body.Code != 0 || body.Token == "" || body.Expires <= 60 {
		return "", fmt.Errorf("token request rejected")
	}
	w.token = body.Token
	w.expires = time.Now().Add(time.Duration(body.Expires-60) * time.Second)
	return w.token, nil
}

func (w *WeCom) call(ctx context.Context, path string, body, out any, sending bool) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, err := w.accessToken(ctx, attempt == 1)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, "POST", w.Base+path+"?access_token="+url.QueryEscape(token), bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("invalid platform endpoint")
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := w.HTTP.Do(req)
		if err != nil {
			if sending {
				return ErrUncertain
			}
			return fmt.Errorf("platform request failed")
		}
		var raw json.RawMessage
		if err = readResponse(response, &raw); err != nil {
			if sending {
				return ErrUncertain
			}
			return err
		}
		var status struct {
			Code *int `json:"errcode"`
		}
		if json.Unmarshal(raw, &status) != nil || status.Code == nil {
			if sending {
				return ErrUncertain
			}
			return fmt.Errorf("platform response lacks status")
		}
		if (*status.Code == 40014 || *status.Code == 42001) && attempt == 0 {
			continue
		}
		if *status.Code != 0 {
			return fmt.Errorf("platform error code %d", *status.Code)
		}
		if out != nil && json.Unmarshal(raw, out) != nil {
			return fmt.Errorf("invalid platform response")
		}
		return nil
	}
	return fmt.Errorf("token refresh failed")
}

func (w *WeCom) Sync(ctx context.Context, account, token, cursor string) (SyncPage, error) {
	var raw json.RawMessage
	err := w.call(ctx, "/cgi-bin/kf/sync_msg", map[string]any{"open_kfid": account, "token": token, "cursor": cursor, "limit": 1000, "voice_format": 0}, &raw, false)
	if err != nil {
		return SyncPage{}, err
	}
	var result struct {
		Cursor   *string    `json:"next_cursor"`
		More     *int       `json:"has_more"`
		Messages *[]Message `json:"msg_list"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Cursor == nil || result.More == nil || result.Messages == nil {
		return SyncPage{}, fmt.Errorf("incomplete sync response")
	}
	return SyncPage{*result.Cursor, *result.More, *result.Messages}, nil
}
func (w *WeCom) State(ctx context.Context, account, customer string) (int, error) {
	var result struct {
		State *int `json:"service_state"`
	}
	err := w.call(ctx, "/cgi-bin/kf/service_state/get", map[string]string{"open_kfid": account, "external_userid": customer}, &result, false)
	if err != nil {
		return 0, err
	}
	if result.State == nil {
		return 0, fmt.Errorf("missing service state")
	}
	return *result.State, nil
}
func (w *WeCom) Assistant(ctx context.Context, account, customer string) error {
	return w.call(ctx, "/cgi-bin/kf/service_state/trans", map[string]any{"open_kfid": account, "external_userid": customer, "service_state": 1}, nil, false)
}
func (w *WeCom) Send(ctx context.Context, account, customer, id, text string) error {
	return w.call(ctx, "/cgi-bin/kf/send_msg", map[string]any{"open_kfid": account, "touser": customer, "msgid": id, "msgtype": "text", "text": map[string]string{"content": text}}, nil, true)
}

type Dify struct {
	Base, Key, Salt string
	HTTP            *http.Client
}

func NewDify(c Config) *Dify {
	return &Dify{Base: strings.TrimRight(c.DifyBase, "/"), Key: c.DifyKey, Salt: c.UserHashKey, HTTP: httpClient(c.RequestTimeout)}
}
func (d *Dify) Ask(ctx context.Context, user, query, conversation string) (Answer, error) {
	hash := hmac.New(sha256.New, []byte(d.Salt))
	hash.Write([]byte(user))
	data, _ := json.Marshal(map[string]any{"inputs": map[string]any{}, "query": query, "response_mode": "blocking", "conversation_id": conversation, "user": hex.EncodeToString(hash.Sum(nil))})
	req, err := http.NewRequestWithContext(ctx, "POST", d.Base+"/chat-messages", bytes.NewReader(data))
	if err != nil {
		return Answer{}, fmt.Errorf("invalid Dify endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+d.Key)
	req.Header.Set("Content-Type", "application/json")
	response, err := d.HTTP.Do(req)
	if err != nil {
		return Answer{}, fmt.Errorf("Dify request failed")
	}
	var result struct {
		Text         string `json:"answer"`
		Conversation string `json:"conversation_id"`
	}
	if err = readResponse(response, &result); err != nil {
		return Answer{}, err
	}
	if strings.TrimSpace(result.Text) == "" || result.Conversation == "" {
		return Answer{}, fmt.Errorf("Dify returned no answer or conversation")
	}
	return Answer{result.Text, result.Conversation}, nil
}
