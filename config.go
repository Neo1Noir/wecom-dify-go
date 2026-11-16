package wecomdify

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Listen, StatePath                          string
	CorpID, Secret, CallbackToken, AESKey      string
	DifyBase, DifyKey, UserHashKey             string
	Workers                                    int
	IdleTimeout, RequestTimeout                time.Duration
	UnsupportedReply, HandoffReply, ErrorReply string
	HumanKeywords                              []string
}

func FromEnv() (Config, error) {
	value := func(name, fallback string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return fallback
	}
	workers, err := strconv.Atoi(value("MAX_CONCURRENCY", "4"))
	if err != nil || workers < 1 || workers > 32 {
		return Config{}, fmt.Errorf("MAX_CONCURRENCY must be 1-32")
	}
	c := Config{Listen: value("LISTEN_ADDR", ":8080"), StatePath: value("SQLITE_PATH", "data/state.db"),
		CorpID: os.Getenv("WECOM_CORP_ID"), Secret: os.Getenv("WECOM_KF_SECRET"), CallbackToken: os.Getenv("WECOM_CALLBACK_TOKEN"), AESKey: os.Getenv("WECOM_ENCODING_AES_KEY"),
		DifyBase: os.Getenv("DIFY_API_BASE_URL"), DifyKey: os.Getenv("DIFY_APP_API_KEY"), UserHashKey: os.Getenv("DIFY_USER_HASH_KEY"), Workers: workers,
		IdleTimeout: time.Hour, RequestTimeout: 90 * time.Second,
		UnsupportedReply: value("UNSUPPORTED_REPLY", "Please send a text message; this assistant currently supports text only."),
		HandoffReply:     value("HANDOFF_REPLY", "Please contact your support team for a human representative."),
		ErrorReply:       value("ERROR_REPLY", "The assistant is temporarily unavailable. Please try again later."),
		HumanKeywords:    strings.Split(value("HUMAN_KEYWORDS", "human,agent,operator"), ",")}
	for name, v := range map[string]string{"WECOM_CORP_ID": c.CorpID, "WECOM_KF_SECRET": c.Secret, "WECOM_CALLBACK_TOKEN": c.CallbackToken, "WECOM_ENCODING_AES_KEY": c.AESKey, "DIFY_APP_API_KEY": c.DifyKey, "DIFY_USER_HASH_KEY": c.UserHashKey} {
		if strings.TrimSpace(v) == "" {
			return Config{}, fmt.Errorf("%s is required", name)
		}
	}
	key, e := base64.RawStdEncoding.DecodeString(c.AESKey)
	if e != nil || len(key) != 32 {
		return Config{}, fmt.Errorf("WECOM_ENCODING_AES_KEY must encode 32 bytes without padding")
	}
	u, e := url.Parse(c.DifyBase)
	if e != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.TrimRight(u.Path, "/") != "/v1" {
		return Config{}, fmt.Errorf("DIFY_API_BASE_URL must be http(s)://host[/prefix]/v1 without credentials or query")
	}
	if len(c.UserHashKey) < 16 {
		return Config{}, fmt.Errorf("DIFY_USER_HASH_KEY must contain at least 16 characters")
	}
	return c, nil
}
