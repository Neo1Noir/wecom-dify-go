package wecomdify

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

func signature(token, timestamp, nonce, encrypted string) string {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	sum := sha1.Sum([]byte(strings.Join(values, "")))
	return hex.EncodeToString(sum[:])
}

func decryptCallback(c Config, encrypted, sig, timestamp, nonce string) ([]byte, error) {
	if sig == "" || timestamp == "" || nonce == "" || subtle.ConstantTimeCompare([]byte(signature(c.CallbackToken, timestamp, nonce, encrypted)), []byte(sig)) != 1 {
		return nil, fmt.Errorf("invalid callback signature")
	}
	key, err := base64.RawStdEncoding.DecodeString(c.AESKey)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("invalid callback key")
	}
	raw, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil || len(raw) == 0 || len(raw)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("invalid callback ciphertext")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	clear := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, key[:16]).CryptBlocks(clear, raw)
	pad := int(clear[len(clear)-1])
	if pad < 1 || pad > 32 || pad > len(clear) {
		return nil, fmt.Errorf("invalid callback padding")
	}
	for _, b := range clear[len(clear)-pad:] {
		if int(b) != pad {
			return nil, fmt.Errorf("invalid callback padding")
		}
	}
	clear = clear[:len(clear)-pad]
	if len(clear) < 20 {
		return nil, fmt.Errorf("short callback")
	}
	n := int(binary.BigEndian.Uint32(clear[16:20]))
	if n > len(clear)-20 || string(clear[20+n:]) != c.CorpID {
		return nil, fmt.Errorf("callback recipient mismatch")
	}
	return clear[20 : 20+n], nil
}
