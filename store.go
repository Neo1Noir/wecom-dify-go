package wecomdify

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct{ DB *sql.DB }
type Notification struct {
	ID, Account, Token string
	Attempts           int
}
type Job struct {
	Row           int64
	Message       Message
	State, Answer string
	Sent          int
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func customerKey(message Message) string { return message.Account + "\x00" + message.Customer }

func OpenStore(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS notifications(id TEXT PRIMARY KEY,account TEXT NOT NULL,token TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS cursors(account TEXT PRIMARY KEY,cursor TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT,account TEXT NOT NULL,msgid TEXT NOT NULL,customer TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',answer TEXT NOT NULL DEFAULT '',sent INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',UNIQUE(account,msgid));
CREATE TABLE IF NOT EXISTS conversations(customer TEXT PRIMARY KEY,conversation TEXT NOT NULL,updated INTEGER NOT NULL);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}

func (s *Store) Notify(account, token, eventID string) error {
	_, err := s.DB.Exec(`INSERT OR IGNORE INTO notifications(id,account,token) VALUES(?,?,?)`, eventID, account, token)
	return err
}
func (s *Store) NextNotification() (Notification, error) {
	var n Notification
	err := s.DB.QueryRow(`SELECT id,account,token,attempts FROM notifications WHERE state='pending' AND next_at<=? ORDER BY rowid LIMIT 1`, time.Now().Unix()).Scan(&n.ID, &n.Account, &n.Token, &n.Attempts)
	return n, err
}
func (s *Store) FailNotification(n Notification) error {
	state := "pending"
	if n.Attempts >= 4 {
		state = "failed"
	}
	_, err := s.DB.Exec(`UPDATE notifications SET state=?,attempts=attempts+1,next_at=? WHERE id=?`, state, time.Now().Add(time.Duration(1<<min(n.Attempts+1, 6))*time.Second).Unix(), n.ID)
	return err
}
func (s *Store) Cursor(account string) (string, error) {
	var cursor string
	err := s.DB.QueryRow(`SELECT cursor FROM cursors WHERE account=?`, account).Scan(&cursor)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return cursor, err
}
func (s *Store) Ingest(n Notification, page SyncPage) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, message := range page.Messages {
		if message.Account == "" {
			message.Account = n.Account
		}
		if message.ID == "" || message.Customer == "" || message.Account != n.Account || message.Origin != 3 || message.Type == "event" {
			continue
		}
		raw, err := json.Marshal(message)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT OR IGNORE INTO messages(account,msgid,customer,payload) VALUES(?,?,?,?)`, message.Account, message.ID, message.Customer, string(raw)); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`INSERT INTO cursors(account,cursor) VALUES(?,?) ON CONFLICT(account) DO UPDATE SET cursor=excluded.cursor`, n.Account, page.Cursor); err != nil {
		return err
	}
	if page.More == 0 {
		if _, err = tx.Exec(`UPDATE notifications SET state='done',token='' WHERE id=?`, n.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) Claim(active map[string]bool) (Job, error) {
	rows, err := s.DB.Query(`SELECT m.id,m.payload,m.state,m.answer,m.sent FROM messages m WHERE m.state IN ('pending','ready') AND NOT EXISTS(SELECT 1 FROM messages earlier WHERE earlier.account=m.account AND earlier.customer=m.customer AND earlier.id<m.id AND earlier.state IN ('pending','ready','processing','sending','uncertain')) ORDER BY m.id LIMIT 64`)
	if err != nil {
		return Job{}, err
	}
	var selected Job
	found := false
	for rows.Next() {
		var j Job
		var raw string
		if err = rows.Scan(&j.Row, &raw, &j.State, &j.Answer, &j.Sent); err != nil {
			break
		}
		if err = json.Unmarshal([]byte(raw), &j.Message); err != nil {
			break
		}
		if !active[customerKey(j.Message)] {
			selected = j
			found = true
			break
		}
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil {
		return Job{}, err
	}
	if rowErr != nil {
		return Job{}, rowErr
	}
	if !found {
		return Job{}, sql.ErrNoRows
	}
	_, err = s.DB.Exec(`UPDATE messages SET state='processing' WHERE id=?`, selected.Row)
	return selected, err
}
func (s *Store) Mark(id int64, state, reason string) error {
	_, err := s.DB.Exec(`UPDATE messages SET state=?,error=? WHERE id=?`, state, reason, id)
	return err
}
func (s *Store) Conversation(key string, idle time.Duration) (string, error) {
	var id string
	var updated int64
	err := s.DB.QueryRow(`SELECT conversation,updated FROM conversations WHERE customer=?`, key).Scan(&id, &updated)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if time.Since(time.Unix(updated, 0)) >= idle {
		return "", nil
	}
	return id, nil
}
func (s *Store) SaveAnswer(job Job, answer Answer) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`UPDATE messages SET state='ready',answer=? WHERE id=?`, answer.Text, job.Row); err != nil {
		return err
	}
	if answer.Conversation != "" {
		if _, err = tx.Exec(`INSERT INTO conversations(customer,conversation,updated) VALUES(?,?,?) ON CONFLICT(customer) DO UPDATE SET conversation=excluded.conversation,updated=excluded.updated`, customerKey(job.Message), answer.Conversation, time.Now().Unix()); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) SentPart(id int64, sent int) error {
	_, err := s.DB.Exec(`UPDATE messages SET state='ready',sent=? WHERE id=?`, sent, id)
	return err
}
func (s *Store) Resolve(id int64, state string) error {
	if state != "sent" && state != "failed" {
		return fmt.Errorf("resolution must be sent or failed")
	}
	result, err := s.DB.Exec(`UPDATE messages SET state=?,error='operator_reconciled' WHERE id=? AND state='uncertain'`, state, id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("no uncertain message with that row ID")
	}
	return nil
}
func (s *Store) Counts() (map[string]int, error) {
	rows, err := s.DB.Query(`SELECT state,count(*) FROM messages GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if err = rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		counts[state] = n
	}
	return counts, rows.Err()
}

// Recover is called only by the single service process, not by status commands.
func (s *Store) Recover() error {
	_, err := s.DB.Exec(`UPDATE messages SET state='pending' WHERE state='processing'; UPDATE messages SET state='uncertain',error='restart_during_send' WHERE state='sending';`)
	return err
}
