# WeCom Dify Go

[![CI](https://github.com/Neo1Noir/wecom-dify-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Neo1Noir/wecom-dify-go/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8.svg)](go.mod)

A Go middleware that connects **WeChat Customer Service (WeCom / WeChat KF)** to a conversational Dify app. One executable, a persistent SQLite queue, and a credential-free local demo.

```text
Encrypted WeCom callback
  → verify and persist notification → acknowledge
  → sync messages + advance cursor atomically
  → durable inbox → per-customer ordering / bounded concurrency
  → Dify conversation → persisted answer → checked delivery
```

## Technical focus

- **Go throughout:** standard-library HTTP, XML, cryptography, and lifecycle handling; pure-Go SQLite through `modernc.org/sqlite`. No Node.js or Python runtime.
- **Durable acknowledgement:** callback notifications are persisted before a successful response. Inbox insertion and cursor advancement share a transaction.
- **Ordering and concurrency:** one active job per customer/account pair; different customers run concurrently with a bounded worker count.
- **Restart recovery:** queued work and generated answers survive restarts. An interrupted send becomes `uncertain` and is not blindly replayed.
- **Conversation isolation:** Dify receives an HMAC-derived user identifier; conversations are stored separately and expire after one idle hour.
- **Human-session awareness:** state is checked before generation and every send; active human, waiting, or ended sessions are not interrupted.
- **Controlled text output:** reasoning tags are removed, UTF-8 is preserved, and long answers are bounded to three text messages.

The public release focuses on the reusable text-support path. It does not include company-specific SN lookup, document delivery, internal operations bots, or automatic human-agent transfer.

## Try the local demo

Requires Go 1.27+. No WeCom/Dify account or network calls to those services are needed:

```sh
git clone https://github.com/Neo1Noir/wecom-dify-go.git
cd wecom-dify-go
go run ./cmd/wecom-dify -demo
```

In another terminal:

```sh
curl -X POST http://127.0.0.1:8080/demo/send -H 'Content-Type: application/json' -d '{"customer":"alice","text":"How do I upgrade?"}'
curl http://127.0.0.1:8080/demo/messages
```

PowerShell alternative:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/demo/send -Method Post -ContentType application/json -Body '{"customer":"alice","text":"How do I upgrade?"}'
Invoke-RestMethod http://127.0.0.1:8080/demo/messages
```

The demo binds to **127.0.0.1** and uses a temporary SQLite file. Its answer explicitly says no model was called. `/demo/*` routes do not exist in production mode. Add `-state ./data/demo.db` to retain demo state deliberately.

## Run with your accounts

1. Create/configure a WeChat Customer Service integration in WeCom. Obtain its permitted secret, callback token, EncodingAESKey, and corporate ID.
2. Publish a Dify conversational app supporting `POST /v1/chat-messages` and obtain its app API key. A Workflow-only app or Knowledge API key cannot be substituted.
3. Copy `.env.example` to `.env` and fill in your own values. Keep `.env` private.
4. Start with Docker Compose:

```sh
docker compose up --build -d
curl http://127.0.0.1:8080/ready
```

5. Put a trusted HTTPS reverse proxy in front of port 8080 and register `https://your-domain.example/callback` as the callback URL. GET verification and encrypted POST notifications use the same path. Configure the required platform-side IP/domain permissions for your environment.

The binary reads environment variables directly; it does not parse `.env` itself. Compose supplies the file through `env_file`. For a native deployment, export variables through your service manager and build:

```sh
go build -trimpath -o bin/wecom-dify ./cmd/wecom-dify
./bin/wecom-dify
```

## Configuration

| Variable | Purpose |
|---|---|
| `WECOM_CORP_ID` | Callback recipient and token identity |
| `WECOM_KF_SECRET` | Secret permitted to use WeChat Customer Service APIs |
| `WECOM_CALLBACK_TOKEN` | Callback signature token |
| `WECOM_ENCODING_AES_KEY` | 43-character unpadded base64 callback key |
| `DIFY_API_BASE_URL` | Dify app endpoint ending in `/v1` |
| `DIFY_APP_API_KEY` | App key for the conversational endpoint |
| `DIFY_USER_HASH_KEY` | Stable random secret, at least 16 characters; keep across restarts |
| `SQLITE_PATH` | Defaults to `data/state.db`; Compose uses `/data/state.db` |
| `LISTEN_ADDR` | Defaults to `:8080` |
| `MAX_CONCURRENCY` | 1-32, default 4 |
| `HUMAN_KEYWORDS` | Comma-separated exact commands; default `human,agent,operator` |
| `HANDOFF_REPLY` | Configurable support-contact text; does not transfer a session |
| `UNSUPPORTED_REPLY` | Reply for images/files/other unsupported incoming types |
| `ERROR_REPLY` | User-facing reply when Dify is unavailable |

Reply text and human-command keywords can be localized through configuration. Secrets, customer identifiers, and message bodies are not emitted in normal application logs. SQLite contains conversation content and pending callback tokens; protect the state volume and its backups.

## Delivery semantics and recovery

This service does not claim distributed exactly-once delivery. Platform calls can succeed while the network response is lost.

- Duplicate inbound IDs are deduplicated per customer-service account.
- Generated answers and completed chunk counts are persisted.
- A network failure, invalid response, or restart during sending marks the message `uncertain`; later messages for that customer remain blocked until an operator reconciles the outcome.
- An explicit platform rejection marks the message failed. It is not automatically resent.
- Callback pulls retry with bounded backoff; persistently failed notifications remain recorded. A later valid notification can resume from the saved cursor.

Inspect state counts:

```sh
./bin/wecom-dify -status -state data/state.db
```

After checking actual delivery and locating the uncertain message's row ID in the local `messages` table, record its outcome:

```sh
./bin/wecom-dify -resolve 42 -as sent -state data/state.db
# Or -as failed when you have confirmed that it was not delivered.
```

This updates only an uncertain row and never resends a message. Keep a backup before operational maintenance. Run **one service process per state database**; multi-instance coordination is outside this release.

## Tests

```sh
go test ./...
go vet ./...
# On a platform with the Go race toolchain:
go test -race ./...
```

Tests use real callback encryption, HTTP mock servers, and temporary SQLite files. They cover the callback→sync→Dify→reply path, deduplication, ordering, concurrent customers, persisted answers, ambiguous sends, human-session checks, and Unicode reply splitting. They do not send real WeChat messages or call a paid model.

## Scope and deployment notes

This is a new Go implementation of selected middleware behavior, not a claim of feature parity with a larger private integration. The public release has local simulation and CI coverage; real WeCom/Dify tenant acceptance is still required before production. Platform quotas and reply windows continue to apply. Images, files, streaming progress messages, proactive messages, and automatic agent handoff are outside the initial scope.

The core entrypoints are `server.go` (callback and processing), `store.go` (durability), `clients.go` (upstream contracts), and `crypto.go` (callback verification). Dify API reference: [Send Chat Message](https://docs.dify.ai/en/api-reference/chat-messages/send-chat-message). WeCom reference: [Receive messages/events](https://developer.work.weixin.qq.com/document/path/94670).

## License

[MIT](LICENSE). Runtime dependencies retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
