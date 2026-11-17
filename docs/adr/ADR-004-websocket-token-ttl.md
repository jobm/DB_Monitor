# ADR-004: Short-Lived WebSocket Session Tokens

**Status:** Implemented  
**Date:** 2026-06-23  
**Deciders:** Project leads  
**Context:** [WebSocket endpoint](../api-docs.md#realtime-endpoint), [WS token config](../configuration.md#auth-and-session-settings)

---

## Context

DB Monitor exposes a WebSocket endpoint (`/ws/events`) that broadcasts
CDC events to connected clients in real-time. Clients authenticate via either:

- A long-lived API key in the `X-API-Key` header.
- A short-lived bearer token in the `Authorization: Bearer` header.
- A **short-lived WebSocket session token** passed as a query parameter
  (`?session_token=...`).

The session-token path exists for clients that cannot set HTTP headers during
the WebSocket upgrade (e.g., browser `WebSocket()` constructor, some
programmatic clients, TUI dashboards).

The question was: what TTL should these session tokens have?

## Decision

Set the default WebSocket session token TTL to **60 seconds**
(`WS_SESSION_TOKEN_TTL_SECONDS=60`), and keep the access token TTL at
**15 minutes** (`ACCESS_TOKEN_TTL_MINUTES=15`).

### Rationale

1. **Session tokens are single-use credentials issued in exchange for a valid
   API key or bearer token.** A short TTL limits the window of exposure if
   the session token is leaked (e.g., in browser logs, referrer headers, or
   captured during transit).

2. **Session tokens appear in the URL query string.** Query strings are more
   likely to be logged by proxies, load balancers, and browser history than
   HTTP headers. A 60-second window limits the practical attack surface.

3. **The WebSocket connection, once established, persists indefinitely.**
   The session token is only used during the upgrade handshake. After the
   WebSocket is connected, no further token checks occur. A short TTL
   therefore does not force clients to reconnect; it only limits re-use of
   the credential itself.

4. **Typical clients reconnect infrequently.** Browser tabs and TUI sessions
   maintain a persistent WebSocket. If a client does disconnect, they can
   request a fresh session token from the `/auth/ws-token` endpoint using
   their API key.

### Rejected alternatives

- **No session token — require headers on upgrade:** Incompatible with
  browser `WebSocket()` and some programmatic clients.
- **Long-lived session tokens (e.g., 24h):** Would expose leaked tokens for
  too long. Attackers could replay the token to open rogue WS connections.
- **Re-authenticate periodically over the WS connection:** Adds protocol
  complexity and breaks the simple send/receive pattern.

## Consequences

**Positive:**

- Browser and TUI clients can connect without header-setting capability.
- Leaked tokens have a ~60-second exposure window.
- The access token (15 min TTL) remains the recommended credential for
  REST API calls.

**Negative:**

- Clients that reconnect frequently (e.g., mobile apps with spotty
  connectivity) must re-fetch session tokens from `/auth/ws-token`.
- The `WS_SESSION_TOKEN_TTL_SECONDS` setting is configurable; an operator
  who sets it too high increases the leak window.

## Compliance

- [x] Default `WS_SESSION_TOKEN_TTL_SECONDS = 60`
- [x] Configurable via environment variable
- [x] Session-token auth path in `/ws/events`
- [x] Token issuance endpoint `POST /auth/ws-token`
- [x] Documented in `docs/configuration.md`
