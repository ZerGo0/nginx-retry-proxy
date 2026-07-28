# nginx-retry-proxy

This is a small HTTP proxy for short backend outages.

Nginx sends traffic straight to the real backend during normal use. If Nginx cannot connect to it, Nginx sends the request here instead. This proxy waits for the backend health URL to work, then sends the request once.

This is not a queue. Waiting requests live only in memory and are lost if the proxy restarts.

## Settings

| Variable | Default | What it does |
| --- | --- | --- |
| `BACKEND_URL` | required | Where requests are sent after recovery |
| `READINESS_URL` | required | Health URL that must return 2xx |
| `PROXY_LISTEN_ADDR` | `:8080` | Port used by Nginx |
| `ADMIN_LISTEN_ADDR` | `:9090` | Port used for health and metrics |
| `HOLD_TIMEOUT` | `30s` | Longest time a request may wait |
| `REQUEST_TIMEOUT` | `60s` | Longest total wait and forward time |
| `SHUTDOWN_TIMEOUT` | request timeout + `10s` | Time allowed for requests to finish during shutdown; must not be shorter than `REQUEST_TIMEOUT` |
| `HEALTH_INTERVAL` | `250ms` | How often health is checked while requests wait |
| `HEALTH_TIMEOUT` | `1s` | Timeout for one health check |
| `MAX_HELD_REQUESTS` | `100` | Maximum number of waiting requests |
| `FORWARD_CONCURRENT` | `25` | Maximum number released at once |

The admin port has:

- `/livez` for container health checks
- `/status` for a small JSON status response
- `/metrics` for Prometheus metrics

Keep both ports private. Only Nginx should reach the request port.

## Nginx rules

- Keep `proxy_request_buffering on`. Nginx stores the request body while choosing an upstream.
- Use `proxy_next_upstream error timeout` and set `proxy_next_upstream_tries 2`.
- Never add `non_idempotent`. Without it, Nginx will not resend a POST after that POST may have reached the backend.
- Do not fall back because the app returned 500 or another HTTP status.
- Send WebSockets, streams, and large uploads straight to the backend.
- Set Nginx send and read timeouts a little higher than `REQUEST_TIMEOUT`.

See [examples/nginx.conf](examples/nginx.conf) for a configuration fragment to place inside an existing Nginx `http` block. It is not a standalone configuration: add your SSL certificate, private key, and project-specific routes.

## What it can promise

- If the backend refuses the first connection, the request can wait and be sent once after recovery.
- App responses, including 500 responses, pass through unchanged.
- If forwarding fails, this proxy returns 502 and does not try again.
- If waiting takes too long or the proxy is full, it returns 503.
- It cannot safely resend a request after the backend may have received it.

## Test Nginx before rollout

The Docker test checks the important POST cases against a real Nginx image:

```sh
NGINX_IMAGE=your-production-nginx-image ./integration/nginx/failure-injection.sh
```

It checks that:

- a POST waits when the backend port is closed and arrives once after recovery;
- the body stays unchanged;
- an app 500 is not sent to the wait proxy;
- a POST is not repeated when the backend reads it and then drops the connection.
