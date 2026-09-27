# Grounded Coffee

A production-minded, full-stack coffee ordering experience built from the supplied engineering brief. The customer kiosk, barista console, and public “now serving” display share one responsive interface with live order updates.

## Start the whole stack

Docker Desktop is the only prerequisite.

```bash
cp .env.example .env
docker compose up --build
```

Then open:

- Role-based sign-in: http://localhost:3000/#login
- Customer kiosk: http://localhost:3000/#menu
- Business workspace: http://localhost:3000/#dashboard
- Now serving: http://localhost:3000/#serving
- API health: http://localhost:8000/health

The credential-free demo uses staff PIN `2468`. PostgreSQL, MongoDB, and Redis use named Docker volumes, so orders, menu data, and queued events survive container restarts.

## What is included

- A MongoDB-backed menu with categories, current prices, availability, and modifier groups. React never hardcodes menu data or trusted prices.
- A polished mobile/desktop kiosk with browsing, search, category filters, detailed customisation, persistent cart, quantity controls, and editing before checkout.
- A guest entry flow that asks only for the customer’s pickup name, plus a separate PIN-protected business workspace.
- An enterprise operations dashboard with daily order, sales, completion and refund KPIs; live payment health; searchable order history; fulfilment controls; and safe reasoned cancellation.
- Staff-created counter orders use the same MongoDB menu and server-owned price calculation. Requests are idempotent, recorded as paid at counter and immediately enter the live queue.
- Server-owned pricing: checkout accepts product/modifier identifiers and recalculates every line from MongoDB.
- Idempotent checkout and payment retry. A repeated checkout key returns the same order; retrying payment reuses that order and never creates a second ticket.
- Razorpay Checkout support in test mode. The browser callback is informational; payment becomes paid only through a valid signed `payment.captured` webhook or the authenticated exact-match reconciliation fallback.
- Concurrent-safe webhook processing using unique provider identifiers, a webhook-event primary key, and a database row lock.
- A barista queue that receives only paid tickets, with live Preparing, Ready, Picked up, and reasoned cancellation actions.
- Customer cancellation before preparation. Paid cancellations and late captures enter an idempotent refund worker; barista cancellation is required after preparation starts.
- Redis pub/sub fan-out across multiple API workers, plus an outbox so committed changes are not lost between PostgreSQL and Redis.
- Audience-specific WebSockets. They carry refresh signals only; authorised HTTP endpoints remain the source of truth. The public board exposes only order number and public status.
- Clear validation and conflict responses for malformed input and invalid state transitions.

## Razorpay test mode

Keep `PAYMENT_MODE=demo` for the included no-credentials walkthrough. To use real Razorpay Checkout, put test-mode credentials in `.env`:

```dotenv
PAYMENT_MODE=razorpay
RAZORPAY_KEY_ID=rzp_test_...
RAZORPAY_KEY_SECRET=...
RAZORPAY_WEBHOOK_SECRET=...
VITE_RAZORPAY_KEY_ID=rzp_test_...
PAYMENT_RECONCILIATION_ENABLED=true
```

Configure Razorpay to send webhooks to `https://your-public-host/api/webhooks/razorpay` and enable `payment.captured` and `payment.failed`. The API deliberately refuses live keys. Refund requests include an idempotency key, making retries safe.

Signed webhooks are the primary confirmation path. In Razorpay mode, the reconciliation worker also checks recent pending or cancelled orders through Razorpay's authenticated API. It accepts only a captured INR payment whose provider order ID and amount exactly match the server record, then uses the same locked, idempotent payment transition as the webhook. This permanently recovers missed webhook deliveries; set `PAYMENT_RECONCILIATION_ENABLED=false` only when an external reconciliation service owns that responsibility.

Razorpay cannot deliver webhooks to `localhost`. For local test-mode payments, use a stable HTTPS tunnel for immediate webhook delivery. If that tunnel is unavailable, the authenticated reconciliation worker recovers captured payments automatically. The webhook secret entered in Razorpay must exactly match `RAZORPAY_WEBHOOK_SECRET` in `.env`.

## Architecture

```text
React kiosk / barista / serving screens
           | HTTP + WebSocket
           v
FastAPI (2 workers) <----> Redis pub/sub
     |          |             ^
     |          +-- outbox ---+
     v
PostgreSQL             MongoDB
orders, payments,      menu, modifiers,
refunds, events        availability
```

Each mutation commits the business state and an outbox event in one PostgreSQL transaction. Every worker listens on Redis and fans refresh notifications to its own authorised WebSocket clients. Reconnecting screens immediately refetch, so a transient socket interruption cannot leave stale state behind.

## Tests

The unit suite covers money calculations, cart validation, token checks, and exact-raw-body webhook signature verification. The integration suite exercises both API workers, duplicate concurrent checkout/webhooks, failed-payment retry, amount validation, legal and illegal state transitions, customer/barista cancellation, late capture, refunds, privacy, Redis fan-out, and malformed input.

```bash
docker compose exec api python -m pytest -q
docker compose exec api python tests/integration.py
```

For an isolated test stack that cannot use real payment credentials:

```bash
docker compose -p grounded-qa -f docker-compose.yml -f compose.test.yml up --build -d
docker compose -p grounded-qa -f docker-compose.yml -f compose.test.yml exec api python -m pytest -q
docker compose -p grounded-qa -f docker-compose.yml -f compose.test.yml exec api python tests/integration.py
```

It runs on ports `3011` and `8011`.

## The three engineering questions

### How does a live ticket reach the right screens with multiple workers?

After a state change commits, an outbox publisher sends an order refresh event through Redis. Every FastAPI worker subscribes to the same channel and fans that signal out only to its connected audience: the signed customer order, authenticated staff queue, or privacy-safe public board. Clients refetch their authorised representation after each signal and on reconnect.

### What changes first for a 20-location chain?

Introduce `location_id` across menu availability, prices, orders, staff identity, tickets, indexes, WebSocket channels, and the outbox. Separate global products from store-level price and availability overrides, bind staff credentials and payment accounts to a location, and add migrations, audited actions, reconciliation, backups, observability, and per-location rate limits.

### What prevents duplicate webhooks from double-ticketing or double-refunding?

The API verifies the signature against the untouched request bytes before mutation. It then records the provider event ID under a primary-key constraint, locks the target order row, validates the provider order, currency, amount, and payment state, and applies the transition once. Payment and refund provider IDs are unique. Refund retries reuse a stable idempotency key, so neither an HTTP retry nor a duplicate webhook creates another financial action or ticket.

## Photo credits

The locally bundled product photography is sourced from Unsplash contributors and used as presentation imagery for this demo.
