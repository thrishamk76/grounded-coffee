from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
import logging
import time
from contextlib import suppress
from datetime import timedelta
from typing import Literal
from fastapi import Header
from redis.asyncio import Redis
from sqlalchemy import text, Boolean
from sqlalchemy.dialects.postgresql import insert
from pydantic import ConfigDict
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, ForeignKey, Identity, Integer, String, Text, UniqueConstraint, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://coffee:coffee@localhost:5432/coffee")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "coffee")
PAYMENT_MODE = os.getenv("PAYMENT_MODE", "demo")
RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "demo_webhook_secret")
PAYMENT_RECONCILIATION_ENABLED = os.getenv("PAYMENT_RECONCILIATION_ENABLED", "true").lower() in ("1", "true", "yes")

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Status(str, Enum):
    awaiting_payment = "awaiting_payment"
    paid = "paid"
    preparing = "preparing"
    ready = "ready"
    picked_up = "picked_up"
    cancelled = "cancelled"


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    number: Mapped[int] = mapped_column(Integer, Identity(start=100), unique=True)
    status: Mapped[str] = mapped_column(String(30), default=Status.awaiting_payment.value)
    total_paise: Mapped[int] = mapped_column(Integer)
    customer_name: Mapped[str] = mapped_column(String(80), default="Guest")
    cancellation_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    items: Mapped[list["OrderItem"]] = relationship(cascade="all, delete-orphan", lazy="selectin")
    payments: Mapped[list["Payment"]] = relationship(cascade="all, delete-orphan", lazy="selectin")


class OrderItem(Base):
    __tablename__ = "order_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    menu_item_id: Mapped[str] = mapped_column(String(40))
    name: Mapped[str] = mapped_column(String(120))
    quantity: Mapped[int] = mapped_column(Integer)
    unit_price_paise: Mapped[int] = mapped_column(Integer)
    modifiers_json: Mapped[str] = mapped_column(Text, default="[]")


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (UniqueConstraint("provider_order_id"), UniqueConstraint("provider_payment_id"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    provider_order_id: Mapped[str] = mapped_column(String(100))
    provider_payment_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="created")
    amount_paise: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class WebhookEvent(Base):
    __tablename__ = "webhook_events"
    event_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class ModifierChoice(BaseModel):
    group_id: str
    option_id: str


class CartLine(BaseModel):
    model_config = ConfigDict(extra="forbid")
    menu_item_id: str
    quantity: int = Field(ge=1, le=20)
    modifiers: list[ModifierChoice] = Field(default_factory=list, max_length=10)


class CheckoutBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    checkout_key: uuid.UUID
    customer_name: str = Field(default="Guest", max_length=80)
    items: list[CartLine] = Field(min_length=1, max_length=30)


class StaffOrderBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_key: uuid.UUID
    customer_name: str = Field(default="Walk-in guest", max_length=80)
    items: list[CartLine] = Field(min_length=1, max_length=30)


class CancelBody(BaseModel):
    actor: str = Field(pattern="^(customer|barista)$")
    reason: Optional[str] = Field(default=None, max_length=300)


class StatusBody(BaseModel):
    status: str = Field(pattern="^(preparing|ready|picked_up|cancelled)$")
    reason: Optional[str] = Field(default=None, max_length=300)


SEED_MENU = [
    {"_id": "espresso", "name": "Espresso", "description": "A bold, syrupy double shot.", "category": "Coffee", "price_paise": 16000, "accent": "#C86B43", "modifier_groups": [{"id": "shots", "name": "Extra shot", "required": False, "options": [{"id": "one", "name": "+1 shot", "price_paise": 6000}]}]},
    {"_id": "flat-white", "name": "Flat White", "description": "Velvety milk over a rich double shot.", "category": "Coffee", "price_paise": 24000, "accent": "#D8A66A", "modifier_groups": [{"id": "milk", "name": "Milk", "required": True, "options": [{"id": "whole", "name": "Whole", "price_paise": 0}, {"id": "oat", "name": "Oat", "price_paise": 4000}, {"id": "almond", "name": "Almond", "price_paise": 4000}]}, {"id": "shots", "name": "Extra shot", "required": False, "options": [{"id": "one", "name": "+1 shot", "price_paise": 6000}]}]},
    {"_id": "iced-latte", "name": "Iced Latte", "description": "Cold milk, espresso and crystal-clear ice.", "category": "Cold", "price_paise": 26000, "accent": "#79A9B9", "modifier_groups": [{"id": "milk", "name": "Milk", "required": True, "options": [{"id": "whole", "name": "Whole", "price_paise": 0}, {"id": "oat", "name": "Oat", "price_paise": 4000}]}, {"id": "sweetness", "name": "Sweetness", "required": True, "options": [{"id": "none", "name": "No sugar", "price_paise": 0}, {"id": "regular", "name": "Regular", "price_paise": 0}]}]},
    {"_id": "matcha", "name": "Iced Matcha", "description": "Ceremonial matcha shaken with milk.", "category": "Cold", "price_paise": 29000, "accent": "#7D9B67", "modifier_groups": [{"id": "milk", "name": "Milk", "required": True, "options": [{"id": "whole", "name": "Whole", "price_paise": 0}, {"id": "oat", "name": "Oat", "price_paise": 4000}]}]},
    {"_id": "croissant", "name": "Butter Croissant", "description": "Flaky, golden and baked this morning.", "category": "Bakery", "price_paise": 18000, "accent": "#D69A49", "modifier_groups": []},
    {"_id": "cookie", "name": "Sea Salt Cookie", "description": "Dark chocolate, brown butter, sea salt.", "category": "Bakery", "price_paise": 14000, "accent": "#8D6048", "modifier_groups": []},
]


class CheckoutKey(Base):
    __tablename__ = "checkout_keys"
    key: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), unique=True)
    fingerprint: Mapped[str] = mapped_column(String(64))

class PaymentAttempt(Base):
    __tablename__ = "payment_attempts"
    provider_payment_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"))
    status: Mapped[str] = mapped_column(String(30))
    amount_paise: Mapped[int] = mapped_column(Integer)

class Refund(Base):
    __tablename__ = "refund_jobs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"))
    payment_id: Mapped[str] = mapped_column(String(100), unique=True)
    amount_paise: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(30), default="pending")
    provider_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    retry_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[str] = mapped_column(String(36))
    published: Mapped[bool] = mapped_column(Boolean, default=False)

SECRET = os.getenv("APP_SECRET", "local-development-change-before-sharing")
STAFF_PIN = os.getenv("STAFF_PIN", "2468")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
log = logging.getLogger("coffee")

def sign(subject):
    return hmac.new(SECRET.encode(), subject.encode(), hashlib.sha256).hexdigest()

def customer_token(order_id):
    return sign("customer:" + order_id)

def is_staff(token):
    try:
        expiry, signature = token.split(".", 1)
        return int(expiry) > time.time() and hmac.compare_digest(signature, sign("staff:" + expiry))
    except (ValueError, AttributeError):
        return False

def bearer(authorization):
    return authorization.removeprefix("Bearer ") if authorization else ""

async def staff(authorization: Optional[str] = Header(None)):
    if not is_staff(bearer(authorization)):
        raise HTTPException(401, "Sign in to the barista counter first.")

def authorize_customer(order_id, authorization):
    if not hmac.compare_digest(bearer(authorization), customer_token(order_id)):
        raise HTTPException(403, "This order belongs to another session.")

async def get_db():
    async with SessionLocal() as session:
        yield session

async def locked_order(db, order_id):
    order = await db.scalar(select(Order).where(Order.id == order_id).with_for_update())
    if not order:
        raise HTTPException(404, "Order not found.")
    return order

def public_menu(doc):
    return {**{k: v for k, v in doc.items() if k != "_id"}, "id": str(doc["_id"])}

def serialize_order(order):
    payment = order.payments[-1] if order.payments else None
    return {"id": order.id, "number": order.number, "status": order.status,
        "total_paise": order.total_paise, "customer_name": order.customer_name,
        "cancellation_reason": order.cancellation_reason, "created_at": order.created_at.isoformat(),
        "payment_status": payment.status if payment else "created",
        "payment_method": "counter" if payment and payment.provider_order_id.startswith("counter_order_") else "razorpay",
        "items": [{"name": i.name, "quantity": i.quantity, "unit_price_paise": i.unit_price_paise,
            "modifiers": json.loads(i.modifiers_json)} for i in order.items]}

async def order_response(db, order):
    data = serialize_order(order)
    refunds = (await db.scalars(select(Refund).where(Refund.order_id == order.id))).all()
    data["refunds"] = [{"status": r.status, "amount_paise": r.amount_paise, "reference": r.provider_id} for r in refunds]
    return data

def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    return hmac.compare_digest(hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(), signature)

def price_cart(cart, docs):
    lines, total = [], 0
    for line in cart:
        item = docs.get(line.menu_item_id)
        if not item:
            raise HTTPException(400, "An item is no longer available. Refresh the menu.")
        groups = {g["id"]: g for g in item.get("modifier_groups", [])}
        chosen, extras, seen = [], 0, set()
        for selection in line.modifiers:
            group = groups.get(selection.group_id)
            option = next((o for o in group["options"] if o["id"] == selection.option_id), None) if group else None
            if not option:
                raise HTTPException(400, "Invalid customisation for " + item["name"])
            if selection.group_id in seen:
                raise HTTPException(400, "Choose only one option for " + group["name"])
            seen.add(selection.group_id)
            extras += option["price_paise"]
            chosen.append(option["name"])
        missing = [g["name"] for g in groups.values() if g.get("required") and g["id"] not in seen]
        if missing:
            raise HTTPException(400, "Choose " + ", ".join(missing) + " for " + item["name"])
        unit = item["price_paise"] + extras
        total += unit * line.quantity
        lines.append((line, item, unit, chosen))
    return lines, total

async def calculate_lines(cart):
    docs = {d["_id"]: d async for d in app.state.mongo.menu.find({"_id": {"$in": [l.menu_item_id for l in cart]}})}
    return price_cart(cart, docs)

async def gateway(method, path, **kwargs):
    if not RAZORPAY_KEY_ID.startswith("rzp_test_") or not RAZORPAY_KEY_SECRET:
        raise HTTPException(503, "Add Razorpay TEST keys on the server to enable test checkout.")
    try:
        async with httpx.AsyncClient(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET), timeout=20) as client:
            response = await client.request(method, "https://api.razorpay.com/v1/" + path, **kwargs)
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError):
        log.exception("Razorpay request failed")
        raise HTTPException(502, "Payment provider is temporarily unavailable. Retry this order shortly.")

def payment_response(order):
    p = order.payments[-1]
    return {"provider_order_id": p.provider_order_id, "amount": order.total_paise,
        "currency": "INR", "key_id": RAZORPAY_KEY_ID, "mode": PAYMENT_MODE}

async def queue_refund(db, order, payment_id, amount):
    await db.execute(insert(Refund).values(id=str(uuid.uuid4()), order_id=order.id,
        payment_id=payment_id, amount_paise=amount, status="pending", attempts=0,
        retry_at=datetime.now(timezone.utc)).on_conflict_do_nothing(index_elements=["payment_id"]))
    order.payments[-1].status = "refund_pending"

def changed(db, order):
    db.add(Outbox(order_id=order.id))

class Hub:
    def __init__(self):
        self.clients = {}
    async def connect(self, ws, audience):
        await ws.accept()
        self.clients[ws] = audience
    async def fanout(self, event):
        for ws, audience in list(self.clients.items()):
            if audience in ("barista", "public", event["order_id"]):
                try:
                    # Public screens never receive names, line items, tokens or payment data.
                    await asyncio.wait_for(ws.send_json({"type": "refresh", "event_id": event["id"]}), 2)
                except Exception:
                    self.clients.pop(ws, None)
    async def subscribe(self):
        while True:
            try:
                async with app.state.redis.pubsub() as sub:
                    await sub.subscribe("coffee:orders")
                    async for msg in sub.listen():
                        if msg["type"] == "message":
                            await self.fanout(json.loads(msg["data"]))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Redis subscriber reconnecting")
                await asyncio.sleep(1)

hub = Hub()

async def dispatch():
    while True:
        try:
            async with SessionLocal() as db:
                rows = (await db.scalars(select(Outbox).where(Outbox.published == False)
                    .order_by(Outbox.id).with_for_update(skip_locked=True).limit(50))).all()
                for event in rows:
                    await app.state.redis.publish("coffee:orders", json.dumps({"id": event.id, "order_id": event.order_id}))
                    event.published = True
                await db.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Event dispatch will retry")
        await asyncio.sleep(0.2)

async def refund_worker():
    while True:
        try:
            async with SessionLocal() as db:
                job = await db.scalar(select(Refund).where(Refund.status.in_(["pending", "submitted"]),
                    Refund.retry_at <= datetime.now(timezone.utc)).order_by(Refund.retry_at)
                    .with_for_update(skip_locked=True).limit(1))
                if job:
                    job.attempts += 1
                    try:
                        if PAYMENT_MODE == "demo":
                            result = {"id": "rfnd_demo_" + job.id, "status": "processed"}
                        elif job.provider_id:
                            result = await gateway("GET", "refunds/" + job.provider_id)
                        else:
                            result = await gateway("POST", "payments/" + job.payment_id + "/refund",
                                headers={"X-Refund-Idempotency": job.id},
                                json={"amount": job.amount_paise, "speed": "normal"})
                        job.provider_id = result["id"]
                        job.status = "processed" if result["status"] == "processed" else ("failed" if result["status"] == "failed" else "submitted")
                        order = await locked_order(db, job.order_id)
                        order.payments[-1].status = "refunded" if job.status == "processed" else "refund_pending"
                        changed(db, order)
                    except HTTPException:
                        log.warning("Refund %s will retry with the same idempotency key", job.id)
                    job.retry_at = datetime.now(timezone.utc) + timedelta(seconds=min(300, 2 ** min(job.attempts, 8)))
                    await db.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Refund worker will retry")
        await asyncio.sleep(1)

async def payment_reconciliation_worker():
    """Recover captured test payments when Razorpay cannot deliver a webhook.

    Webhooks remain the primary path. This fallback reads Razorpay's authenticated
    order-payment API and applies only an exact captured INR payment for the
    provider order and server-calculated total. The order row lock and payment
    attempt primary key make concurrent workers and webhook races idempotent.
    """
    while True:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=7)
            async with SessionLocal() as db:
                candidates = (await db.execute(
                    select(Payment.provider_order_id, Payment.order_id, Order.total_paise)
                    .join(Order, Order.id == Payment.order_id)
                    .where(
                        Payment.status.in_(["created", "failed"]),
                        Order.status.in_([Status.awaiting_payment.value, Status.cancelled.value]),
                        Order.created_at >= cutoff,
                    )
                    .order_by(Order.created_at)
                    .limit(20)
                )).all()

            for provider_order_id, order_id, total_paise in candidates:
                try:
                    # Only one API worker checks a provider order during this window.
                    claimed = await app.state.redis.set(
                        "coffee:payment-reconcile:" + provider_order_id,
                        str(os.getpid()),
                        ex=10,
                        nx=True,
                    )
                    if not claimed:
                        continue
                    result = await gateway("GET", "orders/" + provider_order_id + "/payments")
                    entity = find_captured_payment(result.get("items"), provider_order_id, total_paise)
                    if not entity:
                        continue
                    async with SessionLocal() as db:
                        order = await locked_order(db, order_id)
                        payment = await db.scalar(select(Payment).where(
                            Payment.order_id == order.id,
                            Payment.provider_order_id == provider_order_id,
                        ))
                        if not payment:
                            continue
                        await apply_payment_event(
                            db, order, payment, entity["id"], entity["amount"], "payment.captured"
                        )
                        await db.commit()
                        log.info("Reconciled captured Razorpay payment for order %s", order.number)
                except HTTPException:
                    log.warning("Payment reconciliation will retry provider order %s", provider_order_id)
                except Exception:
                    log.exception("Payment reconciliation failed for provider order %s", provider_order_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Payment reconciliation worker will retry")
        await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app):
    if PAYMENT_MODE not in ("demo", "razorpay"):
        raise RuntimeError("PAYMENT_MODE must be demo or razorpay")
    if PAYMENT_MODE == "razorpay" and (not RAZORPAY_KEY_ID.startswith("rzp_test_") or not RAZORPAY_KEY_SECRET or WEBHOOK_SECRET == "demo_webhook_secret"):
        raise RuntimeError("Razorpay mode requires test keys and a non-default webhook secret. Live keys are forbidden.")
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(813045)"))
        await conn.run_sync(Base.metadata.create_all)
    client = AsyncIOMotorClient(MONGODB_URL)
    app.state.mongo = client[MONGODB_DB]
    for item in SEED_MENU:
        await app.state.mongo.menu.update_one({"_id": item["_id"]}, {"$setOnInsert": item}, upsert=True)
    images = {"espresso": "espresso", "flat-white": "latte", "iced-latte": "iced-latte", "matcha": "matcha", "croissant": "croissant", "cookie": "cookie"}
    for item_id, image in images.items():
        await app.state.mongo.menu.update_one({"_id": item_id, "image": {"$exists": False}}, {"$set": {"image": "/images/" + image + ".jpg"}})
    app.state.redis = Redis.from_url(REDIS_URL, decode_responses=True)
    await app.state.redis.ping()
    tasks = [asyncio.create_task(hub.subscribe()), asyncio.create_task(dispatch()), asyncio.create_task(refund_worker())]
    if PAYMENT_MODE == "razorpay" and PAYMENT_RECONCILIATION_ENABLED:
        tasks.append(asyncio.create_task(payment_reconciliation_worker()))
    yield
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    await app.state.redis.aclose()
    client.close()
    await engine.dispose()

app = FastAPI(title="Grounded Coffee API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:3000", "http://localhost:3011", "http://localhost:5173"],
    allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    async with SessionLocal() as db:
        await db.execute(text("SELECT 1"))
    await app.state.mongo.command("ping")
    await app.state.redis.ping()
    return {"ok": True, "payment_mode": PAYMENT_MODE, "worker": os.getpid()}

@app.get("/api/config")
async def config():
    return {"payment_mode": PAYMENT_MODE, "test_only": True}

@app.get("/api/menu")
async def menu():
    return [public_menu(d) async for d in app.state.mongo.menu.find().sort([("category", 1), ("name", 1)])]

class LoginBody(BaseModel):
    pin: str = Field(min_length=1, max_length=80)

@app.post("/api/staff/session")
async def login(body: LoginBody, request: Request):
    rate_key = "coffee:login:" + request.client.host
    attempts = await app.state.redis.incr(rate_key)
    if attempts == 1:
        await app.state.redis.expire(rate_key, 60)
    if attempts > 10:
        raise HTTPException(429, "Too many attempts. Wait a minute and try again.")
    if not hmac.compare_digest(body.pin, STAFF_PIN):
        raise HTTPException(401, "Incorrect staff PIN.")
    expiry = str(int(time.time()) + 12 * 3600)
    return {"token": expiry + "." + sign("staff:" + expiry)}

@app.post("/api/orders", status_code=201)
async def checkout(body: CheckoutBody, db: AsyncSession = Depends(get_db)):
    key = str(body.checkout_key)
    fingerprint = hashlib.sha256(body.model_dump_json().encode()).hexdigest()
    await db.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": key})
    existing = await db.get(CheckoutKey, key)
    if existing:
        if existing.fingerprint != fingerprint:
            raise HTTPException(409, "This checkout was already submitted with a different cart.")
        order = await locked_order(db, existing.order_id)
    else:
        lines, total = await calculate_lines(body.items)
        order = Order(id=str(uuid.uuid4()), total_paise=total, customer_name=body.customer_name.strip() or "Guest",
            status="awaiting_payment", items=[], payments=[])
        for line, item, unit, mods in lines:
            order.items.append(OrderItem(menu_item_id=item["_id"], name=item["name"], quantity=line.quantity,
                unit_price_paise=unit, modifiers_json=json.dumps(mods)))
        db.add(order)
        await db.flush()
        provider_id = "order_demo_" + uuid.uuid4().hex[:20]
        if PAYMENT_MODE == "razorpay":
            result = await gateway("POST", "orders", json={"amount": total, "currency": "INR", "receipt": order.id})
            provider_id = result["id"]
        order.payments.append(Payment(provider_order_id=provider_id, amount_paise=total, status="created"))
        db.add(CheckoutKey(key=key, order_id=order.id, fingerprint=fingerprint))
        await db.commit()
    return {"order": await order_response(db, order), "token": customer_token(order.id), "payment": payment_response(order)}

@app.get("/api/orders/{order_id}")
async def get_order(order_id: str, authorization: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    authorize_customer(order_id, authorization)
    order = await db.get(Order, order_id)
    if not order:
        raise HTTPException(404, "Order not found.")
    return await order_response(db, order)

@app.post("/api/orders/{order_id}/retry-payment")
async def retry(order_id: str, authorization: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    authorize_customer(order_id, authorization)
    order = await locked_order(db, order_id)
    if order.status != "awaiting_payment":
        raise HTTPException(409, "Payment is no longer needed for this order.")
    # A retry reuses ONE Razorpay order. No second order or extra payable session.
    return payment_response(order)

def transition(current, target, actor, reason=None):
    allowed = {"paid": ["preparing", "cancelled"], "preparing": ["ready", "cancelled"],
        "ready": ["picked_up", "cancelled"], "awaiting_payment": ["cancelled"]}
    if target not in allowed.get(current, []):
        raise HTTPException(409, f"Cannot change {current.replace('_', ' ')} to {target.replace('_', ' ')}.")
    if actor == "customer" and (target != "cancelled" or current not in ("awaiting_payment", "paid")):
        raise HTTPException(409, "Preparation has started. Please ask a barista to cancel.")
    if actor == "barista" and target == "cancelled" and not (reason or "").strip():
        raise HTTPException(400, "Enter a cancellation reason.")

async def change_order(db, order, status, actor, reason=None):
    transition(order.status, status, actor, reason)
    order.status = status
    if status == "cancelled":
        order.cancellation_reason = (reason or "Cancelled by customer").strip()
        captures = (await db.scalars(select(PaymentAttempt).where(
            PaymentAttempt.order_id == order.id, PaymentAttempt.status == "captured"))).all()
        for capture in captures:
            await queue_refund(db, order, capture.provider_payment_id, capture.amount_paise)
        if not captures:
            for p in order.payments:
                if p.status == "captured" and p.provider_payment_id:
                    await queue_refund(db, order, p.provider_payment_id, p.amount_paise)
    changed(db, order)
    await db.commit()
    return await order_response(db, order)

@app.post("/api/orders/{order_id}/cancel")
async def cancel(order_id: str, authorization: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    authorize_customer(order_id, authorization)
    return await change_order(db, await locked_order(db, order_id), "cancelled", "customer")

@app.get("/api/barista/orders", dependencies=[Depends(staff)])
async def barista_orders(db: AsyncSession = Depends(get_db)):
    orders = (await db.scalars(select(Order).where(Order.status.in_(["paid", "preparing", "ready"])).order_by(Order.created_at))).all()
    return [await order_response(db, o) for o in orders]

@app.patch("/api/barista/orders/{order_id}", dependencies=[Depends(staff)])
async def update_status(order_id: str, body: StatusBody, db: AsyncSession = Depends(get_db)):
    return await change_order(db, await locked_order(db, order_id), body.status, "barista", body.reason)

@app.get("/api/business/dashboard", dependencies=[Depends(staff)])
async def business_dashboard(db: AsyncSession = Depends(get_db)):
    local_now = datetime.now(ZoneInfo("Asia/Kolkata"))
    start = local_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    today = (await db.scalars(select(Order).where(Order.created_at >= start)
        .order_by(Order.created_at.desc()).options(selectinload(Order.items), selectinload(Order.payments)))).all()
    recent = (await db.scalars(select(Order).order_by(Order.created_at.desc()).limit(100)
        .options(selectinload(Order.items), selectinload(Order.payments)))).all()
    today_ids = [order.id for order in today]
    processed_refunds = []
    if today_ids:
        processed_refunds = (await db.scalars(select(Refund).where(
            Refund.order_id.in_(today_ids), Refund.status == "processed"))).all()
    paid_states = {"captured", "refund_pending", "refunded"}
    gross = sum(order.total_paise for order in today
        if order.payments and order.payments[-1].status in paid_states)
    refunded = sum(refund.amount_paise for refund in processed_refunds)
    payment_counts = {key: 0 for key in ("created", "failed", "captured", "refund_pending", "refunded")}
    for order in today:
        state = order.payments[-1].status if order.payments else "created"
        payment_counts[state] = payment_counts.get(state, 0) + 1
    active_states = {"paid", "preparing", "ready"}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "orders_today": len(today),
            "active_orders": sum(order.status in active_states for order in today),
            "completed_orders": sum(order.status == "picked_up" for order in today),
            "cancelled_orders": sum(order.status == "cancelled" for order in today),
            "gross_sales_paise": gross,
            "refunds_paise": refunded,
            "net_sales_paise": gross - refunded,
        },
        "payments": payment_counts,
        "orders": [await order_response(db, order) for order in recent],
    }

@app.post("/api/business/orders", status_code=201, dependencies=[Depends(staff)])
async def create_counter_order(body: StaffOrderBody, db: AsyncSession = Depends(get_db)):
    key = str(body.request_key)
    fingerprint = hashlib.sha256(body.model_dump_json().encode()).hexdigest()
    await db.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": key})
    existing = await db.get(CheckoutKey, key)
    if existing:
        if existing.fingerprint != fingerprint:
            raise HTTPException(409, "This counter order was already submitted with different items.")
        order = await locked_order(db, existing.order_id)
    else:
        lines, total = await calculate_lines(body.items)
        order = Order(id=str(uuid.uuid4()), total_paise=total,
            customer_name=body.customer_name.strip() or "Walk-in guest", status="paid", items=[], payments=[])
        for line, item, unit, mods in lines:
            order.items.append(OrderItem(menu_item_id=item["_id"], name=item["name"], quantity=line.quantity,
                unit_price_paise=unit, modifiers_json=json.dumps(mods)))
        db.add(order)
        await db.flush()
        provider_order_id = "counter_order_" + uuid.uuid4().hex[:20]
        provider_payment_id = "counter_pay_" + uuid.uuid4().hex[:20]
        order.payments.append(Payment(provider_order_id=provider_order_id,
            provider_payment_id=provider_payment_id, amount_paise=total, status="captured"))
        db.add(PaymentAttempt(provider_payment_id=provider_payment_id, order_id=order.id,
            amount_paise=total, status="captured"))
        db.add(CheckoutKey(key=key, order_id=order.id, fingerprint=fingerprint))
        changed(db, order)
        await db.commit()
    return await order_response(db, order)

@app.get("/api/serving")
async def serving(db: AsyncSession = Depends(get_db)):
    rows = await db.execute(select(Order.number, Order.status).where(Order.status.in_(["paid", "preparing", "ready"])).order_by(Order.number))
    return [{"number": n, "status": s} for n, s in rows]

async def apply_payment_event(db, order, payment, payment_id, amount, event):
    """Apply a provider-confirmed payment exactly once while the order row is locked."""
    if amount != order.total_paise:
        raise HTTPException(400, "Payment amount does not match the server total.")
    attempt = await db.get(PaymentAttempt, payment_id)
    if attempt and attempt.order_id != order.id:
        raise HTTPException(409, "Payment is already associated with another order.")
    if not attempt:
        attempt = PaymentAttempt(provider_payment_id=payment_id, order_id=order.id,
            amount_paise=amount, status="failed")
        db.add(attempt)
    if event == "payment.captured" and attempt.status != "captured":
        attempt.status = "captured"
        if order.status == "cancelled" or (payment.provider_payment_id and payment.provider_payment_id != payment_id):
            await queue_refund(db, order, payment_id, amount)
        else:
            payment.status = "captured"
            payment.provider_payment_id = payment_id
            if order.status == "awaiting_payment":
                order.status = "paid"
        changed(db, order)
    elif event == "payment.failed" and attempt.status != "captured" and order.status == "awaiting_payment":
        payment.status = "failed"
        changed(db, order)

def find_captured_payment(items, provider_order, amount):
    """Accept only a captured INR payment for this exact provider order and server total."""
    if not isinstance(items, list):
        return None
    return next((entity for entity in items
        if isinstance(entity, dict)
        and entity.get("order_id") == provider_order
        and entity.get("status") == "captured"
        and entity.get("captured") is True
        and entity.get("currency") == "INR"
        and type(entity.get("amount")) is int
        and entity.get("amount") == amount
        and isinstance(entity.get("id"), str)
        and len(entity["id"]) <= 100), None)

async def process_webhook(body, signature, event_id, db):
    if not verify_signature(body, signature, WEBHOOK_SECRET):
        raise HTTPException(400, "Invalid webhook signature.")
    try:
        payload = json.loads(body)
        event = payload["event"]
        if event not in ("payment.captured", "payment.failed"):
            return {"ok": True, "ignored": True}
        entity = payload["payload"]["payment"]["entity"]
        payment_id, provider_order, amount = entity["id"], entity["order_id"], entity["amount"]
        if not isinstance(payment_id, str) or len(payment_id) > 100 or not isinstance(provider_order, str) or len(provider_order) > 100:
            raise ValueError()
        if type(amount) is not int or amount <= 0 or entity["currency"] != "INR":
            raise ValueError()
        if event == "payment.captured" and entity.get("status") != "captured":
            raise ValueError()
    except (ValueError, KeyError, TypeError):
        raise HTTPException(400, "Malformed payment webhook.")
    if len(event_id) > 120:
        raise HTTPException(400, "Invalid event ID.")
    inserted = await db.scalar(insert(WebhookEvent).values(event_id=event_id).on_conflict_do_nothing().returning(WebhookEvent.event_id))
    if not inserted:
        return {"ok": True, "duplicate": True}
    payment = await db.scalar(select(Payment).where(Payment.provider_order_id == provider_order))
    if not payment:
        raise HTTPException(404, "Payment order not found; retry delivery.")
    order = await locked_order(db, payment.order_id)
    await db.refresh(payment)
    await apply_payment_event(db, order, payment, payment_id, amount, event)
    await db.commit()
    return {"ok": True}

@app.post("/api/webhooks/razorpay")
async def webhook(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.body()
    if len(body) > 1000000:
        raise HTTPException(413, "Webhook is too large.")
    return await process_webhook(body, request.headers.get("x-razorpay-signature", ""),
        request.headers.get("x-razorpay-event-id") or hashlib.sha256(body).hexdigest(), db)

class DemoBody(BaseModel):
    outcome: Literal["captured", "failed"] = "captured"

@app.post("/api/orders/{order_id}/demo-payment")
async def demo_payment(order_id: str, body: DemoBody, authorization: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    if PAYMENT_MODE != "demo":
        raise HTTPException(404, "Demo payments are disabled.")
    authorize_customer(order_id, authorization)
    order = await db.get(Order, order_id)
    if not order:
        raise HTTPException(404, "Order not found.")
    if order.status != "awaiting_payment":
        raise HTTPException(409, "This order no longer needs payment.")
    entity = {"id": "pay_demo_" + uuid.uuid4().hex[:20], "order_id": order.payments[-1].provider_order_id,
        "amount": order.total_paise, "currency": "INR", "status": body.outcome}
    raw = json.dumps({"event": "payment." + body.outcome, "payload": {"payment": {"entity": entity}}}).encode()
    await db.rollback()
    signature = hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    # The simulator goes through the same signed transactional webhook handler.
    return await process_webhook(raw, signature, "evt_demo_" + uuid.uuid4().hex, db)

@app.websocket("/ws")
async def websocket(ws: WebSocket):
    audience = ws.query_params.get("audience", "public")
    token = ws.query_params.get("token", "")
    valid = is_staff(token) if audience == "barista" else audience == "public" or hmac.compare_digest(token, customer_token(audience))
    if not valid:
        await ws.close(code=1008)
        return
    await hub.connect(ws, audience)
    try:
        await ws.send_json({"type": "refresh"})
        while True:
            await ws.receive_text()
            if audience == "barista" and not is_staff(token):
                await ws.close(code=1008)
                break
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.pop(ws, None)
