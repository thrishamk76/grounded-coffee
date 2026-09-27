import hashlib
import hmac
from app.main import verify_signature


def test_signature_accepts_exact_body():
    body, secret = b'{"event":"payment.captured"}', "secret"
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(body, signature, secret)


def test_signature_rejects_tampering():
    assert not verify_signature(b"changed", "bad", "secret")
