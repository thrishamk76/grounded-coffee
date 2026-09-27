import hashlib
import hmac
import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from app.main import CartLine, CheckoutBody, SEED_MENU, find_captured_payment, price_cart, transition, verify_signature

MENU = {d['_id']: d for d in SEED_MENU}

def test_total_uses_menu_prices_and_quantity():
    lines = [CartLine(menu_item_id='flat-white', quantity=2, modifiers=[{'group_id':'milk','option_id':'oat'}, {'group_id':'shots','option_id':'one'}]), CartLine(menu_item_id='croissant', quantity=1)]
    _, total = price_cart(lines, MENU)
    assert total == 86000

@pytest.mark.parametrize('line', [
    {'menu_item_id':'missing','quantity':1},
    {'menu_item_id':'flat-white','quantity':1},
    {'menu_item_id':'espresso','quantity':1,'modifiers':[{'group_id':'fake','option_id':'one'}]},
    {'menu_item_id':'espresso','quantity':1,'modifiers':[{'group_id':'shots','option_id':'one'}]*2},
])
def test_bad_modifiers_rejected(line):
    with pytest.raises(HTTPException): price_cart([CartLine(**line)], MENU)

def test_client_cannot_supply_price():
    with pytest.raises(ValidationError):
        CartLine(menu_item_id='espresso', quantity=1, price_paise=1)

@pytest.mark.parametrize('quantity',[0,-1,21])
def test_invalid_quantities(quantity):
    with pytest.raises(ValidationError): CartLine(menu_item_id='espresso', quantity=quantity)

@pytest.mark.parametrize('state,target,actor,reason',[
    ('picked_up','cancelled','barista','mistake'),
    ('cancelled','preparing','barista',None),
    ('awaiting_payment','preparing','barista',None),
    ('paid','ready','barista',None),
    ('preparing','cancelled','customer',None),
    ('ready','cancelled','barista','  '),
])
def test_invalid_transitions(state,target,actor,reason):
    with pytest.raises(HTTPException): transition(state,target,actor,reason)

def test_customer_can_cancel_only_before_preparing():
    transition('awaiting_payment','cancelled','customer')
    transition('paid','cancelled','customer')
    transition('preparing','cancelled','barista','Out of milk')

def test_signature_checks_exact_raw_bytes():
    body=b'{"event": "payment.captured"}'
    sig=hmac.new(b'secret',body,hashlib.sha256).hexdigest()
    assert verify_signature(body,sig,'secret')
    assert not verify_signature(body.replace(b' ',b''),sig,'secret')
    assert not verify_signature(body,sig,'wrong-secret')

def test_reconciliation_accepts_only_exact_captured_payment():
    valid={'id':'pay_test','order_id':'order_test','status':'captured','captured':True,'currency':'INR','amount':14000}
    assert find_captured_payment([valid],'order_test',14000)==valid
    for change in ({'order_id':'other'},{'status':'authorized'},{'captured':False},{'currency':'USD'},{'amount':1}):
        candidate={**valid,**change}
        assert find_captured_payment([candidate],'order_test',14000) is None
