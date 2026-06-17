"""Message-type naming + flexible-matching variant behavior.

These tests pin the deterministic (and occasionally surprising) output of
``SQSEvent.get_message_type`` / ``get_message_type_variants`` for edge class
names, because that snake_case regex is what produces the route key. They also
cover the flexible-matching dispatch surface of ``SQSRouter`` / ``FastSQS``:
matching a payload ``type`` value against any of the ClassName/snake/camel/kebab
variant forms, the fact that variants are ignored when ``flexible_matching`` is
off, and the duplicate-registration guard that fires when two classes' variant
sets collide.

Only 1-2 word class names are exercised. Direct classmethod calls plus
SQSTestClient for the matching behavior; no AWS.
"""

import pytest

from fastsqs import FastSQS, SQSEvent, SQSRouter
from fastsqs.testing import SQSTestClient


class Order(SQSEvent):
    order_id: str = "x"


class OrderCreated(SQSEvent):
    order_id: str = "x"


class OrderCreatedV2(SQSEvent):
    order_id: str = "x"


class HTTPRequest(SQSEvent):
    order_id: str = "x"


class ABCEvent(SQSEvent):
    order_id: str = "x"


# --------------------------------------------------------------------------
# get_message_type: the snake_case regex, including its surprising edge cases
# --------------------------------------------------------------------------

def test_get_message_type_single_word():
    # Single word: just lowercased, no underscores inserted.
    assert Order.get_message_type() == "order"


def test_get_message_type_multiword_camel():
    # CamelCase word boundary -> single underscore.
    assert OrderCreated.get_message_type() == "order_created"


def test_get_message_type_with_trailing_digits():
    # The regex inserts an underscore before each capital (not before digits),
    # so the trailing "V2" becomes "_v2": "OrderCreatedV2" -> "order_created_v2".
    assert OrderCreatedV2.get_message_type() == "order_created_v2"


def test_get_message_type_with_acronym():
    # re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower() treats EVERY capital after the
    # first char as a word boundary, so a leading acronym is split letter by letter.
    # "HTTPRequest" -> "h_t_t_p_request". Surprising, but deterministic; pin it.
    assert HTTPRequest.get_message_type() == "h_t_t_p_request"


def test_get_message_type_acronym_at_start_split_per_letter():
    # Same rule for a 3-letter leading acronym: "ABCEvent" -> "a_b_c_event".
    assert ABCEvent.get_message_type() == "a_b_c_event"


# --------------------------------------------------------------------------
# get_message_type_variants: the four flexible-matching forms
# --------------------------------------------------------------------------

def test_message_type_variants_contains_all_four_forms():
    # ClassName, snake, camel, kebab.
    variants = OrderCreated.get_message_type_variants()
    assert variants == {
        "OrderCreated",
        "order_created",
        "orderCreated",
        "order-created",
    }


def test_message_type_variants_single_word_collapses_to_two_forms():
    # For a single word ClassName the snake/camel/kebab forms all equal the
    # lowercased name, so the set collapses to {ClassName, lowercased}.
    assert Order.get_message_type_variants() == {"Order", "order"}


# --------------------------------------------------------------------------
# flexible_matching=True: a payload ``type`` value matches any variant form
# --------------------------------------------------------------------------

def test_flexible_matching_matches_classname_form():
    app = FastSQS(flexible_matching=True)
    ran = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        ran.append(msg.order_id)

    r = SQSTestClient(app).send({"type": "OrderCreated", "order_id": "1"})

    assert r == {"batchItemFailures": []}
    assert ran == ["1"]


def test_flexible_matching_matches_kebab_type_value():
    # The kebab form matches as a TYPE value via the variant set. (This is
    # distinct from kebab-case payload KEYS, which stay unsupported -- only the
    # discriminator VALUE is matched flexibly here.)
    app = FastSQS(flexible_matching=True)
    ran = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        ran.append(msg.order_id)

    r = SQSTestClient(app).send({"type": "order-created", "order_id": "1"})

    assert r == {"batchItemFailures": []}
    assert ran == ["1"]


@pytest.mark.parametrize("type_value", ["order_created", "orderCreated"])
def test_flexible_matching_matches_snake_and_camel_forms(type_value):
    app = FastSQS(flexible_matching=True)
    ran = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        ran.append(type_value)

    r = SQSTestClient(app).send({"type": type_value, "order_id": "1"})

    assert r == {"batchItemFailures": []}
    assert ran == [type_value]


def test_flexible_matching_all_four_forms_route_to_handler():
    # All four variant forms route to the single registered handler.
    app = FastSQS(flexible_matching=True)
    ran = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        ran.append(msg.order_id)

    client = SQSTestClient(app)
    for tv in ("OrderCreated", "order_created", "orderCreated", "order-created"):
        assert client.send({"type": tv, "order_id": "1"}) == {"batchItemFailures": []}

    assert ran == ["1", "1", "1", "1"]


# --------------------------------------------------------------------------
# flexible_matching=False (default): only the exact snake_case primary matches
# --------------------------------------------------------------------------

def test_variants_ignored_when_flexible_matching_disabled():
    # With flexible matching off, the ClassName variant "OrderCreated" does NOT
    # match -- only the exact primary "order_created" routes. The unmatched
    # record fails (no default handler).
    app = FastSQS()
    ran = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        ran.append(msg.order_id)

    r = SQSTestClient(app).send({"type": "OrderCreated", "order_id": "1"}, message_id="x")

    assert r == {"batchItemFailures": [{"itemIdentifier": "x"}]}
    assert ran == []


def test_exact_primary_still_matches_when_flexible_disabled():
    # Sanity: the exact snake_case primary routes fine with flexible off.
    app = FastSQS()
    ran = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        ran.append(msg.order_id)

    r = SQSTestClient(app).send({"type": "order_created", "order_id": "1"})

    assert r == {"batchItemFailures": []}
    assert ran == ["1"]


# --------------------------------------------------------------------------
# variant collision across two classes on one flexible router
# --------------------------------------------------------------------------

def test_flexible_matching_variant_collision_raises_valueerror():
    # Two classes whose variant SETS collide. ``XFoo`` and ``xFoo`` differ only
    # in the first-character case, and the snake_case regex ignores the first
    # char (``(?<!^)`` lookbehind), so BOTH yield primary "x_foo" and their
    # variant sets overlap on {"x_foo", "xFoo", "x-foo"}.
    #
    # Real-behavior note: because any overlap in variant sets implies an
    # identical primary type, the SECOND registration trips the duplicate-primary
    # guard (router.py "Handler for message type ... already exists") BEFORE the
    # dedicated variant-collision branch. The latter (the only message that names
    # ``flexible_matching``) is unreachable for two distinct event classes via the
    # public route() API. We therefore assert the TRUE reachable ValueError, whose
    # message names the colliding form "x_foo".
    class XFoo(SQSEvent):
        order_id: str = "x"

    class xFoo(SQSEvent):  # noqa: N801 - intentional collision-by-case
        order_id: str = "x"

    # Both produce the same primary; their variant sets overlap.
    assert XFoo.get_message_type() == xFoo.get_message_type() == "x_foo"
    assert XFoo.get_message_type_variants() & xFoo.get_message_type_variants()

    router = SQSRouter(flexible_matching=True)

    @router.route(XFoo)
    async def first(msg: XFoo):
        pass

    with pytest.raises(ValueError) as excinfo:
        @router.route(xFoo)
        async def second(msg: xFoo):
            pass

    assert "x_foo" in str(excinfo.value)
