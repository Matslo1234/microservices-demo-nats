# frontend

The frontend is the public HTTP edge. It serves the browser application and
translates reads and writes into the repository's NATS contracts; it does not
read domain-owner storage directly.

## Checkout preparation

`POST /cart/checkout` performs two independent preparations concurrently:

1. Request `boutique.qry.storefront.checkout.v1` with the session, currency,
   correlation ID, and the `min_cart_version` observed on the rendered cart.
2. Request `boutique.qry.payment.tokenize.v1` using the order ID as the
   tokenization idempotency key.

The order command is published only after both preparations succeed. A failed
or empty cart cancels the tokenization context; a failed tokenization prevents
the order publish. The durable checkout saga remains asynchronous after the
frontend receives the order publish acknowledgement.

HTML cart pages include `cart_version` as a hidden checkout field. Clients that
omit it send a zero minimum and receive the newest projection available to the
responder. In all cases, `checkoutservice` independently validates the expected
cart, catalog, and rate revisions before accepting an order.

Storefront query timeouts and explicit `OVERLOADED` responses map to bounded
HTTP errors. The frontend does not retry overload responses inside the request,
which prevents retry amplification.
