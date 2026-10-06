"""Bounded local transport for governed GAX/IMX messages.

Transport establishes durable routing and delivery evidence only. It does not
authenticate an institution, grant authority, authorize an effect, or verify a
destination effect independently.
"""

from .transport import (
    TRANSPORT_PROFILE,
    TRANSPORT_VERSION,
    AcceptedGaxRecipientAdapter,
    LocalDurableTransport,
    RecipientOutcome,
    Route,
    TrustedRouteTable,
    canonical_bytes,
    commitment,
    make_transport_envelope,
)

__all__ = [
    "TRANSPORT_PROFILE",
    "TRANSPORT_VERSION",
    "AcceptedGaxRecipientAdapter",
    "LocalDurableTransport",
    "RecipientOutcome",
    "Route",
    "TrustedRouteTable",
    "canonical_bytes",
    "commitment",
    "make_transport_envelope",
]
