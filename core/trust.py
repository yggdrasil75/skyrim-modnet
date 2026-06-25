"""Trust model — two orthogonal axes.

AXIS 1: node_state (a node's standing in the wider network)
    gateway   — self-declared, stores 0 files; exists only to bootstrap others
                and answer "who's on the network?". Zero files ⇒ zero reason to
                seize it. Excluded from content votes (it holds nothing).
    newcomer  — recently joined, still introducing itself. Low weight.
    common    — a normal user-hosted node past the newcomer window.
    trusted   — long-lived (>= TRUST_TENURE_S) with few failures. Slightly
                higher weight on content/moderation votes.
    malicious — repeatedly originated files that trusted nodes blocked. Weight
                floored to ~0; we stop replicating from/to it.

    Rather than a hard trusted/untrusted split we expose `tenure_years` and let
    weight scale continuously with it (capped), so trust grows gradually.

AXIS 2: relation (THIS node's private opinion of a peer)
    friend    — prioritize downloads from here (fast/reliable).
    neutral   — default.
    distant   — far away (high RTT); prioritize as a replication target for
                fault isolation.
    neighbor  — close (low RTT); prioritize for dedup / load-balancing, NOT for
                making far-apart replicas.
    enemy     — never replicate to/from; never replicate files this node
                originated. A local quarantine that doesn't require global
                consensus.

The two axes are combined only when computing a *vote weight*: global state
sets the base weight, and we additionally refuse to count enemies at all.
"""
from __future__ import annotations

import time

# --- node_state constants ---
STATE_GATEWAY = "gateway"
STATE_NEWCOMER = "newcomer"
STATE_COMMON = "common"
STATE_TRUSTED = "trusted"
STATE_MALICIOUS = "malicious"

# --- relation constants ---
REL_FRIEND = "friend"
REL_NEUTRAL = "neutral"
REL_DISTANT = "distant"
REL_NEIGHBOR = "neighbor"
REL_ENEMY = "enemy"

# Tunables (could move to config; kept here so the model reads in one place).
NEWCOMER_WINDOW_S = 30 * 24 * 3600        # 30 days before leaving 'newcomer'
TRUST_TENURE_S = 365 * 24 * 3600          # 1 year => eligible for 'trusted'
MAX_FAILURES_FOR_TRUST = 50               # too many flaky periods => not trusted
MALICIOUS_BAD_FILE_THRESHOLD = 3          # blocked-file count that marks malicious
NEIGHBOR_MS = 25.0                        # <= this RTT => neighbor candidate
DISTANT_MS = 150.0                        # >= this RTT => distant candidate


def tenure_seconds(first_seen: int | None, now: int | None = None) -> int:
    if not first_seen:
        return 0
    now = now or int(time.time())
    return max(0, now - first_seen)


def tenure_years(first_seen: int | None, now: int | None = None) -> float:
    return tenure_seconds(first_seen, now) / (365 * 24 * 3600)


def classify_state(peer: dict, now: int | None = None) -> str:
    """Derive a node's global state from observed facts.

    NOTE: we deliberately do NOT auto-assign 'malicious' from bad-vote counts.
    Per the network's anti-influence design, no accumulation of bad votes ever
    bans or condemns a node automatically. 'malicious' is only reached when a
    user has explicitly marked the node 'enemy' (a local decision). Everything
    here is otherwise tenure/role based.

    `peer` is a dict-like row with: declared_role, first_seen, failures,
    relation. We never let a node *declare* itself trusted; only 'gateway' is
    honored as a self-declaration (a capability, not a privilege).
    """
    now = now or int(time.time())
    if peer.get("relation") == REL_ENEMY:
        return STATE_MALICIOUS
    if peer.get("declared_role") == "gateway":
        return STATE_GATEWAY
    age = tenure_seconds(peer.get("first_seen"), now)
    if age < NEWCOMER_WINDOW_S:
        return STATE_NEWCOMER
    if age >= TRUST_TENURE_S and peer.get("failures", 0) <= MAX_FAILURES_FOR_TRUST:
        return STATE_TRUSTED
    return STATE_COMMON


def vote_weight(peer: dict, now: int | None = None) -> float:
    """Weight of a node's vote/attestation.

    Base by state, scaled by tenure for non-trusted/common so trust accrues
    smoothly. Enemies and malicious nodes contribute ~0. Gateways hold no
    files so they don't vote on content (weight 0 here; they still gossip).
    """
    state = peer.get("node_state") or classify_state(peer, now)
    relation = peer.get("relation", REL_NEUTRAL)

    if relation == REL_ENEMY:
        return 0.0
    if state == STATE_MALICIOUS:
        return 0.0
    if state == STATE_GATEWAY:
        return 0.0

    yrs = tenure_years(peer.get("first_seen"), now)
    if state == STATE_NEWCOMER:
        base = 0.25
    elif state == STATE_TRUSTED:
        base = 1.5
    else:  # common
        base = 1.0
    # smooth tenure bonus, capped, so a 5-year node isn't unboundedly powerful
    tenure_bonus = min(yrs * 0.1, 0.5)

    w = base + tenure_bonus
    if relation == REL_FRIEND:
        w *= 1.1   # we trust friends slightly more in our own tally
    return round(w, 4)


def suggest_relation(distance_ms: float | None, current: str) -> str:
    """Auto-suggest neighbor/distant from measured RTT, but never override a
    human-set friend/enemy (those are deliberate)."""
    if current in (REL_FRIEND, REL_ENEMY):
        return current
    if distance_ms is None:
        return REL_NEUTRAL
    if distance_ms <= NEIGHBOR_MS:
        return REL_NEIGHBOR
    if distance_ms >= DISTANT_MS:
        return REL_DISTANT
    return REL_NEUTRAL
