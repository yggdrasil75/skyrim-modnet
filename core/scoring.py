"""Social scoring — tags, ratings, content flags.

This layer turns raw community votes into trustworthy aggregates. The guiding
analogy (yours): AO3 lets authors spam meaningless tags; a booru has
meaningful tags but no voting so junk sticks; e-hentai votes tags into
confirmed / low-weight / rejected states. We follow the e-h model and lean on
the existing node/user trust system so trusted participants carry more weight.

Pieces:
  1. TAG STATES — proposed -> low_weight -> confirmed (or rejected), from the
     for/against weight balance.
  2. TAG CLASS — focus | contains | minor, itself decided by vote (your
     gore-as-a-setting vs gore-as-the-point distinction).
  3. RATING AGGREGATION with three corrections:
       a. voter calibration: a user who always votes 5* (or always 1*) carries
          less weight because the vote is less informative; a user breaking
          their own pattern carries MORE.
       b. review-bomb damping: if a mod gets a sudden burst of votes far above
          its normal rate, votes in that window are down-weighted on a smooth
          log curve.
       c. time decay: older votes fade so updates can outweigh stale opinion.
  4. CONTENT FLAGS — author sets a 0-5 baseline for sexual/violent intensity;
     community votes nudge the displayed value.

All weights ultimately reference user/node trust via a passed-in callable so
this module stays decoupled from the trust tables.
"""
from __future__ import annotations

import math
import statistics
import time

# ---- tunables --------------------------------------------------------------
TAG_CONFIRM_WEIGHT = 3.0       # for_weight needed to confirm a tag
TAG_LOWWEIGHT_MIN = 0.5        # above this (but below confirm) => low_weight
TAG_REJECT_RATIO = 2.0         # against >= ratio*for AND past min => rejected

RATING_DECAY_HALFLIFE_DAYS = 180.0   # a vote's weight halves after ~6 months
BURST_WINDOW_HOURS = 6
BURST_THRESHOLD = 0.7          # fraction-of-monthly within window before damping
BURST_FLOOR = 0.01             # most a burst vote can be damped to


# ---------------------------------------------------------------------------
# 1 + 2. Tags
# ---------------------------------------------------------------------------

def classify_tag(for_weight: float, against_weight: float) -> str:
    """Map for/against weight onto e-h-style states."""
    if for_weight >= TAG_CONFIRM_WEIGHT and for_weight > against_weight:
        return "confirmed"
    if (against_weight >= TAG_REJECT_RATIO * max(for_weight, 1e-9)
            and against_weight >= TAG_LOWWEIGHT_MIN):
        return "rejected"
    if for_weight >= TAG_LOWWEIGHT_MIN:
        return "low_weight"
    return "proposed"


def aggregate_tag(votes: list[dict], weight_of) -> dict:
    """Aggregate per-voter tag votes into for/against weight, state, and the
    winning tag_class.

    `votes`: list of {voter_pubkey, vote(+1/-1), class_vote}
    `weight_of`: callable(voter_pubkey) -> float trust weight.
    """
    for_w = 0.0
    against_w = 0.0
    class_w: dict[str, float] = {"focus": 0.0, "contains": 0.0, "minor": 0.0}
    for v in votes:
        w = max(0.0, weight_of(v["voter_pubkey"]))
        if w <= 0:
            continue
        if v["vote"] > 0:
            for_w += w
            cv = v.get("class_vote") or "contains"
            if cv in class_w:
                class_w[cv] += w
        else:
            against_w += w
    state = classify_tag(for_w, against_w)
    winning_class = max(class_w.items(), key=lambda kv: kv[1])[0] if for_w else "contains"
    return {
        "for_weight": round(for_w, 4),
        "against_weight": round(against_w, 4),
        "state": state,
        "tag_class": winning_class,
        "class_breakdown": {k: round(v, 4) for k, v in class_w.items()},
    }


# ---------------------------------------------------------------------------
# 3a. Voter calibration
# ---------------------------------------------------------------------------

def voter_calibration(this_score: float, voter_history: list[float],
                      scale_min: float = 1.0, scale_max: float = 5.0) -> float:
    """Weight multiplier for one rating, based on how informative it is given
    the voter's own history.

    - A voter with near-zero variance (always the same score) is uninformative:
      multiplier shrinks toward a floor.
    - A vote that DEVIATES from the voter's personal mean is more informative:
      multiplier grows. This rewards a habitual 5*-er who finally gives a 2*.
    Returns a multiplier roughly in [0.2, 2.0].
    """
    if len(voter_history) < 3:
        return 1.0  # not enough history to judge; treat as neutral
    mean = statistics.fmean(voter_history)
    span = scale_max - scale_min
    # spread of their history, normalized 0..1 (0 = always identical)
    stdev = statistics.pstdev(voter_history)
    spread = min(1.0, stdev / (span / 2))
    # base weight from how varied (informative) the voter is overall
    base = 0.2 + 0.8 * spread            # 0.2 (monotone) .. 1.0 (varied)
    # deviation bonus: how far THIS score is from their personal mean
    deviation = abs(this_score - mean) / span     # 0..1
    bonus = 1.0 + deviation               # 1.0 .. 2.0
    return round(min(2.0, base * bonus), 4)


# ---------------------------------------------------------------------------
# 3b. Review-bomb damping
# ---------------------------------------------------------------------------

def burst_damping(votes_in_window: int, monthly_avg_votes: float,
                  window_hours: int = BURST_WINDOW_HOURS) -> float:
    """Smooth log damping for a spike of votes.

    burst_ratio = (votes in the window) / (the mod's typical monthly volume).
    Calibrated so ratio 1.0 -> ~0.5, 1.5 -> ~0.16, 2.0 -> ~0.05, flooring at
    BURST_FLOOR. Undamped until BURST_THRESHOLD so normal activity is untouched.
    """
    if monthly_avg_votes <= 0:
        return 1.0
    burst_ratio = votes_in_window / monthly_avg_votes
    if burst_ratio <= BURST_THRESHOLD:
        return 1.0
    factor = 0.1 ** (burst_ratio - BURST_THRESHOLD)
    return max(BURST_FLOOR, min(1.0, factor))


# ---------------------------------------------------------------------------
# 3c. Time decay
# ---------------------------------------------------------------------------

def time_decay(vote_ts: int, now: int | None = None,
               halflife_days: float = RATING_DECAY_HALFLIFE_DAYS) -> float:
    now = now or int(time.time())
    age_days = max(0.0, (now - vote_ts) / 86400.0)
    return 0.5 ** (age_days / halflife_days)


# ---------------------------------------------------------------------------
# 3. Full rating aggregation
# ---------------------------------------------------------------------------

def aggregate_rating(votes: list[dict], weight_of, voter_histories: dict,
                     monthly_avg_votes: float, now: int | None = None) -> dict:
    """Combine all corrections into a single weighted mean score.

    `votes`: list of {voter_pubkey, score, ts}
    `weight_of`: callable(pubkey)->trust weight
    `voter_histories`: {pubkey: [their past scores]}
    `monthly_avg_votes`: typical monthly vote count for this mod
    """
    now = now or int(time.time())
    window_start = now - BURST_WINDOW_HOURS * 3600
    votes_in_window = sum(1 for v in votes if v["ts"] >= window_start)
    damp = burst_damping(votes_in_window, monthly_avg_votes)

    num = 0.0
    den = 0.0
    breakdown = []
    for v in votes:
        trust = max(0.0, weight_of(v["voter_pubkey"]))
        if trust <= 0:
            continue
        cal = voter_calibration(v["score"], voter_histories.get(v["voter_pubkey"], []))
        dec = time_decay(v["ts"], now)
        in_burst = v["ts"] >= window_start
        w = trust * cal * dec * (damp if in_burst else 1.0)
        num += w * v["score"]
        den += w
        breakdown.append({"voter": v["voter_pubkey"][:12], "score": v["score"],
                          "trust": round(trust, 3), "calibration": cal,
                          "decay": round(dec, 3),
                          "burst_damped": round(damp, 3) if in_burst else 1.0,
                          "effective_weight": round(w, 4)})
    score = round(num / den, 3) if den else None
    return {
        "score": score,
        "effective_votes": round(den, 3),
        "raw_votes": len(votes),
        "votes_in_burst_window": votes_in_window,
        "burst_damping": round(damp, 4),
        "breakdown": breakdown,
    }


# ---------------------------------------------------------------------------
# 4. Content flags (author baseline + community nudge)
# ---------------------------------------------------------------------------

def aggregate_content_flag(author_baseline: int, votes: list[int],
                           weight_of_idx=None) -> dict:
    """Blend the author's 0-5 baseline with community votes.

    The author's value anchors the result (they know their content) but the
    community can pull it. We give the baseline the weight of a few votes so a
    single troll vote can't swing it, but a clear community consensus will.
    """
    baseline_weight = 3.0
    num = author_baseline * baseline_weight
    den = baseline_weight
    for i, val in enumerate(votes):
        if val is None:
            continue
        w = 1.0 if weight_of_idx is None else max(0.0, weight_of_idx(i))
        num += val * w
        den += w
    blended = num / den if den else author_baseline
    return {
        "displayed": round(blended, 2),
        "author_baseline": author_baseline,
        "community_votes": len([v for v in votes if v is not None]),
    }
