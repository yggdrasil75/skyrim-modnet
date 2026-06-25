"""Metadata & social-scoring endpoints.

Author-controlled (require signed author challenge):
  - category, games, content-flag baselines, downloadables, translations,
    compatibility matrix.
Community-voted (require a signed user challenge; one vote per user):
  - tags (+ tag class), star ratings, content-intensity votes.

Aggregations are computed on read using core.scoring, weighted by user/node
trust. We resolve a voter's trust via their node relationship where known,
falling back to a neutral weight for unknown users.
"""
from __future__ import annotations

import json
import time
import secrets

from flask import Blueprint, request, jsonify, current_app

from .db import get_db
from .identity_helpers import require_author
from . import scoring

bp = Blueprint("meta", __name__, url_prefix="/api/meta")


# --- trust weighting for users ---------------------------------------------

def _user_weight_fn(db):
    """Return a callable(pubkey)->weight. Users are weighted by the trust of
    the node that vouches for them where known; unknown users get a modest
    default so brand-new accounts can't dominate but can still participate."""
    from .trust import vote_weight
    cache: dict[str, float] = {}

    def weight(pubkey: str) -> float:
        if pubkey in cache:
            return cache[pubkey]
        row = db.execute(
            "SELECT p.node_state, p.first_seen, p.relation FROM users u "
            "LEFT JOIN peers p ON p.node_id = u.current_holder "
            "WHERE u.pubkey=?", (pubkey,)
        ).fetchone()
        if row and row["node_state"]:
            w = vote_weight({"node_state": row["node_state"],
                             "first_seen": row["first_seen"],
                             "relation": row["relation"] or "neutral"})
        else:
            w = 0.5  # unknown / unvouched user
        cache[pubkey] = w
        return w
    return weight


# --- author-controlled metadata --------------------------------------------

@bp.route("/<mod_id>/metadata", methods=["POST"])
def set_metadata(mod_id):
    db = get_db()
    mod = db.execute("SELECT author_pubkey FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not_found"), 404
    pubkey = request.form["pubkey"]
    if pubkey != mod["author_pubkey"]:
        return jsonify(error="only_author"), 403
    ok, warn = require_author(pubkey, request.form["sig"],
                             request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403
    meta = json.loads(request.form.get("meta", "{}"))
    sets, vals = [], []
    if "category" in meta:
        sets.append("category=?"); vals.append(meta["category"][:64])
    if "games" in meta:
        sets.append("games=?"); vals.append(json.dumps(meta["games"]))
    if "sexual_flag" in meta:
        sets.append("sexual_flag=?"); vals.append(max(0, min(5, int(meta["sexual_flag"]))))
    if "violent_flag" in meta:
        sets.append("violent_flag=?"); vals.append(max(0, min(5, int(meta["violent_flag"]))))
    if sets:
        vals.append(mod_id)
        db.execute(f"UPDATE mods SET {','.join(sets)} WHERE id=?", vals)
        db.commit()
    return jsonify(ok=True, warning=warn)


@bp.route("/<mod_id>/downloadable", methods=["POST"])
def add_downloadable(mod_id):
    db = get_db()
    mod = db.execute("SELECT author_pubkey FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not_found"), 404
    pubkey = request.form["pubkey"]
    if pubkey != mod["author_pubkey"]:
        return jsonify(error="only_author"), 403
    ok, warn = require_author(pubkey, request.form["sig"],
                             request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403
    d = json.loads(request.form["downloadable"])
    did = secrets.token_hex(12)
    db.execute(
        "INSERT INTO downloadables(id,mod_id,name,kind,description,content_list,created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (did, mod_id, d["name"][:128], d.get("kind", "main"),
         d.get("description", "")[:1000], json.dumps(d.get("content_list", [])),
         int(time.time())),
    )
    db.commit()
    return jsonify(ok=True, downloadable_id=did)


@bp.route("/<mod_id>/translation", methods=["POST"])
def add_translation(mod_id):
    db = get_db()
    mod = db.execute("SELECT author_pubkey FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not_found"), 404
    # translations may be contributed by others; just require a valid user sig
    body = json.loads(request.form["translation"])
    tid = secrets.token_hex(12)
    db.execute(
        "INSERT INTO translations(id,mod_id,translation_mod_id,language,"
        "translator_pubkey,created_at) VALUES(?,?,?,?,?,?)",
        (tid, mod_id, body.get("translation_mod_id"), body["language"][:16],
         request.form.get("pubkey"), int(time.time())),
    )
    db.commit()
    return jsonify(ok=True, translation_id=tid)


@bp.route("/<mod_id>/compat", methods=["POST"])
def set_compat(mod_id):
    db = get_db()
    mod = db.execute("SELECT author_pubkey FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not_found"), 404
    c = json.loads(request.form["compat"])
    db.execute(
        "INSERT INTO compat_matrix(mod_id,game,game_version,status,note,set_by,updated_at) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(mod_id,game,game_version) DO UPDATE SET "
        "status=excluded.status, note=excluded.note, updated_at=excluded.updated_at",
        (mod_id, c["game"][:64], c["game_version"][:32], c["status"],
         c.get("note", "")[:256], request.form.get("pubkey", "author"),
         int(time.time())),
    )
    db.commit()
    return jsonify(ok=True)


# --- community voting -------------------------------------------------------

@bp.route("/<mod_id>/tag-vote", methods=["POST"])
def tag_vote(mod_id):
    """One signed tag vote. body form: pubkey, sig, tag, vote(+1/-1), class_vote."""
    from .identity_helpers import require_author as require_user
    pubkey = request.form["pubkey"]
    ok, warn = require_user(pubkey, request.form["sig"],
                           request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403
    tag = request.form["tag"].strip().lower()[:48]
    vote = 1 if int(request.form.get("vote", 1)) >= 0 else -1
    class_vote = request.form.get("class_vote")
    if class_vote not in ("focus", "contains", "minor", None, ""):
        class_vote = "contains"
    db = get_db()
    now = int(time.time())
    db.execute(
        "INSERT INTO tag_votes(mod_id,tag,voter_pubkey,vote,class_vote,ts,sig) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(mod_id,tag,voter_pubkey) DO UPDATE SET "
        "vote=excluded.vote, class_vote=excluded.class_vote, ts=excluded.ts",
        (mod_id, tag, pubkey, vote, class_vote or "contains", now,
         request.form.get("sig")),
    )
    _recompute_tag(db, mod_id, tag, now)
    db.commit()
    row = db.execute("SELECT * FROM tags WHERE mod_id=? AND tag=?",
                     (mod_id, tag)).fetchone()
    return jsonify(ok=True, tag=dict(row), warning=warn)


def _recompute_tag(db, mod_id, tag, now):
    votes = [dict(r) for r in db.execute(
        "SELECT voter_pubkey, vote, class_vote FROM tag_votes WHERE mod_id=? AND tag=?",
        (mod_id, tag)).fetchall()]
    agg = scoring.aggregate_tag(votes, _user_weight_fn(db))
    db.execute(
        "INSERT INTO tags(mod_id,tag,state,tag_class,for_weight,against_weight,updated_at) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(mod_id,tag) DO UPDATE SET "
        "state=excluded.state, tag_class=excluded.tag_class, "
        "for_weight=excluded.for_weight, against_weight=excluded.against_weight, "
        "updated_at=excluded.updated_at",
        (mod_id, tag, agg["state"], agg["tag_class"], agg["for_weight"],
         agg["against_weight"], now),
    )


@bp.route("/<mod_id>/rate", methods=["POST"])
def rate(mod_id):
    """One signed star rating. body form: pubkey, sig, score."""
    from .identity_helpers import require_author as require_user
    pubkey = request.form["pubkey"]
    ok, warn = require_user(pubkey, request.form["sig"],
                           request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403
    score = max(1.0, min(5.0, float(request.form["score"])))
    db = get_db()
    db.execute(
        "INSERT INTO ratings(mod_id,voter_pubkey,score,ts,sig) VALUES(?,?,?,?,?) "
        "ON CONFLICT(mod_id,voter_pubkey) DO UPDATE SET score=excluded.score, ts=excluded.ts",
        (mod_id, pubkey, score, int(time.time()), request.form.get("sig")),
    )
    db.commit()
    return jsonify(ok=True, warning=warn)


@bp.route("/<mod_id>/content-vote", methods=["POST"])
def content_vote(mod_id):
    """Signed 0-5 sexual/violent intensity vote."""
    from .identity_helpers import require_author as require_user
    pubkey = request.form["pubkey"]
    ok, warn = require_user(pubkey, request.form["sig"],
                           request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403

    def _clamp(v):
        return None if v in (None, "") else max(0, min(5, int(v)))
    sx = _clamp(request.form.get("sexual"))
    vl = _clamp(request.form.get("violent"))
    db = get_db()
    db.execute(
        "INSERT INTO content_votes(mod_id,voter_pubkey,sexual,violent,ts) "
        "VALUES(?,?,?,?,?) ON CONFLICT(mod_id,voter_pubkey) DO UPDATE SET "
        "sexual=excluded.sexual, violent=excluded.violent, ts=excluded.ts",
        (mod_id, pubkey, sx, vl, int(time.time())),
    )
    db.commit()
    return jsonify(ok=True, warning=warn)


# --- aggregated read --------------------------------------------------------

@bp.route("/<mod_id>", methods=["GET"])
def get_full_metadata(mod_id):
    """Everything a mod page needs, with scores computed live."""
    db = get_db()
    mod = db.execute("SELECT * FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not_found"), 404
    mod = dict(mod)
    now = int(time.time())

    downloadables = [dict(r) for r in db.execute(
        "SELECT * FROM downloadables WHERE mod_id=?", (mod_id,)).fetchall()]
    for d in downloadables:
        d["content_list"] = json.loads(d["content_list"] or "[]")
    translations = [dict(r) for r in db.execute(
        "SELECT * FROM translations WHERE mod_id=?", (mod_id,)).fetchall()]
    compat = [dict(r) for r in db.execute(
        "SELECT * FROM compat_matrix WHERE mod_id=?", (mod_id,)).fetchall()]
    tags = [dict(r) for r in db.execute(
        "SELECT tag,state,tag_class,for_weight,against_weight FROM tags "
        "WHERE mod_id=? AND state != 'rejected' ORDER BY for_weight DESC",
        (mod_id,)).fetchall()]

    # rating aggregation
    rating_rows = [dict(r) for r in db.execute(
        "SELECT voter_pubkey, score, ts FROM ratings WHERE mod_id=?",
        (mod_id,)).fetchall()]
    histories = {}
    for r in rating_rows:
        histories.setdefault(r["voter_pubkey"], [])
    for pk in histories:
        histories[pk] = [x["score"] for x in db.execute(
            "SELECT score FROM ratings WHERE voter_pubkey=?", (pk,)).fetchall()]
    # estimate monthly volume from rating timestamps
    monthly = _monthly_volume(rating_rows, now)
    rating = scoring.aggregate_rating(rating_rows, _user_weight_fn(db),
                                      histories, monthly, now)
    rating.pop("breakdown", None)  # keep response lean

    # content flags
    cv = db.execute("SELECT sexual, violent FROM content_votes WHERE mod_id=?",
                    (mod_id,)).fetchall()
    sexual = scoring.aggregate_content_flag(mod["sexual_flag"],
                                            [r["sexual"] for r in cv])
    violent = scoring.aggregate_content_flag(mod["violent_flag"],
                                             [r["violent"] for r in cv])
    mod["games"] = json.loads(mod["games"] or "[]")

    return jsonify(
        mod=mod, downloadables=downloadables, translations=translations,
        compatibility=compat, tags=tags, rating=rating,
        content_flags={"sexual": sexual, "violent": violent},
    )


def _monthly_volume(rating_rows, now):
    """Rough typical monthly vote count, used as the review-bomb baseline.
    Uses the mod's lifetime vote rate; if too new, assume a small baseline."""
    if not rating_rows:
        return 1.0
    oldest = min(r["ts"] for r in rating_rows)
    months = max(1.0, (now - oldest) / (30 * 86400))
    return max(1.0, len(rating_rows) / months)
