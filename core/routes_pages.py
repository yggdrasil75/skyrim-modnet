"""Server-rendered pages.

These render from the local node's DB so browsing works with no JavaScript.
JS is layered on only for actions that require the user's key (upload, vote,
download verification). Each page passes the node identity to the template so
the header pip and footer always reflect this node.
"""
from __future__ import annotations

import json
import time

from flask import Blueprint, render_template, current_app, request, abort

from .db import get_db
from . import scoring

bp = Blueprint("pages", __name__)


def _node_ctx():
    return {
        "node_id": current_app.config["NODE_ID"],
        "node_role": current_app.config.get("DECLARED_ROLE", "common"),
    }


def _star_parts(score):
    """Return (full, half, empty) counts for a 0-5 score, for rendering."""
    if score is None:
        return 0, 0, 5
    full = int(score)
    half = 1 if (score - full) >= 0.5 else 0
    empty = 5 - full - half
    return full, half, empty


@bp.route("/")
def home():
    db = get_db()
    mod_count = db.execute("SELECT COUNT(*) c FROM mods").fetchone()["c"]
    peer_count = db.execute("SELECT COUNT(*) c FROM peers").fetchone()["c"]
    recent = _mod_cards(db, "SELECT * FROM mods WHERE deprecated=0 "
                            "ORDER BY updated_at DESC LIMIT 6")
    # a few top games by mod count
    games = _game_counts(db)[:6]
    return render_template("home.html", active="home", recent=recent,
                           games=games, mod_count=mod_count,
                           peer_count=peer_count, **_node_ctx())


@bp.route("/browse")
def browse():
    db = get_db()
    q = (request.args.get("q") or "").strip()
    category = (request.args.get("category") or "").strip()
    game = (request.args.get("game") or "").strip()
    sort = request.args.get("sort", "recent")

    sql = "SELECT * FROM mods WHERE deprecated=0"
    args = []
    if q:
        sql += " AND (name LIKE ? OR description LIKE ?)"
        args += [f"%{q}%", f"%{q}%"]
    if category:
        sql += " AND category = ?"
        args.append(category)
    if game:
        sql += " AND games LIKE ?"
        args.append(f"%{game}%")
    sql += " ORDER BY " + ("name" if sort == "name" else "updated_at DESC")
    sql += " LIMIT 60"
    cards = _mod_cards(db, sql, args)

    categories = [r["category"] for r in db.execute(
        "SELECT DISTINCT category FROM mods WHERE category IS NOT NULL "
        "AND category != '' ORDER BY category").fetchall()]
    return render_template("browse.html", active="browse", cards=cards,
                           q=q, category=category, game=game, sort=sort,
                           categories=categories, **_node_ctx())


@bp.route("/games")
def games():
    db = get_db()
    return render_template("games.html", active="games",
                           games=_game_counts(db), **_node_ctx())


@bp.route("/game/<path:game>")
def game_page(game):
    db = get_db()
    cards = _mod_cards(db, "SELECT * FROM mods WHERE games LIKE ? AND deprecated=0 "
                           "ORDER BY updated_at DESC LIMIT 100", [f"%{game}%"])
    # category breakdown within this game
    cats = {}
    for c in cards:
        cats[c["category"]] = cats.get(c["category"], 0) + 1
    return render_template("game.html", active="games", game=game,
                           cards=cards, cats=sorted(cats.items()), **_node_ctx())


@bp.route("/mod/<mod_id>")
def mod_page(mod_id):
    db = get_db()
    mod = db.execute("SELECT * FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        abort(404)
    mod = dict(mod)
    mod["games"] = json.loads(mod["games"] or "[]")
    now = int(time.time())

    downloadables = []
    for r in db.execute("SELECT * FROM downloadables WHERE mod_id=?", (mod_id,)):
        d = dict(r)
        d["content_list"] = json.loads(d["content_list"] or "[]")
        downloadables.append(d)
    translations = [dict(r) for r in db.execute(
        "SELECT * FROM translations WHERE mod_id=?", (mod_id,))]
    compat = [dict(r) for r in db.execute(
        "SELECT * FROM compat_matrix WHERE mod_id=? ORDER BY game, game_version",
        (mod_id,))]
    tags = [dict(r) for r in db.execute(
        "SELECT tag,state,tag_class,for_weight,against_weight FROM tags "
        "WHERE mod_id=? AND state!='rejected' ORDER BY "
        "CASE state WHEN 'confirmed' THEN 0 WHEN 'low_weight' THEN 1 ELSE 2 END, "
        "for_weight DESC", (mod_id,))]

    rating = _rating_for(db, mod_id, now)
    full, half, empty = _star_parts(rating["score"])

    cv = db.execute("SELECT sexual, violent FROM content_votes WHERE mod_id=?",
                    (mod_id,)).fetchall()
    sexual = scoring.aggregate_content_flag(mod["sexual_flag"],
                                            [r["sexual"] for r in cv])
    violent = scoring.aggregate_content_flag(mod["violent_flag"],
                                             [r["violent"] for r in cv])

    # shard/consensus availability summary
    shards = db.execute(
        "SELECT s.id, COALESCE(c.status,'insufficient') status FROM shards s "
        "LEFT JOIN shard_consensus c ON c.shard_id=s.id WHERE s.mod_id=?",
        (mod_id,)).fetchall()
    avail = {"available": 0, "total": len(shards)}
    for s in shards:
        if s["status"] == "available":
            avail["available"] += 1
    downloadable_now = avail["total"] > 0 and avail["available"] == avail["total"]

    return render_template("mod.html", active="browse", mod=mod,
                           downloadables=downloadables, translations=translations,
                           compat=compat, tags=tags, rating=rating,
                           stars=(full, half, empty),
                           sexual=sexual, violent=violent,
                           avail=avail, downloadable_now=downloadable_now,
                           **_node_ctx())


@bp.route("/upload")
def upload():
    db = get_db()
    categories = [r["category"] for r in db.execute(
        "SELECT DISTINCT category FROM mods WHERE category IS NOT NULL "
        "AND category!='' ORDER BY category").fetchall()]
    return render_template("upload.html", active="upload",
                           categories=categories, **_node_ctx())


@bp.route("/status")
def status():
    db = get_db()
    cfg = current_app.config
    counts = {
        "mods": db.execute("SELECT COUNT(*) c FROM mods").fetchone()["c"],
        "shards": db.execute("SELECT COUNT(*) c FROM shards").fetchone()["c"],
        "held": db.execute("SELECT COUNT(*) c FROM shards WHERE have_local=1").fetchone()["c"],
        "peers": db.execute("SELECT COUNT(*) c FROM peers").fetchone()["c"],
        "users": db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"],
    }
    storage = db.execute(
        "SELECT COALESCE(SUM(size_bytes),0) b FROM shards WHERE have_local=1"
    ).fetchone()["b"]
    peers = [dict(r) for r in db.execute(
        "SELECT node_id,url,node_state,relation,distance_ms,good_votes,bad_votes,"
        "last_seen FROM peers ORDER BY distance_ms IS NULL, distance_ms LIMIT 50")]
    # under-replicated shards (rarity)
    target = cfg["TARGET_REPLICAS"]
    rare = db.execute(
        "SELECT COUNT(*) c FROM (SELECT shard_id FROM shard_locations "
        "GROUP BY shard_id HAVING COUNT(DISTINCT node_id) < ?)", (target,)
    ).fetchone()["c"]
    return render_template("status.html", active="status", counts=counts,
                           storage=storage, peers=peers, target=target,
                           rare=rare, public_url=cfg.get("PUBLIC_URL"),
                           now=int(time.time()), now_ts=int(time.time()),
                           **_node_ctx())


# --- helpers ---------------------------------------------------------------

def _mod_cards(db, sql, args=()):
    rows = db.execute(sql, args).fetchall()
    now = int(time.time())
    cards = []
    for r in rows:
        m = dict(r)
        m["games"] = json.loads(m["games"] or "[]")
        rating = _rating_for(db, m["id"], now, lean=True)
        m["score"] = rating["score"]
        m["star_parts"] = _star_parts(rating["score"])
        m["tag_list"] = [t["tag"] for t in db.execute(
            "SELECT tag FROM tags WHERE mod_id=? AND state IN "
            "('confirmed','low_weight') ORDER BY for_weight DESC LIMIT 4",
            (m["id"],)).fetchall()]
        cards.append(m)
    return cards


def _rating_for(db, mod_id, now, lean=False):
    rows = [dict(r) for r in db.execute(
        "SELECT voter_pubkey, score, ts FROM ratings WHERE mod_id=?", (mod_id,))]
    if not rows:
        return {"score": None, "raw_votes": 0, "effective_votes": 0,
                "burst_damping": 1.0}
    histories = {}
    for r in rows:
        histories.setdefault(r["voter_pubkey"], [])
    for pk in histories:
        histories[pk] = [x["score"] for x in db.execute(
            "SELECT score FROM ratings WHERE voter_pubkey=?", (pk,)).fetchall()]
    oldest = min(r["ts"] for r in rows)
    months = max(1.0, (now - oldest) / (30 * 86400))
    monthly = max(1.0, len(rows) / months)
    res = scoring.aggregate_rating(rows, _weight_fn(db), histories, monthly, now)
    if lean:
        res.pop("breakdown", None)
    return res


def _weight_fn(db):
    from .trust import vote_weight
    cache = {}

    def w(pubkey):
        if pubkey in cache:
            return cache[pubkey]
        row = db.execute(
            "SELECT p.node_state,p.first_seen,p.relation FROM users u "
            "LEFT JOIN peers p ON p.node_id=u.current_holder WHERE u.pubkey=?",
            (pubkey,)).fetchone()
        val = (vote_weight({"node_state": row["node_state"],
                            "first_seen": row["first_seen"],
                            "relation": row["relation"] or "neutral"})
               if row and row["node_state"] else 0.5)
        cache[pubkey] = val
        return val
    return w


def _game_counts(db):
    counts = {}
    for r in db.execute("SELECT games FROM mods WHERE deprecated=0"):
        for g in json.loads(r["games"] or "[]"):
            counts[g] = counts.get(g, 0) + 1
    return sorted(({"name": k, "count": v} for k, v in counts.items()),
                  key=lambda x: -x["count"])
