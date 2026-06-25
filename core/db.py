"""SQLite layer.

Schema notes:
  users   — public keys of mod authors. We never store private keys; the
            user holds those. `current_holder` enforces "key moved to a new
            machine ⇒ must re-prove ownership".
  mods    — metadata for a published mod. Ownership = author_pubkey.
  shards  — the 7z-split pieces of a mod's content blob.
  shard_locations — which peers claim to hold a given shard (for rarity /
            replication accounting).
  peers   — known nodes, with a measured network distance (RTT-based) used
            to spread replicas far apart.
"""
from __future__ import annotations

import sqlite3
from flask import current_app, g


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    pubkey          TEXT PRIMARY KEY,
    display_name    TEXT,
    created_at      INTEGER NOT NULL,
    -- fingerprint of the machine that last "claimed" this key; if a key is
    -- used elsewhere the author is prompted to regenerate/rotate.
    current_holder  TEXT,
    holder_claimed_at INTEGER
);

CREATE TABLE IF NOT EXISTS mods (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    author_pubkey   TEXT NOT NULL,
    description     TEXT,
    version         TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    -- a few mutable "pointers" that non-authors may suggest edits to via
    -- governance; the author still controls everything else.
    category        TEXT,
    deprecated      INTEGER DEFAULT 0,
    manifest_sig    TEXT NOT NULL,        -- author signature over manifest
    -- author-controlled static metadata
    games           TEXT,                 -- JSON array of game ids/names
    -- author-set BASELINE content intensity (votes can shift the shown value)
    sexual_flag     INTEGER DEFAULT 0,    -- 0..5 (0 none, 5 focus)
    violent_flag    INTEGER DEFAULT 0,    -- 0..5
    FOREIGN KEY (author_pubkey) REFERENCES users(pubkey)
);

-- Downloadable files within a mod (a mod may ship several downloads, e.g.
-- main + optional patches). Each maps to one or more shards via shard.mod_id
-- plus this grouping. content_list is the human-facing file manifest.
CREATE TABLE IF NOT EXISTS downloadables (
    id              TEXT PRIMARY KEY,
    mod_id          TEXT NOT NULL,
    name            TEXT NOT NULL,
    kind            TEXT DEFAULT 'main',  -- main|optional|patch|translation
    description     TEXT,
    content_list    TEXT,                 -- JSON: list of contained files/paths
    created_at      INTEGER NOT NULL,
    FOREIGN KEY (mod_id) REFERENCES mods(id)
);

-- Directly-linked translations: a translation downloadable points at the mod
-- (and optionally a specific downloadable) it translates, in a given language.
CREATE TABLE IF NOT EXISTS translations (
    id              TEXT PRIMARY KEY,
    mod_id          TEXT NOT NULL,        -- the mod being translated
    translation_mod_id TEXT,              -- the mod/downloadable providing it
    language        TEXT NOT NULL,        -- BCP-47-ish code, e.g. 'de', 'pt-BR'
    translator_pubkey TEXT,
    created_at      INTEGER NOT NULL,
    FOREIGN KEY (mod_id) REFERENCES mods(id)
);

-- Game-version compatibility matrix: per (mod, game, game_version) a status.
CREATE TABLE IF NOT EXISTS compat_matrix (
    mod_id          TEXT NOT NULL,
    game            TEXT NOT NULL,
    game_version    TEXT NOT NULL,
    status          TEXT NOT NULL,        -- works|partial|broken|untested
    note            TEXT,
    set_by          TEXT,                 -- author or a voting aggregate
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (mod_id, game, game_version)
);

-- TAGS are community-voted. Category is on the mod (author-set); tags live
-- here with per-(mod,tag) vote tallies and a confirmation state.
-- tag_class is itself votable: focus|contains|minor.
CREATE TABLE IF NOT EXISTS tags (
    mod_id          TEXT NOT NULL,
    tag             TEXT NOT NULL,
    state           TEXT DEFAULT 'proposed',  -- proposed|low_weight|confirmed|rejected
    tag_class       TEXT DEFAULT 'contains',  -- focus|contains|minor (winning class)
    for_weight      REAL DEFAULT 0,
    against_weight  REAL DEFAULT 0,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (mod_id, tag)
);

-- Individual tag votes (so we can recompute with trust weights & dedupe by
-- voter). vote: +1 supports the tag, -1 opposes. class_vote: this voter's
-- opinion of the tag's class.
CREATE TABLE IF NOT EXISTS tag_votes (
    mod_id          TEXT NOT NULL,
    tag             TEXT NOT NULL,
    voter_pubkey    TEXT NOT NULL,
    vote            INTEGER NOT NULL,     -- +1 | -1
    class_vote      TEXT,                 -- focus|contains|minor
    ts              INTEGER NOT NULL,
    sig             TEXT,
    PRIMARY KEY (mod_id, tag, voter_pubkey)
);

-- Mod ratings (the 0-5 star score) with all metadata needed for calibration,
-- review-bomb damping, and time decay.
CREATE TABLE IF NOT EXISTS ratings (
    mod_id          TEXT NOT NULL,
    voter_pubkey    TEXT NOT NULL,
    score           REAL NOT NULL,        -- e.g. 1..5
    ts              INTEGER NOT NULL,
    sig             TEXT,
    PRIMARY KEY (mod_id, voter_pubkey)
);

-- Content-intensity votes (sexual/violent 0-5), aggregated to adjust the
-- author baseline. Separate from star ratings.
CREATE TABLE IF NOT EXISTS content_votes (
    mod_id          TEXT NOT NULL,
    voter_pubkey    TEXT NOT NULL,
    sexual          INTEGER,              -- 0..5 or NULL if abstain
    violent         INTEGER,              -- 0..5 or NULL
    ts              INTEGER NOT NULL,
    PRIMARY KEY (mod_id, voter_pubkey)
);

CREATE TABLE IF NOT EXISTS shards (
    id              TEXT PRIMARY KEY,     -- sha256 of shard bytes
    mod_id          TEXT NOT NULL,
    seq             INTEGER NOT NULL,     -- order within the 7z split set
    size_bytes      INTEGER NOT NULL,
    have_local      INTEGER DEFAULT 0,    -- this node holds the bytes
    created_at      INTEGER NOT NULL,
    FOREIGN KEY (mod_id) REFERENCES mods(id)
);

CREATE TABLE IF NOT EXISTS shard_locations (
    shard_id        TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    last_seen       INTEGER NOT NULL,
    PRIMARY KEY (shard_id, node_id)
);

CREATE TABLE IF NOT EXISTS peers (
    node_id         TEXT PRIMARY KEY,
    url             TEXT NOT NULL,
    verify_key      TEXT,
    distance_ms     REAL,                 -- measured RTT, proxy for net distance
    last_seen       INTEGER,
    failures        INTEGER DEFAULT 0,

    -- GLOBAL state (objective-ish, observed): gateway|common|trusted|
    -- newcomer|malicious. Derived from observed behavior + self-declared role.
    node_state      TEXT DEFAULT 'newcomer',
    -- when we first saw this node; drives the "age = trust level" tenure model.
    first_seen      INTEGER,
    -- count of files this node originated that trusted nodes later blocked.
    bad_file_count  INTEGER DEFAULT 0,
    -- self-declared role; only 'gateway' is meaningful (stores 0 files).
    declared_role   TEXT DEFAULT 'common',

    -- RELATIVE trust (this node's private opinion of the peer):
    -- friend|neutral|distant|neighbor|enemy. Orthogonal to node_state.
    relation        TEXT DEFAULT 'neutral',

    -- REPUTATION accounting. Crucially, NONE of these ever auto-ban a node.
    -- They are surfaced to users who decide whom to mark 'enemy'. A node with
    -- huge storage will naturally collect more raw bad votes, so consumers
    -- should use the RATE (bad / (good+bad)) and storage context, not totals.
    good_votes      INTEGER DEFAULT 0,    -- successful replications of known-good files
    bad_votes       INTEGER DEFAULT 0,    -- failed transfers / security flags raised
    stored_bytes    REAL DEFAULT 0,       -- self-reported, cross-checked over time
    stored_files    INTEGER DEFAULT 0
);

-- Per-(reporter -> subject) node reputation votes. Stored individually so the
-- tally can be recomputed with trust weights and so no single reporter can
-- spam a subject (one row per pair; last write wins on counts).
CREATE TABLE IF NOT EXISTS node_rep_votes (
    subject_id      TEXT NOT NULL,        -- node being rated
    reporter_id     TEXT NOT NULL,        -- node doing the rating
    good            INTEGER DEFAULT 0,
    bad             INTEGER DEFAULT 0,
    last_ts         INTEGER NOT NULL,
    sig             TEXT,
    verify_key      TEXT,
    PRIMARY KEY (subject_id, reporter_id)
);

-- Security votes on a FILE (mod). >=5% weighted security-node agreement marks
-- the file with a *warning* (never auto-removal).
CREATE TABLE IF NOT EXISTS file_security_votes (
    mod_id          TEXT NOT NULL,
    reporter_id     TEXT NOT NULL,
    verdict         TEXT NOT NULL,        -- 'malicious' | 'clean'
    ts              INTEGER NOT NULL,
    sig             TEXT,
    verify_key      TEXT,
    PRIMARY KEY (mod_id, reporter_id)
);

-- Provenance: HOW this node legitimately obtained each shard it holds.
-- 'origin' = author-signed (we published it); 'receipt' = source-node-signed
-- transfer. A node cannot claim to hold a shard without one of these, which
-- is the anti-Sybil possession proof. record is the signed JSON blob.
CREATE TABLE IF NOT EXISTS provenance (
    shard_id        TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,        -- 'origin' | 'receipt'
    source_node_id  TEXT,                 -- who gave it to us (receipt) / author
    ts              INTEGER NOT NULL,
    record          TEXT NOT NULL         -- the signed provenance JSON
);

-- Log of corruption / liveness cross-checks we have performed on peers, and
-- results we have received. Feeds reputation (good/bad votes) and our own
-- knowledge of which holders are reliable.
CREATE TABLE IF NOT EXISTS integrity_checks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    shard_id        TEXT NOT NULL,
    target_node_id  TEXT NOT NULL,
    nonce           TEXT NOT NULL,
    result          TEXT NOT NULL,        -- 'pass' | 'corrupt' | 'missing' | 'no_reference'
    ts              INTEGER NOT NULL
);

-- Signed attestations that a node holds a shard whose bytes hash to a value.
-- This is the raw material for content voting: agreement = same content_hash.
CREATE TABLE IF NOT EXISTS attestations (
    shard_id        TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    content_hash    TEXT NOT NULL,        -- what bytes the node claims to serve
    ts              INTEGER NOT NULL,
    sig             TEXT NOT NULL,        -- signature over the attestation
    verify_key      TEXT NOT NULL,
    -- 1 if the attester answered a proof-of-storage challenge correctly (or is
    -- ourselves). 0 if unverified/failed -> excluded from consensus weighting.
    proven          INTEGER DEFAULT 0,
    PRIMARY KEY (shard_id, node_id)
);

-- Cached result of the voting resolver per shard, so downloads are fast.
CREATE TABLE IF NOT EXISTS shard_consensus (
    shard_id        TEXT PRIMARY KEY,
    status          TEXT NOT NULL,        -- available|withheld|insufficient|contested
    agreed_hash     TEXT,                 -- the content_hash that won, if any
    agree_weight    REAL DEFAULT 0,
    disagree_weight REAL DEFAULT 0,
    voters          INTEGER DEFAULT 0,
    updated_at      INTEGER NOT NULL
);

-- Signed moderation flags: a node asserts a mod/shard is malicious (e.g. an
-- executable found during a security sweep). Weighted by the flagger's state.
CREATE TABLE IF NOT EXISTS moderation_flags (
    id              TEXT PRIMARY KEY,     -- hash of (target,reason,node_id)
    target_type     TEXT NOT NULL,        -- 'mod' | 'shard'
    target_id       TEXT NOT NULL,
    reason          TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    sig             TEXT NOT NULL,
    verify_key      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shards_mod ON shards(mod_id);
CREATE INDEX IF NOT EXISTS idx_loc_shard ON shard_locations(shard_id);
CREATE INDEX IF NOT EXISTS idx_attest_shard ON attestations(shard_id);
CREATE INDEX IF NOT EXISTS idx_modflag_target ON moderation_flags(target_type, target_id);
CREATE INDEX IF NOT EXISTS idx_repvotes_subject ON node_rep_votes(subject_id);
CREATE INDEX IF NOT EXISTS idx_secvotes_mod ON file_security_votes(mod_id);
CREATE INDEX IF NOT EXISTS idx_integrity_shard ON integrity_checks(shard_id);
CREATE INDEX IF NOT EXISTS idx_downloadables_mod ON downloadables(mod_id);
CREATE INDEX IF NOT EXISTS idx_translations_mod ON translations(mod_id);
CREATE INDEX IF NOT EXISTS idx_compat_mod ON compat_matrix(mod_id);
CREATE INDEX IF NOT EXISTS idx_tags_mod ON tags(mod_id);
CREATE INDEX IF NOT EXISTS idx_tagvotes_mod ON tag_votes(mod_id, tag);
CREATE INDEX IF NOT EXISTS idx_ratings_mod ON ratings(mod_id);
CREATE INDEX IF NOT EXISTS idx_contentvotes_mod ON content_votes(mod_id);
"""


def get_db() -> sqlite3.Connection:
    if "_database" not in g:
        conn = sqlite3.connect(current_app.config["DB_PATH"])
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        g._database = conn
    return g._database


def init_db() -> None:
    db = get_db()
    db.executescript(SCHEMA)
    db.commit()
