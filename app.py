"""
DistMod — a decentralized, ownerless mod-hosting network node.

Design goals (see README):
  - No single point of financial / moderation / jurisdictional failure.
  - Each node is autonomous and runs the same code; the "network" is the
    set of nodes that gossip with each other. There is no master.
  - Users are identified by a self-generated keypair, not an account on a
    server, so identity survives any single node disappearing.

This file wires together the blueprints. Most real logic lives in the
modules under ./core so each concern stays testable in isolation.
"""
from __future__ import annotations

import os
from flask import Flask, render_template, g

from core.db import init_db, get_db
from core.config import load_config
from core.routes_identity import bp as identity_bp
from core.routes_mods import bp as mods_bp
from core.routes_network import bp as network_bp
from core.scheduler import start_background_jobs


def create_app(config_overrides: dict | None = None) -> Flask:
    app = Flask(__name__)
    cfg = load_config()
    if config_overrides:
        cfg.update(config_overrides)
    app.config.update(cfg)

    os.makedirs(app.config["NODE_DATA_DIR"], exist_ok=True)
    os.makedirs(app.config["BLOB_DIR"], exist_ok=True)

    with app.app_context():
        init_db()

    app.register_blueprint(identity_bp)
    app.register_blueprint(mods_bp)
    app.register_blueprint(network_bp)

    @app.teardown_appcontext
    def _close_db(_exc):
        db = g.pop("_database", None)
        if db is not None:
            db.close()

    @app.route("/")
    def index():
        db = get_db()
        mod_count = db.execute("SELECT COUNT(*) c FROM mods").fetchone()["c"]
        peer_count = db.execute("SELECT COUNT(*) c FROM peers").fetchone()["c"]
        return render_template(
            "home.html",
            node_id=app.config["NODE_ID"],
            mod_count=mod_count,
            peer_count=peer_count,
        )

    if app.config.get("ENABLE_BACKGROUND_JOBS", True):
        start_background_jobs(app)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
