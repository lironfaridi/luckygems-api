# elite_content.py
# =============================================================================
#  Elite Adaptation backend content + state (Sprints 1-7).
#
#  Dependency-free on purpose: NO FastAPI, NO import of server.py -> no circular
#  import. server.py imports THIS module and the thin route handlers call these
#  pure functions. State is in-memory MOCK (resets on restart) to simulate DB
#  queries; the JSON shapes match docs/SERVER_CONTRACT_*.md exactly.
#
#  CATALOGS mirrors the client's ContentRegistry._OFFLINE_FALLBACK byte-for-byte
#  so a live /meta/catalog == the client's offline fallback (no drift).
# =============================================================================
import time
import threading

CONTENT_VERSION = 1
MIN_SUPPORTED_CONTENT_VERSION = 1

# Generous mock starting balances so the new flows are testable immediately.
DEFAULT_GOLD = 50000
DEFAULT_GEMS = 500

# -----------------------------------------------------------------------------
#  Static catalogs (mirror ContentRegistry._OFFLINE_FALLBACK)
# -----------------------------------------------------------------------------
CATALOGS = {
    "iap_bundles": [
        {"item_id": "gems_50",        "name": "Gem Pouch",         "price": "$0.99",  "gems":   50, "gold":    0, "variant": "blue",   "badge": ""},
        {"item_id": "bundle_welcome", "name": "Royal Starter Kit", "price": "$1.99",  "gems":  120, "gold": 1000, "variant": "gold",   "badge": "WELCOME BUNDLE"},
        {"item_id": "gems_350",       "name": "Gem Chest",         "price": "$4.99",  "gems":  350, "gold":    0, "variant": "purple", "badge": ""},
        {"item_id": "gems_1500",      "name": "Royal Vault",       "price": "$9.99",  "gems": 1500, "gold":    0, "variant": "gold",   "badge": ""},
        {"item_id": "gems_1800",      "name": "Royal Hoard",       "price": "$19.99", "gems": 1800, "gold":    0, "variant": "purple", "badge": "MOST POPULAR"},
        {"item_id": "gems_6000",      "name": "Titan Reserve",     "price": "$49.99", "gems": 6000, "gold":    0, "variant": "gold",   "badge": "BEST VALUE"},
    ],
    "boosts": [
        {"id": "boost_cashout_2x", "name": "Cashout Boost x2",  "description": "All cashout payouts are doubled for the duration.",        "cost_gold": 500, "duration_min": 15, "icon": "res://assets/golden_crystal.png"},
        {"id": "boost_spawn_rate", "name": "Lucky Spawn Boost", "description": "Higher-tier gems spawn more frequently on the board.",     "cost_gold": 750, "duration_min": 20, "icon": "res://assets/GEM2.png"},
        {"id": "boost_piggy_fill", "name": "Piggy Rush",        "description": "Gold deposited per merge into the Piggy Bank is doubled.", "cost_gold": 400, "duration_min": 30, "icon": "res://assets/icon_piggy.png"},
    ],
    "special_tiles": [
        {"id": "wildcard_core",    "name": "Wildcard Gem",    "description": "A wildcard gem auto-spawns and merges with any normal gem.",             "cost":  20, "currency": "gems", "req_tier": 4, "icon": "res://assets/wildcard.png",                     "tooltip": "Merges with any normal gem to instantly upgrade it!",                 "requires": ""},
        {"id": "golden_license",   "name": "Golden Crystal",  "description": "A golden crystal auto-spawns and multiplies your cashout by x4.",        "cost":  80, "currency": "gems", "req_tier": 4, "icon": "res://assets/golden_crystal.png",               "tooltip": "Multiplies your cashout row by x4!",                                  "requires": "",               "tint": False},
        {"id": "golden_license_2", "name": "Rock Cleanser",   "description": "Hammer the Rock Cleanser to instantly destroy ALL Cursed Tiles at once.", "cost": 100, "currency": "gems", "req_tier": 5, "icon": "res://assets/icon_rock_cleanser.png",           "tooltip": "Smash with a Hammer to destroy all Cursed Tiles!",                    "requires": "golden_license", "tint": False},
        {"id": "catalyst_core",    "name": "Fusion Catalyst", "description": "Smash with a Hammer to upgrade all adjacent gems by +1!",                 "cost": 120, "currency": "gems", "req_tier": 5, "icon": "res://assets/icon_hammer_upgrade_adjacent.png", "tooltip": "Smash with a Hammer to upgrade all adjacent gems by +1!",             "requires": ""},
        {"id": "board_insurance",  "name": "Board Insurance", "description": "Survive one game-over with your gems intact.",                           "cost": 150, "currency": "gems", "req_tier": 1, "icon": "res://assets/icon_insurance.png",               "tooltip": "When your board fills up, destroys Cursed Tiles and saves your run!", "requires": ""},
    ],
    "chapters": [
        {"id": "ch_first_cashout", "title": "First Fortune",    "goal_id": "run_cashouts",            "target": 1,    "reward": {"gems": 10}},
        {"id": "ch_combo_3",       "title": "Combo Starter",    "goal_id": "run_combo_count",         "target": 3,    "reward": {"gems": 15}},
        {"id": "ch_merges_50",     "title": "Merge Apprentice", "goal_id": "run_merges",              "target": 50,   "reward": {"gems": 20}},
        {"id": "ch_big_cashout",   "title": "Big Score",        "goal_id": "run_best_single_cashout", "target": 2000, "reward": {"gems": 30}},
    ],
    "estate_rooms": [
        {
            "id": "room_foyer", "name": "The Grand Foyer", "bg_asset": "res://assets/estate/foyer_bg.png",
            "reward": {"gems": 25}, "unlock_requires": "",
            "tasks": [
                {"id": "foyer_floor",      "label": "Marble Floor",       "cost": 800,  "asset_broken": "res://assets/estate/foyer_floor_broken.png",      "asset_restored": "res://assets/estate/foyer_floor.png",      "state": "broken"},
                {"id": "foyer_chandelier", "label": "Crystal Chandelier", "cost": 1500, "asset_broken": "res://assets/estate/foyer_chandelier_broken.png", "asset_restored": "res://assets/estate/foyer_chandelier.png", "state": "broken"},
                {"id": "foyer_doors",      "label": "Golden Doors",       "cost": 1200, "asset_broken": "res://assets/estate/foyer_doors_broken.png",      "asset_restored": "res://assets/estate/foyer_doors.png",      "state": "broken"},
            ],
        },
        {
            "id": "room_treasury", "name": "The Treasury", "bg_asset": "res://assets/estate/treasury_bg.png",
            "reward": {"gems": 40}, "unlock_requires": "room_foyer",
            "tasks": [
                {"id": "treasury_vault_door", "label": "Vault Door",  "cost": 2500, "asset_broken": "res://assets/estate/treasury_door_broken.png",    "asset_restored": "res://assets/estate/treasury_door.png",    "state": "broken"},
                {"id": "treasury_shelves",    "label": "Gem Shelves", "cost": 2000, "asset_broken": "res://assets/estate/treasury_shelves_broken.png", "asset_restored": "res://assets/estate/treasury_shelves.png", "state": "broken"},
            ],
        },
    ],
    "events": [
        {"id": "ev_weekend_cup", "type": "tournament",   "title": "Weekend Cashout Cup", "ends_at": 0, "progress_goal_id": "run_cashouts",      "progress": 0, "milestones": [{"threshold": 5, "reward": {"gems": 10}, "claimed": False}, {"threshold": 15, "reward": {"gems": 25}, "claimed": False}, {"threshold": 40, "reward": {"gems": 60}, "claimed": False}], "rewards": {}},
        {"id": "ev_curse_purge", "type": "shard_rush",   "title": "Curse Purge",         "ends_at": 0, "progress_goal_id": "run_cursed_removed", "progress": 0, "milestones": [{"threshold": 20, "reward": {"gems": 8}, "claimed": False}, {"threshold": 80, "reward": {"gems": 30}, "claimed": False}], "rewards": {}},
        {"id": "ev_estate_gala", "type": "estate_event", "title": "Estate Gala",         "ends_at": 0, "progress_goal_id": "run_merges",         "progress": 0, "milestones": [{"threshold": 100, "reward": {"type": "estate_decor", "label": "Gala Banner"}, "claimed": False}, {"threshold": 400, "reward": {"gems": 40}, "claimed": False}], "rewards": {}},
    ],
    "offers": {
        "default": {"product_id": "gems_350",      "title": "Starter Deal",   "blurb": "A solid first pickup.",      "badge": "DEAL"},
        "new":     {"product_id": "bundle_welcome", "title": "Welcome Bundle", "blurb": "120 gems + 1,000 gold.",     "badge": "NEW PLAYER"},
        "minnow":  {"product_id": "gems_50",        "title": "Quick Top-Up",   "blurb": "Just enough to keep going.", "badge": "VALUE"},
        "dolphin": {"product_id": "gems_1500",      "title": "Vault Filler",   "blurb": "Stock up and save.",         "badge": "POPULAR"},
        "whale":   {"product_id": "gems_6000",      "title": "Titan Reserve",  "blurb": "Maximum value.",             "badge": "BEST VALUE"},
        "lapsing": {"product_id": "gems_350",       "title": "We Miss You",    "blurb": "Bonus value to return.",     "badge": "COMEBACK"},
    },
    "album": [
        {"id": "tier_1",  "value": 1,  "kind": "tier", "name": "Emerald",           "flavor": "Where every fortune begins.",    "reward": {"gems": 3}},
        {"id": "tier_2",  "value": 2,  "kind": "tier", "name": "Sapphire",          "flavor": "A deeper shade of wealth.",      "reward": {"gems": 3}},
        {"id": "tier_3",  "value": 3,  "kind": "tier", "name": "Ruby",              "flavor": "Heat that draws the eye.",       "reward": {"gems": 4}},
        {"id": "tier_4",  "value": 4,  "kind": "tier", "name": "Topaz",             "flavor": "Golden glint of momentum.",      "reward": {"gems": 5}},
        {"id": "tier_5",  "value": 5,  "kind": "tier", "name": "Amethyst",          "flavor": "Royalty in every facet.",        "reward": {"gems": 6}},
        {"id": "tier_6",  "value": 6,  "kind": "tier", "name": "Onyx",              "flavor": "Power forged in shadow.",        "reward": {"gems": 8}},
        {"id": "tier_7",  "value": 7,  "kind": "tier", "name": "Star Opal",         "flavor": "A galaxy held in glass.",        "reward": {"gems": 10}},
        {"id": "tier_8",  "value": 8,  "kind": "tier", "name": "Prismatic Crystal", "flavor": "Light splits into fortune.",     "reward": {"gems": 14}},
        {"id": "tier_9",  "value": 9,  "kind": "tier", "name": "Astral Core",       "flavor": "The heart of a fallen star.",    "reward": {"gems": 18}},
        {"id": "tier_10", "value": 10, "kind": "tier", "name": "Divine Crown",      "flavor": "Worn only by the worthy.",       "reward": {"gems": 24}},
        {"id": "tier_11", "value": 11, "kind": "tier", "name": "Infinity Heart",    "flavor": "Wealth without end.",            "reward": {"gems": 30}},
        {"id": "sp_wildcard", "value": 98, "kind": "special", "name": "Wildcard",        "flavor": "Becomes whatever it touches.",   "reward": {"gems": 15}},
        {"id": "sp_golden",   "value": 99, "kind": "special", "name": "Golden Crystal",  "flavor": "Multiplies a cashout fourfold.", "reward": {"gems": 20}},
        {"id": "sp_catalyst", "value": 97, "kind": "special", "name": "Fusion Catalyst", "flavor": "Lifts all its neighbours.",      "reward": {"gems": 18}},
        {"id": "sp_cleanser", "value": 95, "kind": "special", "name": "Rock Cleanser",   "flavor": "Purges every curse at once.",    "reward": {"gems": 22}},
        {"id": "sp_x4",       "value": 96, "kind": "special", "name": "X4 Diamond",      "flavor": "A lucky-spin treasure.",         "reward": {"gems": 25}},
    ],
}

# -----------------------------------------------------------------------------
#  Persistent shared state (multi-worker fix): EliteStore prefers Redis -- which is
#  cross-worker consistent AND survives restarts -- and transparently falls back to
#  in-process dicts when REDIS_URL is unset (local dev / single worker). The
#  threading.Lock guards ONLY the in-memory fallback; with Redis, Redis itself is
#  the cross-process sync point. Public function signatures are UNCHANGED, so
#  server.py route handlers need zero edits.
#
#  Key scheme: elite:{domain}:{player_id}  (sets for memberships, hashes for maps).
# -----------------------------------------------------------------------------
import os


class EliteStore:
    def __init__(self):
        self._r = None
        self._mem = {}                 # key -> set | dict (mirrors Redis value types)
        self._lock = threading.Lock()  # fallback-only synchronization
        url = os.environ.get("REDIS_URL", "").strip() or os.environ.get("REDIS", "").strip()
        if url:
            try:
                import redis as _redis
                self._r = _redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
                self._r.ping()
            except Exception as _e:
                print("[elite] Redis unavailable (%s) -- using in-memory fallback" % _e)
                self._r = None
        self.backend = "redis" if self._r is not None else "memory"
        print("[elite] EliteStore backend = %s" % self.backend)

    # ---- set ops (string members) ----
    def sadd(self, key, member):
        m = str(member)
        if self._r is not None:
            self._r.sadd(key, m)
        else:
            with self._lock:
                self._mem.setdefault(key, set()).add(m)

    def smembers(self, key):
        if self._r is not None:
            return set(self._r.smembers(key))
        with self._lock:
            return set(self._mem.get(key, set()))

    def sismember(self, key, member):
        m = str(member)
        if self._r is not None:
            return bool(self._r.sismember(key, m))
        with self._lock:
            return m in self._mem.get(key, set())

    # ---- hash ops (string fields/values) ----
    def hget(self, key, field, default=None):
        if self._r is not None:
            v = self._r.hget(key, field)
            return v if v is not None else default
        with self._lock:
            return self._mem.get(key, {}).get(field, default)

    def hset(self, key, field, value):
        if self._r is not None:
            self._r.hset(key, field, str(value))
        else:
            with self._lock:
                self._mem.setdefault(key, {})[field] = str(value)

    def hincrby(self, key, field, amount):
        if self._r is not None:
            return int(self._r.hincrby(key, field, int(amount)))
        with self._lock:
            d = self._mem.setdefault(key, {})
            d[field] = str(int(d.get(field, "0")) + int(amount))
            return int(d[field])


STORE = EliteStore()


# ---------------------------------------------------------------------------
#  Mock wallet (isolated from the real economy DB; swap for the players table).
# ---------------------------------------------------------------------------
def _wkey(pid):
    return "elite:wallet:%s" % pid


def _wallet_init(pid):
    key = _wkey(pid)
    if STORE.hget(key, "gold") is None:
        STORE.hset(key, "gold", DEFAULT_GOLD)
        STORE.hset(key, "gems", DEFAULT_GEMS)
    return key


def _wallet_get(pid):
    key = _wallet_init(pid)
    return {"gold": int(STORE.hget(key, "gold", DEFAULT_GOLD)),
            "gems": int(STORE.hget(key, "gems", DEFAULT_GEMS))}


def _wallet_add_gems(pid, n):
    if int(n) != 0:
        STORE.hincrby(_wallet_init(pid), "gems", int(n))


def _wallet_spend_gold(pid, n):
    key = _wallet_init(pid)
    if int(STORE.hget(key, "gold", 0)) < int(n):
        return False
    STORE.hincrby(key, "gold", -int(n))
    return True


def _wallet_spend_gems(pid, n):
    key = _wallet_init(pid)
    if int(STORE.hget(key, "gems", 0)) < int(n):
        return False
    STORE.hincrby(key, "gems", -int(n))
    return True


def wallet_snapshot(pid):
    return _wallet_get(pid)


def meta_catalog():
    return {
        "content_version": CONTENT_VERSION,
        "min_supported_content_version": MIN_SUPPORTED_CONTENT_VERSION,
        "catalogs": CATALOGS,
    }


# ---------------------------------------------------------------------------
#  Chapters
# ---------------------------------------------------------------------------
def _chapters():
    return CATALOGS["chapters"]


def chapter_state(pid):
    key = "elite:chapter:%s" % pid
    cur = STORE.hget(key, "current_chapter_id")
    if cur is None:
        cur = _chapters()[0]["id"] if _chapters() else ""
        STORE.hset(key, "current_chapter_id", cur)
        STORE.hset(key, "progress", 0)
    return {"current_chapter_id": cur, "progress": int(STORE.hget(key, "progress", 0))}


def complete_chapter(pid, chapter_id):
    chapters = _chapters()
    first = chapters[0]["id"] if chapters else ""
    ckey = "elite:chapter:%s" % pid
    dkey = "elite:chapter_done:%s" % pid
    if STORE.hget(ckey, "current_chapter_id") is None:
        STORE.hset(ckey, "current_chapter_id", first)
        STORE.hset(ckey, "progress", 0)
    ch = next((c for c in chapters if c["id"] == chapter_id), None)
    if ch is None or STORE.sismember(dkey, chapter_id):
        return {"status": "noop"}
    # Anti-exploit: only the CURRENT chapter, and only once its goal target is met.
    if chapter_id != STORE.hget(ckey, "current_chapter_id"):
        return {"status": "error", "message": "not current chapter"}
    if int(STORE.hget(ckey, "progress", 0)) < int(ch.get("target", 0)):
        return {"status": "error", "message": "not reached"}
    STORE.sadd(dkey, chapter_id)
    reward = ch.get("reward", {})
    _wallet_add_gems(pid, int(reward.get("gems", 0)))
    nxt = ""
    for c in chapters:
        if not STORE.sismember(dkey, c["id"]):
            nxt = c["id"]
            break
    STORE.hset(ckey, "current_chapter_id", nxt or chapter_id)
    STORE.hset(ckey, "progress", 0)
    out = {"status": "success", "chapter_id": chapter_id, "reward": reward}
    if nxt:
        out["next_chapter_id"] = nxt
    return out


# ---------------------------------------------------------------------------
#  Vault Estate
# ---------------------------------------------------------------------------
def _room_of_task(task_id):
    for r in CATALOGS["estate_rooms"]:
        for t in r["tasks"]:
            if t["id"] == task_id:
                return r
    return None


def _task_cost(task_id):
    for r in CATALOGS["estate_rooms"]:
        for t in r["tasks"]:
            if t["id"] == task_id:
                return int(t["cost"])
    return None


def _maybe_pay_room(pid, room):
    fkey = "elite:estate_funded:%s" % pid
    pkey = "elite:estate_paid:%s" % pid
    funded = STORE.smembers(fkey)
    rid = room["id"]
    if not STORE.sismember(pkey, rid) and all(t["id"] in funded for t in room["tasks"]):
        STORE.sadd(pkey, rid)
        _wallet_add_gems(pid, int(room.get("reward", {}).get("gems", 0)))


def estate_state(pid):
    funded = STORE.smembers("elite:estate_funded:%s" % pid)
    cur = ""
    for r in CATALOGS["estate_rooms"]:
        if not all(t["id"] in funded for t in r["tasks"]):
            cur = r["id"]
            break
    if not cur and CATALOGS["estate_rooms"]:
        cur = CATALOGS["estate_rooms"][-1]["id"]
    return {"current_room_id": cur, "funded_tasks": sorted(funded)}


def fund_task(pid, task_id, _client_cost):
    cost = _task_cost(task_id)
    if cost is None:
        return {"status": "error", "message": "Unknown task"}
    fkey = "elite:estate_funded:%s" % pid
    if not STORE.sismember(fkey, task_id):
        if not _wallet_spend_gold(pid, cost):
            return {"status": "error", "message": "Insufficient funds"}
        STORE.sadd(fkey, task_id)
        room = _room_of_task(task_id)
        if room:
            _maybe_pay_room(pid, room)
    return {"status": "success", "task_id": task_id,
            "funded_tasks": sorted(STORE.smembers(fkey)),
            "new_balance": _wallet_get(pid)["gold"]}


def finish_room(pid, room_id):
    room = next((r for r in CATALOGS["estate_rooms"] if r["id"] == room_id), None)
    if room is None:
        return {"status": "error", "message": "Unknown room"}
    fkey = "elite:estate_funded:%s" % pid
    funded = STORE.smembers(fkey)
    remaining = [t for t in room["tasks"] if t["id"] not in funded]
    gem_price = len(remaining) * 25
    if gem_price > 0:
        if not _wallet_spend_gems(pid, gem_price):
            return {"status": "error", "message": "Insufficient gems"}
        for t in remaining:
            STORE.sadd(fkey, t["id"])
        _maybe_pay_room(pid, room)
    return {"status": "success",
            "funded_tasks": sorted(STORE.smembers(fkey)),
            "new_balance": _wallet_get(pid)["gold"]}


# ---------------------------------------------------------------------------
#  Collection Album
# ---------------------------------------------------------------------------
def album_state(pid):
    return {"discovered": sorted(int(v) for v in STORE.smembers("elite:album:%s" % pid))}


def claim_discovery(pid, value):
    card = next((c for c in CATALOGS["album"] if int(c["value"]) == int(value)), None)
    if card is None:
        return {"status": "error", "message": "Unknown value"}
    akey = "elite:album:%s" % pid
    if STORE.sismember(akey, value):
        return {"status": "success", "value": value, "reward": {}}
    STORE.sadd(akey, value)
    reward = card.get("reward", {})
    _wallet_add_gems(pid, int(reward.get("gems", 0)))
    return {"status": "success", "value": value, "reward": reward}


# ---------------------------------------------------------------------------
#  Live-Ops Event Calendar
# ---------------------------------------------------------------------------
def active_events(pid):
    now = int(time.time())
    pkey = "elite:event_prog:%s" % pid
    out = []
    for ev in CATALOGS["events"]:
        ends = int(ev.get("ends_at", 0))
        if ends != 0 and ends <= now:
            continue
        prog = int(STORE.hget(pkey, ev["id"], int(ev.get("progress", 0))))
        claimed = STORE.smembers("elite:event_claimed:%s:%s" % (pid, ev["id"]))
        milestones = [{"threshold": m["threshold"], "reward": m["reward"],
                       "claimed": str(m["threshold"]) in claimed} for m in ev["milestones"]]
        entry = {k: v for k, v in ev.items() if k != "milestones"}
        entry["progress"] = prog
        entry["milestones"] = milestones
        out.append(entry)
    return {"events": out}


def claim_milestone(pid, event_id, threshold):
    ev = next((e for e in CATALOGS["events"] if e["id"] == event_id), None)
    if ev is None:
        return {"status": "error", "message": "Unknown event"}
    m = next((mm for mm in ev["milestones"] if int(mm["threshold"]) == int(threshold)), None)
    if m is None:
        return {"status": "error", "message": "Unknown milestone"}
    ckey = "elite:event_claimed:%s:%s" % (pid, event_id)
    if STORE.sismember(ckey, threshold):
        return {"status": "noop"}
    # Anti-exploit: the server must have accrued progress >= threshold for this event.
    prog = int(STORE.hget("elite:event_prog:%s" % pid, event_id, int(ev.get("progress", 0))))
    if prog < int(threshold):
        return {"status": "error", "message": "not reached"}
    STORE.sadd(ckey, threshold)
    reward = m.get("reward", {})
    if isinstance(reward, dict) and int(reward.get("gems", 0)) > 0:
        _wallet_add_gems(pid, int(reward["gems"]))
    return {"status": "success", "event_id": event_id, "threshold": threshold, "reward": reward}


# Maps /stats/submit_run payload field names -> meta goal_ids.
_RUN_FIELD_MAP = {
    "cashouts":             "run_cashouts",
    "run_merges":           "run_merges",
    "run_combo_count":      "run_combo_count",
    "cursed_removed":       "run_cursed_removed",
    "best_cashout_run":     "run_best_single_cashout",
    "weekly_cashout_total": "session_cashout_total",
}
# Monotonic 'best' goals accumulate with max(); the rest are counters (+=).
_MAX_GOALS = {"run_best_single_cashout", "session_cashout_total"}


def accrue_run(player_id, stats):
    # Normalize the submit_run payload to goal_id -> int value.
    goals = {}
    for src, goal in _RUN_FIELD_MAP.items():
        if src in stats:
            try:
                goals[goal] = int(stats[src])
            except (TypeError, ValueError):
                pass
    if not goals:
        return
    # Advance every active event whose progress_goal_id matches a reported stat.
    pkey = "elite:event_prog:%s" % player_id
    for ev in CATALOGS["events"]:
        gid = ev.get("progress_goal_id", "")
        if gid in goals:
            cur = int(STORE.hget(pkey, ev["id"], int(ev.get("progress", 0))))
            val = goals[gid]
            STORE.hset(pkey, ev["id"], max(cur, val) if gid in _MAX_GOALS else cur + val)
    # Advance the CURRENT chapter's progress.
    ckey = "elite:chapter:%s" % player_id
    cur_id = STORE.hget(ckey, "current_chapter_id")
    if cur_id is None:
        cur_id = _chapters()[0]["id"] if _chapters() else ""
        STORE.hset(ckey, "current_chapter_id", cur_id)
        STORE.hset(ckey, "progress", 0)
    ch = next((c for c in _chapters() if c["id"] == cur_id), None)
    if ch:
        gid = ch.get("goal_id", "")
        if gid in goals:
            cur = int(STORE.hget(ckey, "progress", 0))
            val = goals[gid]
            STORE.hset(ckey, "progress", max(cur, val) if gid in _MAX_GOALS else cur + val)


# ---------------------------------------------------------------------------
#  Monetization -- offer segment (real, spend + lifecycle based)
# ---------------------------------------------------------------------------
import datetime as _dt


def _days_since(iso_str, now):
    if not iso_str:
        return None
    try:
        d = _dt.datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        if d.tzinfo is not None:
            d = d.replace(tzinfo=None)
        return (now - d).total_seconds() / 86400.0
    except Exception:
        return None


def offer_segment(player_id, cursor=None):
    # Bucket from lifetime IAP spend + lifecycle. 'default' on any missing data
    # or error, so the store always renders a valid offer. Fully crash-safe.
    try:
        own = None
        cur = cursor
        if cur is None:
            import server as _srv
            own = _srv.get_connection()
            cur = own.cursor()
        try:
            cur.execute("SELECT COALESCE(SUM(usd_amount), 0) FROM iap_receipts WHERE player_id = ?", (player_id,))
            srow = cur.fetchone()
            lifetime = float(srow[0]) if srow and srow[0] is not None else 0.0
            cur.execute("SELECT install_date, last_session_time FROM players WHERE player_id = ?", (player_id,))
            prow = cur.fetchone()
        finally:
            if own is not None:
                own.close()

        if lifetime >= 100.0:
            return "whale"
        if lifetime >= 20.0:
            return "dolphin"
        if lifetime > 0.0:
            return "minnow"

        # Never spent -> lifecycle bucket.
        if prow is None:
            return "default"
        now = _dt.datetime.utcnow()
        days_install = _days_since(prow[0], now)
        if days_install is None:
            return "default"
        if days_install < 3.0:
            return "new"
        days_active = _days_since(prow[1] if len(prow) > 1 else None, now)
        if days_active is None or days_active >= 7.0:
            return "lapsing"
        return "minnow"
    except Exception:
        return "default"
