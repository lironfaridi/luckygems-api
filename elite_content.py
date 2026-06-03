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
#  In-memory mock state (resets on restart). Real impl -> DB tables.
# -----------------------------------------------------------------------------
_lock = threading.Lock()
_chapter_state = {}     # pid -> {"current_chapter_id": str, "progress": int, "completed": set}
_estate_funded = {}     # pid -> set(task_id)
_estate_room_paid = {}  # pid -> set(room_id)  (rooms whose completion reward was paid)
_album_disc = {}        # pid -> set(value)
_events_state = {}      # pid -> {event_id -> {"progress": int, "claimed": set(threshold)}}
_mock_wallet = {}       # pid -> {"gold": int, "gems": int}


def _wallet(pid):
    return _mock_wallet.setdefault(pid, {"gold": DEFAULT_GOLD, "gems": DEFAULT_GEMS})


def wallet_snapshot(pid):
    w = _wallet(pid)
    return {"gold": w["gold"], "gems": w["gems"]}


# ---------------------------------------------------------------------------
#  /meta/catalog
# ---------------------------------------------------------------------------
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
    with _lock:
        first = _chapters()[0]["id"] if _chapters() else ""
        st = _chapter_state.setdefault(pid, {"current_chapter_id": first, "progress": 0, "completed": set()})
        return {"current_chapter_id": st["current_chapter_id"], "progress": st["progress"]}


def complete_chapter(pid, chapter_id):
    with _lock:
        chapters = _chapters()
        first = chapters[0]["id"] if chapters else ""
        st = _chapter_state.setdefault(pid, {"current_chapter_id": first, "progress": 0, "completed": set()})
        ch = next((c for c in chapters if c["id"] == chapter_id), None)
        if ch is None or chapter_id in st["completed"]:
            return {"status": "noop"}
        st["completed"].add(chapter_id)
        reward = ch.get("reward", {})
        _wallet(pid)["gems"] += int(reward.get("gems", 0))
        nxt = ""
        for c in chapters:
            if c["id"] not in st["completed"]:
                nxt = c["id"]
                break
        st["current_chapter_id"] = nxt or chapter_id
        st["progress"] = 0
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
    funded = _estate_funded.setdefault(pid, set())
    paid = _estate_room_paid.setdefault(pid, set())
    rid = room["id"]
    if rid not in paid and all(t["id"] in funded for t in room["tasks"]):
        paid.add(rid)
        _wallet(pid)["gems"] += int(room.get("reward", {}).get("gems", 0))


def estate_state(pid):
    with _lock:
        funded = _estate_funded.setdefault(pid, set())
        cur = ""
        for r in CATALOGS["estate_rooms"]:
            if not all(t["id"] in funded for t in r["tasks"]):
                cur = r["id"]
                break
        if not cur and CATALOGS["estate_rooms"]:
            cur = CATALOGS["estate_rooms"][-1]["id"]
        return {"current_room_id": cur, "funded_tasks": sorted(funded)}


def fund_task(pid, task_id, _client_cost):
    with _lock:
        cost = _task_cost(task_id)   # AUTHORITATIVE -- ignore the client's cost
        if cost is None:
            return {"status": "error", "message": "Unknown task"}
        funded = _estate_funded.setdefault(pid, set())
        w = _wallet(pid)
        if task_id not in funded:
            if w["gold"] < cost:
                return {"status": "error", "message": "Insufficient funds"}
            w["gold"] -= cost
            funded.add(task_id)
            room = _room_of_task(task_id)
            if room:
                _maybe_pay_room(pid, room)
        return {"status": "success", "task_id": task_id, "funded_tasks": sorted(funded), "new_balance": w["gold"]}


def finish_room(pid, room_id):
    with _lock:
        room = next((r for r in CATALOGS["estate_rooms"] if r["id"] == room_id), None)
        if room is None:
            return {"status": "error", "message": "Unknown room"}
        funded = _estate_funded.setdefault(pid, set())
        remaining = [t for t in room["tasks"] if t["id"] not in funded]
        gem_price = len(remaining) * 25   # AUTHORITATIVE price (matches client estimate)
        w = _wallet(pid)
        if gem_price > 0:
            if w["gems"] < gem_price:
                return {"status": "error", "message": "Insufficient gems"}
            w["gems"] -= gem_price
            for t in remaining:
                funded.add(t["id"])
            _maybe_pay_room(pid, room)
        return {"status": "success", "funded_tasks": sorted(funded), "new_balance": w["gold"]}


# ---------------------------------------------------------------------------
#  Collection Album
# ---------------------------------------------------------------------------
def album_state(pid):
    with _lock:
        return {"discovered": sorted(_album_disc.setdefault(pid, set()))}


def claim_discovery(pid, value):
    with _lock:
        card = next((c for c in CATALOGS["album"] if int(c["value"]) == int(value)), None)
        if card is None:
            return {"status": "error", "message": "Unknown value"}
        disc = _album_disc.setdefault(pid, set())
        if value in disc:
            return {"status": "success", "value": value, "reward": {}}   # idempotent
        disc.add(value)
        reward = card.get("reward", {})
        _wallet(pid)["gems"] += int(reward.get("gems", 0))
        return {"status": "success", "value": value, "reward": reward}


# ---------------------------------------------------------------------------
#  Live-Ops Event Calendar
# ---------------------------------------------------------------------------
def _player_events(pid):
    st = _events_state.setdefault(pid, {})
    for ev in CATALOGS["events"]:
        st.setdefault(ev["id"], {"progress": int(ev.get("progress", 0)), "claimed": set()})
    return st


def active_events(pid):
    with _lock:
        st = _player_events(pid)
        now = int(time.time())
        out = []
        for ev in CATALOGS["events"]:
            ends = int(ev.get("ends_at", 0))
            if ends != 0 and ends <= now:
                continue
            pst = st[ev["id"]]
            milestones = []
            for m in ev["milestones"]:
                milestones.append({
                    "threshold": m["threshold"],
                    "reward": m["reward"],
                    "claimed": m["threshold"] in pst["claimed"],
                })
            entry = {k: v for k, v in ev.items() if k != "milestones"}
            entry["progress"] = pst["progress"]
            entry["milestones"] = milestones
            out.append(entry)
        return {"events": out}


def claim_milestone(pid, event_id, threshold):
    with _lock:
        ev = next((e for e in CATALOGS["events"] if e["id"] == event_id), None)
        if ev is None:
            return {"status": "error", "message": "Unknown event"}
        st = _player_events(pid)
        pst = st[event_id]
        m = next((mm for mm in ev["milestones"] if int(mm["threshold"]) == int(threshold)), None)
        if m is None:
            return {"status": "error", "message": "Unknown milestone"}
        if threshold in pst["claimed"]:
            return {"status": "noop"}
        # Mock accepts the claim. Real server: require pst["progress"] >= threshold.
        pst["claimed"].add(threshold)
        reward = m.get("reward", {})
        if isinstance(reward, dict) and int(reward.get("gems", 0)) > 0:
            _wallet(pid)["gems"] += int(reward["gems"])
        return {"status": "success", "event_id": event_id, "threshold": threshold, "reward": reward}


# ---------------------------------------------------------------------------
#  Monetization -- offer segment
# ---------------------------------------------------------------------------
def offer_segment(_pid):
    # MOCK: real server computes from spend history / lifecycle. Default is valid.
    return "default"
