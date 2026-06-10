from fastapi import FastAPI, Body, Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn
import sqlite3
import asyncio
import random
import datetime
import json
import re
import time
import hashlib
import math
import hmac
import os
import functools
import logging
import traceback
from typing import List, Dict, Any

# Requires: pip install PyJWT
import jwt

# Optional: push_service.py provides FCM/APNs dispatch.
# Gracefully degrades to no-op when the module is absent.
try:
    from push_service import send_push_to_player as _push_player
    from push_service import push_status as _push_status
except ImportError:
    def _push_player(*_a, **_k): return {"tokens": 0, "sent": 0, "failed": 0}
    def _push_status(): return {"fcm": False, "apns": False}
    print("[push] push_service not available -- push disabled")

# Optional: receipt_validator.py validates Apple/Google store receipts.
# IAP_SANDBOX=1 or BYPASS_RECEIPT_VALIDATION=true skips all store API calls.
_receipt_validator_available: bool = False
try:
    from receipt_validator import (
        validate_receipt      as _validate_receipt,
        verify_apple_receipt  as _verify_apple_receipt,
        verify_google_receipt as _verify_google_receipt,
    )
    _receipt_validator_available = True
    print("[iap] receipt_validator loaded")
except ImportError:
    async def _validate_receipt(platform, item_id, receipt, product_id):
        return False  # safe fallback: reject all receipts when validator is missing
    async def _verify_apple_receipt(receipt_data, is_sandbox):
        return {"valid": False, "error": "receipt_validator not installed"}
    async def _verify_google_receipt(package_name, product_id, purchase_token, credentials_json):
        return {"valid": False, "error": "receipt_validator not installed"}
    print("[iap] receipt_validator not found -- all IAP purchases will be rejected")

# Optional Sentry integration. Set SENTRY_DSN to enable.
# Requires: pip install sentry-sdk[fastapi]
_SENTRY_DSN = os.environ.get("SENTRY_DSN", "").strip()
if _SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        sentry_sdk.init(
            dsn=_SENTRY_DSN,
            integrations=[StarletteIntegration(), FastApiIntegration()],
            traces_sample_rate=0.1,
            environment=os.environ.get("APP_ENV", "development"),
        )
        print(f"[observability] Sentry initialized (env={os.environ.get('APP_ENV', 'development')})")
    except ImportError:
        print("[observability] sentry-sdk not installed -- Sentry disabled")
    except Exception as _sentry_err:
        print(f"[observability] Sentry init failed: {_sentry_err}")

app = FastAPI()


class V1StripMiddleware(BaseHTTPMiddleware):
    """Transparently accepts /v1/<path> alongside /<path> for forward compatibility."""
    async def dispatch(self, request: Request, call_next):
        path = request.scope.get("path", "")
        if path.startswith("/v1/"):
            request.scope["path"]     = path[3:]          # "/v1/bank/balance" -> "/bank/balance"
            request.scope["raw_path"] = path[3:].encode()
        client_ver = request.headers.get("X-Client-Version", "")
        if client_ver and os.environ.get("APP_ENV", "development").lower() != "production":
            print(f"[api] client_version={client_ver} path={request.scope['path']}")
        _excluded_paths = {"/auth/register", "/health"}
        if (client_ver and MIN_CLIENT_VERSION != "0.0.0"
                and request.scope.get("path", "") not in _excluded_paths):
            if _parse_version(client_ver) < _parse_version(MIN_CLIENT_VERSION):
                from starlette.responses import JSONResponse
                return JSONResponse(
                    status_code=426,
                    content={
                        "error":       "update_required",
                        "min_version": MIN_CLIENT_VERSION,
                        "message":     "Please update the game to continue playing.",
                    }
                )
        return await call_next(request)


app.add_middleware(V1StripMiddleware)

_MAX_REQUEST_BODY = 65_536  # 64 KB

class _MaxBodySizeMiddleware(BaseHTTPMiddleware):
    """Reject any request body larger than _MAX_REQUEST_BODY bytes with HTTP 413."""
    async def dispatch(self, request: Request, call_next):
        cl_raw = request.headers.get("content-length", "")
        if cl_raw:
            try:
                if int(cl_raw) > _MAX_REQUEST_BODY:
                    from starlette.responses import JSONResponse
                    return JSONResponse(
                        status_code=413,
                        content={"error": "request_too_large",
                                 "max_bytes": _MAX_REQUEST_BODY}
                    )
            except ValueError:
                pass
        body = await request.body()
        if len(body) > _MAX_REQUEST_BODY:
            from starlette.responses import JSONResponse
            return JSONResponse(
                status_code=413,
                content={"error": "request_too_large",
                         "max_bytes": _MAX_REQUEST_BODY}
            )
        return await call_next(request)

app.add_middleware(_MaxBodySizeMiddleware)

# ---------------------------------------------------------------------------
# Database configuration
# Set DATABASE_URL for PostgreSQL (e.g. "postgresql://user:pass@host/db").
# Leave unset (or set to empty string) to use the local SQLite dev setup.
# Set REDIS_URL for caching   (e.g. "redis://localhost:6379/0").
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL and os.environ.get("APP_ENV", "development").lower() == "production":
    raise RuntimeError(
        "FATAL: DATABASE_URL environment variable is not set. "
        "Set it to your PostgreSQL connection string before starting the server in production."
    )
REDIS_URL:    str = os.environ.get("REDIS_URL",    "").strip()
DB_NAME:      str = os.environ.get("SQLITE_DB",    "economy.db")  # SQLite path (dev only)

# IAP receipt validation config -- read by both receipt_validator.py and the
# /iap/verify endpoint below.  IAP_SANDBOX=1 bypasses all store API calls.
_IAP_SANDBOX_MODE: bool = (
    os.getenv("IAP_SANDBOX", "0").strip() == "1"
    or os.getenv("BYPASS_RECEIPT_VALIDATION", "").lower() == "true"
)
_GOOGLE_CREDS_STR: str = os.getenv("GOOGLE_PLAY_CREDENTIALS_JSON", "").strip()
_GOOGLE_PLAY_PACKAGE: str = os.getenv("GOOGLE_PLAY_PACKAGE",
                                       "com.faridistudio.luckygems").strip()

_USE_POSTGRES: bool = DATABASE_URL.startswith(("postgresql", "postgres"))
_APP_ENV_BOOT: str = os.environ.get("APP_ENV", "development").strip().lower()

# -- PostgreSQL connection pool (psycopg2) -----------------------------------
# Render cold-starts and Supabase's connection pooler can both refuse the very
# first connection attempt for a few seconds after boot. A bare try/except here
# would permanently downgrade this worker to ephemeral local SQLite for its
# entire lifetime -- on Render that file is wiped on every restart/redeploy,
# which is exactly the "Groundhog Day" progress-loss symptom. Retry with
# backoff before giving up, and in production never fall back silently: a
# silently-degraded worker writing to a doomed local file is worse than a
# worker that fails to boot.
_PG_POOL = None
if _USE_POSTGRES:
    try:
        import psycopg2
        from psycopg2 import pool as _pg_pool_mod
    except ImportError:
        print("[db] WARN: psycopg2 not installed — falling back to SQLite")
        _USE_POSTGRES = False
        _pg_pool_mod = None

    if _USE_POSTGRES:
        _PG_CONNECT_ATTEMPTS = 5
        _PG_CONNECT_BACKOFF_SECS = 2.0
        _pg_last_err = None
        for _pg_attempt in range(1, _PG_CONNECT_ATTEMPTS + 1):
            try:
                _PG_POOL = _pg_pool_mod.ThreadedConnectionPool(10, 20, DATABASE_URL)
                print(f"[db] PostgreSQL pool ready ({DATABASE_URL[:48]}...) "
                      f"on attempt {_pg_attempt}/{_PG_CONNECT_ATTEMPTS}")
                break
            except Exception as _pg_err:
                _pg_last_err = _pg_err
                print(f"[db] WARN: PostgreSQL connection attempt {_pg_attempt}/"
                      f"{_PG_CONNECT_ATTEMPTS} failed ({_pg_err})")
                if _pg_attempt < _PG_CONNECT_ATTEMPTS:
                    time.sleep(_PG_CONNECT_BACKOFF_SECS)

        if _PG_POOL is None:
            if _APP_ENV_BOOT == "production":
                raise RuntimeError(
                    "FATAL: could not establish a PostgreSQL connection pool after "
                    f"{_PG_CONNECT_ATTEMPTS} attempts ({_pg_last_err}). Refusing to "
                    "fall back to ephemeral SQLite in production -- that would "
                    "silently wipe player progress on every restart."
                )
            print(f"[db] WARN: PostgreSQL unreachable ({_pg_last_err}) — "
                  "falling back to SQLite (development only)")
            _USE_POSTGRES = False


class _PgCursor:
    """Translates SQLite-dialect SQL to PostgreSQL using deterministic string
    replacement -- no regex.

    Handles three patterns that appear in our DDL and DML:
      ?  ->  %s
          Parameter placeholder swap. Safe because ? never appears inside
          string literals in our queries -- only as a param marker.
      INTEGER PRIMARY KEY AUTOINCREMENT  ->  BIGSERIAL PRIMARY KEY
          DDL only (CREATE TABLE in init_db). Never appears in DML.
      DATETIME  ->  TIMESTAMP
          DDL only (CREATE TABLE column types). Never appears in DML.

    INSERT OR IGNORE / INSERT OR REPLACE are intentionally NOT handled here.
    Every call site that uses those constructs must use a _SQL_*_SQ / _SQL_*_PG
    dual-dialect constant so the correct SQL is selected at import time, with
    no runtime translation needed.
    """

    def __init__(self, raw_cursor):
        self._cur = raw_cursor

    def _translate(self, sql: str) -> str:
        sql = sql.replace("?",                                 "%s")
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        sql = sql.replace("DATETIME",                          "TIMESTAMP")
        return sql

    def execute(self, sql: str, params=()):
        self._cur.execute(self._translate(sql), params if params else None)
        return self

    def fetchone(self):  return self._cur.fetchone()
    def fetchall(self):  return self._cur.fetchall()


class _PgConn:
    """Pool-backed PostgreSQL connection mimicking the sqlite3.Connection API."""
    def __init__(self, raw_conn):
        self._raw = raw_conn

    def cursor(self)            -> _PgCursor: return _PgCursor(self._raw.cursor())
    def commit(self)            -> None:       self._raw.commit()
    def execute(self, *_a, **_k) -> None:     pass   # swallows PRAGMA calls silently
    def close(self)             -> None:
        if _PG_POOL:
            _PG_POOL.putconn(self._raw)


# -- Redis cache client -------------------------------------------------------
_REDIS = None
if REDIS_URL:
    try:
        import redis as _redis_mod
        _REDIS = _redis_mod.from_url(
            REDIS_URL,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=True,
        )
        _REDIS.ping()
        print(f"[cache] Redis connected ({REDIS_URL[:48]}...)")
    except ImportError:
        print("[cache] WARN: redis-py not installed — caching disabled")
    except Exception as _redis_err:
        print(f"[cache] WARN: Redis unreachable ({_redis_err}) — caching disabled")
        _REDIS = None


def _invalidate_balance_cache(player_id: str) -> None:
    """Drop the Redis balance cache entry for a player after any financial write."""
    if _REDIS is not None:
        try:
            _REDIS.delete(f"balance:{player_id}")
        except Exception:
            pass  # Redis being unavailable must never block a write path

# Special tile values — kept in sync with node_2d.gd constants
GOLDEN_TILE_VALUE  = 99
WILDCARD_VALUE     = 98
CATALYST_VALUE     = 97
# Special values excluded from market pricing; counted as occupied cells for fill-rate math.
_SPECIAL_TILE_VALUES = {GOLDEN_TILE_VALUE, WILDCARD_VALUE, CATALYST_VALUE, -1}

MARKET_CONFIG = {
    1: {"min": -20.0, "max": 15.0,   "base":    5.0, "threshold": 6, "penalty":   1.5},
    2: {"min": -10.0, "max": 40.0,   "base":   12.0, "threshold": 4, "penalty":   3.0},
    3: {"min":   0.0, "max": 100.0,  "base":   30.0, "threshold": 3, "penalty":   6.0},
    4: {"min":  15.0, "max": 300.0,  "base":   85.0, "threshold": 2, "penalty":  12.0},
    5: {"min":  50.0, "max": 700.0,  "base":  200.0, "threshold": 2, "penalty":  25.0},
    6: {"min": 120.0, "max": 1600.0, "base":  450.0, "threshold": 2, "penalty":  55.0},
    7: {"min": 300.0, "max": 4000.0, "base": 1500.0, "threshold": 1, "penalty": 120.0},
}

# Board stages: ordered list of dicts with rows, cols, cost, and cashout_tier.
# Stage 0 is the 2x2 tutorial board (always free). Clients receive explicit rows/cols.
# cashout_tier: the gem tier whose line triggers a cashout at this board size.
# Costs are the single source of truth -- the client reads them from /shop/state.
BOARD_STAGES = [
    {"rows": 2, "cols": 2, "cost":           0, "cashout_tier": 2},  # Stage 0 – tutorial  (The Hook)
    {"rows": 2, "cols": 3, "cost":         100, "cashout_tier": 2},  # Stage 1             (The Hook)
    {"rows": 3, "cols": 3, "cost":         500, "cashout_tier": 3},  # Stage 2             (The Hook)
    {"rows": 4, "cols": 3, "cost":       4_000, "cashout_tier": 3},  # Stage 3 -- The Wall
    {"rows": 4, "cols": 4, "cost":      15_000, "cashout_tier": 4},  # Stage 4
    {"rows": 5, "cols": 4, "cost":      45_000, "cashout_tier": 4},  # Stage 5
    {"rows": 5, "cols": 5, "cost":     120_000, "cashout_tier": 5},  # Stage 6
    {"rows": 6, "cols": 5, "cost":     350_000, "cashout_tier": 5},  # Stage 7
    {"rows": 6, "cols": 6, "cost":   1_000_000, "cashout_tier": 6},  # Stage 8
]

def get_stage(index: int) -> dict:
    index = max(0, min(index, len(BOARD_STAGES) - 1))
    return BOARD_STAGES[index]

# Tier unlock costs for all purchasable tiers (1 and 2 are free via BASE_TIERS).
# Single source of truth -- client reads these from /shop/state.
TIER_UNLOCK_COSTS = {
    3:           300,  # The Hook
    4:         7_000,  # The Wall
    5:        15_000,
    6:        35_000,
    7:       100_000,
    8:       250_000,
    9:       600_000,
    10:    1_500_000,
    11:    4_000_000,
}

# Mirrors GameTheme.TILE_NAMES in GDScript -- kept in sync manually.
TILE_NAMES = {
    1:  "Rough Emerald",
    2:  "Diamond",
    3:  "Ruby Diamond",
    4:  "Gold Ring",
    5:  "Royal Pendant",
    6:  "Diamond Crown",
    7:  "Diamond Flower",
    8:  "Prismatic Crystal",
    9:  "Astral Core",
    10: "Divine Crown",
    11: "Infinity Heart",
}

# Tiers that are unlocked from the very start (no shop purchase required).
BASE_TIERS = {1, 2}

# Piggy Bank hard cap: base + (econ_piggy_cap mastery level × per-level bonus).
# The cap prevents runaway passive accumulation without punishing light play.
PIGGY_BASE_CAP          = 20_000
PIGGY_CAP_PER_MASTERY   =  5_000


# ---------------------------------------------------------------------------
# Rate limiter (in-memory, per-player, per-endpoint)
# Resets on server restart -- acceptable for Sprint 2 solo backend.
# ---------------------------------------------------------------------------
# Rate limiters: Redis-backed with in-process dict fallback.
# Redis variants are safe under multi-worker uvicorn (state is shared across
# processes).  The in-process fallback activates when Redis is unavailable;
# it protects single-worker deployments and dev environments.
# ---------------------------------------------------------------------------
_RATE_LIMIT_STORE: Dict[str, float] = {}

def _check_rate_limit(player_id: str, endpoint: str, min_interval_secs: float) -> bool:
    """Return True (allow) if enough time has elapsed since the last call; False (block) otherwise."""
    key = f"rl:{player_id}:{endpoint}"
    int_ms = max(1, int(min_interval_secs * 1000))
    if _REDIS is not None:
        try:
            # SET NX EX: set key only if not exists; auto-expires after the interval.
            # If the key already exists the player is within the cooldown window.
            allowed = _REDIS.set(key, "1", px=int_ms, nx=True)
            return bool(allowed)
        except Exception:
            pass  # Redis fault: fall through to in-process dict
    # In-process fallback (not safe across workers, acceptable for dev/single-worker).
    now = time.monotonic()
    if now - _RATE_LIMIT_STORE.get(key, 0.0) < min_interval_secs:
        return False
    _RATE_LIMIT_STORE[key] = now
    return True


# ---------------------------------------------------------------------------
# Sliding-window burst limiter (count-based, per-player, per-endpoint)
# ---------------------------------------------------------------------------
_BURST_RATE_STORE: Dict[str, list] = {}

def _check_burst_limit(player_id: str, endpoint: str,
                       max_calls: int, window_secs: float) -> bool:
    """Return True (allow) if fewer than max_calls were made in the last window_secs."""
    key = f"bl:{player_id}:{endpoint}"
    if _REDIS is not None:
        try:
            now_ms      = int(time.time() * 1000)
            window_ms   = int(window_secs * 1000)
            cutoff_ms   = now_ms - window_ms
            pipe        = _REDIS.pipeline()
            # Remove timestamps older than the rolling window.
            pipe.zremrangebyscore(key, "-inf", cutoff_ms)
            # Count remaining calls in the window.
            pipe.zcard(key)
            # Add this call's timestamp (member = timestamp so duplicates are unique).
            pipe.zadd(key, {str(now_ms): now_ms})
            # Auto-expire the sorted set so Redis memory is not leaked.
            pipe.expire(key, int(window_secs) + 10)
            results = pipe.execute()
            count_after_prune = results[1]
            if count_after_prune >= max_calls:
                # Already at limit before this call was recorded; undo the zadd.
                _REDIS.zrem(key, str(now_ms))
                return False
            return True
        except Exception:
            pass  # Redis fault: fall through to in-process dict
    # In-process fallback.
    now = time.monotonic()
    timestamps = _BURST_RATE_STORE.get(key, [])
    timestamps = [t for t in timestamps if now - t < window_secs]
    if len(timestamps) >= max_calls:
        _BURST_RATE_STORE[key] = timestamps
        return False
    timestamps.append(now)
    _BURST_RATE_STORE[key] = timestamps
    return True


# ---------------------------------------------------------------------------
# Anti-cheat: per-run sanity caps
# Values chosen at ~3x the realistic theoretical maximum so legitimate
# marathon sessions are never rejected, but clearly fabricated packets are.
# ---------------------------------------------------------------------------
_MAX_CASH_PER_RUN       = 5_000_000   # 5 M gold
_MAX_SURVIVAL_SECS      = 3_600       # 60-minute run ceiling
_MAX_CASHOUTS_PER_RUN   = 300
_MAX_MERGES_PER_RUN     = 3_000
_MAX_COMBO_VALUE        = 10          # realistically peaks at 3-4x
_MAX_CURSED_REMOVED     = 500
_MAX_COMBO_COUNT_RUN    = 500

# Per-CALL deposit ceiling (interim anti-injection guard).
# A single legitimate /bank/deposit is one of:
#   * one cashout line (node_2d -> deposit_money(total_reward)), or
#   * the Rush end-of-run 3x bonus (run_cashout_money * 2, a short ~60-130 s run).
# Both are far below 1 M; the FULL run caps at _MAX_CASH_PER_RUN (5 M) across up to
# _MAX_CASHOUTS_PER_RUN (300) separate deposits.  So 1 M per call never rejects a
# legitimate packet, but blocks a hacked client from minting millions in one shot.
# NOTE: this is interim.  The real fix is server-derived payouts (client sends the
# board event; server computes the reward).  A cumulative per-run deposit cap keyed
# to board stage is the recommended next step -- see audit.
_MAX_DEPOSIT_PER_CALL   = 1_000_000

# Server-derived cashout payouts (the authoritative gold source).
GOLDEN_TILE_CASHOUT_MULT = 4          # mirrors node_2d.golden_tile_cashout_multiplier
_MAX_CASHOUT_PAYOUT      = 1_000_000  # per-cashout ceiling (anti-cheat clamp)
_MAX_CASHOUT_LINES       = 16         # sane upper bound on simultaneous lines
_MAX_CASHOUT_LINE_LEN    = 8          # max board edge


# ---------------------------------------------------------------------------
# Idle Piggy Bank constants
# ---------------------------------------------------------------------------
IDLE_PIGGY_GOLD_PER_HOUR = 100        # gold earned per full offline hour
IDLE_PIGGY_MAX_HOURS     = 72         # hard ceiling: 3 days of offline earnings

# ---------------------------------------------------------------------------
# Free-player gem faucet budget  (rebalanced 2026-05-27)
#
# Design target: one meaningful premium unlock per 2-3 weeks of active play.
# IAP should feel like a satisfying time-saver, not a necessity or redundant.
#
# Free gem income breakdown (active player, 7-day login streak):
#   Daily rewards (Days 2/5/7)     :  3 + 5 + 15 = 23 gems / week
#   First-cashout daily bonus      :  5 gems / active day  (capped below)
#   Daily quests (max tier)        :  0-5 gems / day       (capped below)
#   Combined daily cap             : 15 gems / calendar day max
#   Realistic weekly total         : ~50-70 gems (active), ~23 (login-only)
#
# Gem sink costs (NOT changed -- sinks are balanced, faucets were the leak):
#   Wildcard core   :   20 gems  (~0.5-1 week  active)
#   Vault slot      :   50 gems  (~1-2 weeks   active)
#   Golden license  :   80 gems  (~1.5-2 weeks active)
#   Golden Lic. 2   :  100 gems  (~2 weeks     active)
#   Catalyst core   :  120 gems  (~2.5 weeks   active)
#   Board insurance :  150 gems  (~3 weeks     active)
#   Cosmetic tier 1 :  250 gems  (~5 weeks     active)
#   Cosmetic tier 2 :  600 gems  (~12 weeks    active)
#
# Lifetime achievement ceiling dropped from 1,210 to 240 gems (all tiers
# individually capped at 20) so new players cannot skip the economy via
# first-session achievement farming.
# ---------------------------------------------------------------------------
DAILY_FREE_GEM_CAP       = 15         # max free gems earnable per calendar day (IAP value protection)

# Vault Pass tier thresholds (cumulative shards required to reach each tier).
# Tier 0 = 0 shards (everyone starts here).  Tier 4 = max tier.
VAULT_PASS_TIERS = [0, 100, 300, 600, 1000]

# ---------------------------------------------------------------------------
# Ad reward tokens  --  server-issued one-time tokens that prove an ad was
# watched before crediting an economic reward (piggy smash 60%, daily double).
# Tokens expire in 90 s and are consumed on first use.
# Dict grows by at most one entry per active player at any time; negligible
# memory footprint for a solo mobile backend.
# ---------------------------------------------------------------------------
_AD_PENDING_TOKENS: Dict[str, Dict] = {}


# ---------------------------------------------------------------------------
# JWT authentication
# Set JWT_SECRET as an environment variable before deploying.
# The dev fallback is intentionally weak -- do NOT ship it.
# ---------------------------------------------------------------------------
_JWT_SECRET_DEFAULT = "hostile-merge-dev-secret-change-before-ship"
_JWT_SECRET         = os.environ.get("JWT_SECRET", "").strip()
_JWT_ALGORITHM      = "HS256"

# APP_ENV is the single canonical env-mode variable used throughout this file.
_ENV = os.environ.get("APP_ENV", "development").strip().lower()

# Covers both "not set" (empty string) and "still the dev placeholder" cases.
_JWT_INSECURE = (not _JWT_SECRET or _JWT_SECRET == _JWT_SECRET_DEFAULT)

if _JWT_INSECURE:
    if _ENV == "production":
        raise RuntimeError(
            "\n\n"
            "  FATAL: JWT_SECRET is missing or set to the dev fallback in production!\n"
            "  Generate a strong secret and set it as an environment variable:\n"
            "  export JWT_SECRET=$(python -c \"import secrets; print(secrets.token_hex(32))\")\n"
        )
    else:
        _JWT_SECRET = _JWT_SECRET_DEFAULT
        print(
            "\n"
            "  [security] WARNING: Running with the default dev JWT secret.\n"
            "  This is acceptable for local development ONLY.\n"
            "  Set JWT_SECRET before deploying to any shared or production environment.\n"
        )

_JWT_USING_DEFAULT = (_JWT_SECRET == _JWT_SECRET_DEFAULT)

# ---------------------------------------------------------------------------
# Minimum client version enforcement
# Set MIN_CLIENT_VERSION=1.2.0 to force clients < 1.2.0 to update.
# Set to "0.0.0" (default) to disable enforcement.
# ---------------------------------------------------------------------------
MIN_CLIENT_VERSION: str = os.environ.get("MIN_CLIENT_VERSION", "0.0.0")


def _parse_version(v: str) -> tuple:
    """Parse "1.2.3" into (1, 2, 3) for comparison. Returns (0,0,0) on error."""
    try:
        parts = str(v).strip().split(".")
        return tuple(int(x) for x in parts[:3])
    except (ValueError, AttributeError):
        return (0, 0, 0)


# ---------------------------------------------------------------------------
# Soft-launch geo-gate
# Populate e.g. SOFT_LAUNCH_COUNTRIES=CA,AU,PH to restrict new registrations.
# Leave empty (default) to allow global access.
# ---------------------------------------------------------------------------
_SOFT_LAUNCH_COUNTRIES: set = set(
    c.strip().upper()
    for c in os.environ.get("SOFT_LAUNCH_COUNTRIES", "").split(",")
    if c.strip()
)


# Manual IP-to-country cache (replaces @functools.lru_cache, which cannot wrap async
# functions).  Unbounded growth is acceptable: the unique-IP count for a mobile game
# is small, and each entry is a 2-byte country code string.
_IP_COUNTRY_CACHE: Dict[str, str] = {}


def _lookup_country_blocking(ip: str) -> str:
    """Synchronous urllib lookup — always called via asyncio.to_thread, never directly."""
    try:
        import urllib.request
        with urllib.request.urlopen(
            f"http://ip-api.com/json/{ip}?fields=countryCode",
            timeout=2
        ) as resp:
            data = json.loads(resp.read())
            return data.get("countryCode", "XX")
    except Exception:
        return "XX"


async def _get_country_from_ip(ip: str) -> str:
    """Returns ISO 3166-1 alpha-2 country code, or 'XX' on failure.

    Cache-first: hits the in-process dict before making any network call.
    On a cache miss the blocking urllib call is dispatched to a thread-pool
    worker via asyncio.to_thread so the event loop is never stalled.
    """
    if ip in ("127.0.0.1", "::1", "testclient"):
        return "XX"   # local dev / test runner
    cached = _IP_COUNTRY_CACHE.get(ip)
    if cached is not None:
        return cached
    country = await asyncio.to_thread(_lookup_country_blocking, ip)
    _IP_COUNTRY_CACHE[ip] = country
    return country


def _issue_ad_token(player_id: str, context: str) -> str:
    """Generate a 40-hex one-time token scoped to player_id + context.

    Persists the token in Redis (TTL 90 s) when available so the token
    survives server restarts and is visible to all uvicorn workers.
    Falls back to the in-process _AD_PENDING_TOKENS dict when Redis is down.
    """
    raw   = f"{player_id}:{context}:{time.monotonic()}:{random.getrandbits(64)}"
    token = hashlib.sha256(raw.encode()).hexdigest()[:40]
    entry = {
        "context":    context,
        "token":      token,
        "expires_at": time.monotonic() + 90.0,
    }
    _redis_ok = False
    if _REDIS is not None:
        try:
            _REDIS.setex(
                f"ad_token:{player_id}",
                90,
                json.dumps({"context": context, "token": token}),
            )
            _redis_ok = True
        except Exception:
            pass  # Redis fault: fall through to in-process dict
    if not _redis_ok:
        _AD_PENDING_TOKENS[player_id] = entry
    return token


def _consume_ad_token(player_id: str, expected_context: str, provided_token: str) -> bool:
    """Validate and consume the token.  Returns True only on an exact, unexpired match.

    Reads from Redis first.  If the key is absent in Redis (miss, expired, or Redis
    is down) falls back to the in-process _AD_PENDING_TOKENS dict so tokens issued
    before a Redis outage are still honoured.
    """
    # --- Redis path ---
    if _REDIS is not None:
        try:
            raw = _REDIS.get(f"ad_token:{player_id}")
            if raw is not None:
                stored = json.loads(raw)
                if stored.get("context") != expected_context:
                    return False
                if stored.get("token") != provided_token:
                    return False
                _REDIS.delete(f"ad_token:{player_id}")  # one-time use
                return True
            # Key absent in Redis: fall through to in-process dict (handles
            # tokens that were issued while Redis was unavailable).
        except Exception:
            pass  # Redis fault: fall through to in-process dict

    # --- In-process fallback ---
    entry = _AD_PENDING_TOKENS.get(player_id)
    if not entry:
        return False
    if entry["context"] != expected_context or entry["token"] != provided_token:
        return False
    if time.monotonic() > entry["expires_at"]:
        _AD_PENDING_TOKENS.pop(player_id, None)
        return False
    _AD_PENDING_TOKENS.pop(player_id, None)  # one-time use
    return True


def _safe_parse_mastery(raw) -> dict:
    """Robustly decode mastery_state from DB. Handles None, empty, and double-encoded JSON."""
    if not raw:
        return {}
    try:
        state = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(state, str):   # double-encoded: decode again
            state = json.loads(state)
        return state if isinstance(state, dict) else {}
    except Exception:
        return {}


def get_piggy_cap(mastery_state_json: str) -> int:
    """Return the effective piggy cap for a player given their mastery_state JSON string."""
    state = _safe_parse_mastery(mastery_state_json)
    level = int(state.get("econ_piggy_cap", {}).get("level", 0)) if isinstance(state.get("econ_piggy_cap"), dict) \
        else int(state.get("econ_piggy_cap", 0))
    return PIGGY_BASE_CAP + level * PIGGY_CAP_PER_MASTERY


def get_piggy_gem_cost(piggy_balance: int) -> int:
    """Dynamic gem cost to do a full (100%) piggy smash.
    Scales linearly: full 20k piggy = 100 Gems. Minimum 10 Gems."""
    return max(10, int((piggy_balance / float(PIGGY_BASE_CAP)) * 100))


def _award_free_gems(cursor, player_id: str, amount: int, today_str: str) -> int:
    """Award up to `amount` free gems, respecting the daily DAILY_FREE_GEM_CAP.
    Returns the actual gems credited (0 if the cap is already reached)."""
    cursor.execute(
        "SELECT daily_gems_earned, last_gem_cap_reset FROM players "
        "WHERE player_id = ?", (player_id,)
    )
    row = cursor.fetchone()
    earned_today = int(row[0]) if row and row[0] else 0
    last_reset   = str(row[1]) if row and row[1] else ""
    if last_reset != today_str:
        earned_today = 0   # new calendar day -- reset counter
        cursor.execute(
            "UPDATE players SET daily_gems_earned = 0, "
            "last_gem_cap_reset = ? WHERE player_id = ?",
            (today_str, player_id)
        )
    headroom = max(0, DAILY_FREE_GEM_CAP - earned_today)
    actual   = min(amount, headroom)
    if actual <= 0:
        return 0
    cursor.execute(
        "UPDATE players "
        "SET gems_balance      = COALESCE(gems_balance, 0) + ?, "
        "    daily_gems_earned = COALESCE(daily_gems_earned, 0) + ? "
        "WHERE player_id = ?",
        (actual, actual, player_id)
    )
    return actual

# Quest pool — one is assigned at a time; on completion a new random one is picked.
# "type" maps to the key in the run_stats payload submitted at end of run.
QUEST_POOL = [
    {"id": "q_merge_20",    "title": "Fusion Frenzy",  "desc": "Merge 20 times in one run.",      "type": "run_merges",      "target": 20,  "reward": 300,  "gems_reward": 0},
    {"id": "q_merge_50",    "title": "Crystal Weaver", "desc": "Merge 50 times in one run.",      "type": "run_merges",      "target": 50,  "reward": 800,  "gems_reward": 2},
    {"id": "q_cashout_5",   "title": "Line Cleaner",   "desc": "Complete 5 cashouts in one run.", "type": "cashouts",        "target": 5,   "reward": 500,  "gems_reward": 0},
    {"id": "q_cashout_10",  "title": "Cash Torrent",   "desc": "Complete 10 cashouts in a run.",  "type": "cashouts",        "target": 10,  "reward": 1500, "gems_reward": 3},
    {"id": "q_combo_3",     "title": "Combo Starter",  "desc": "Trigger 3 combos in one run.",    "type": "run_combo_count", "target": 3,   "reward": 400,  "gems_reward": 0},
    {"id": "q_survive_180", "title": "Time Vault",     "desc": "Survive 3 minutes in one run.",   "type": "survival_time",   "target": 180, "reward": 600,  "gems_reward": 0},
    {"id": "q_survive_300", "title": "Iron Lockbox",   "desc": "Survive 5 minutes in one run.",   "type": "survival_time",   "target": 300, "reward": 1200, "gems_reward": 5},
    {"id": "q_earn_5000",   "title": "Fat Stack",      "desc": "Earn 5,000 cash in one run.",     "type": "cash_earned",     "target": 5000,"reward": 700,  "gems_reward": 2},
]
_QUEST_MAP = {q["id"]: q for q in QUEST_POOL}

# Gold reward multiplier per board stage.  Stages 6+ use the max bucket (25x).
QUEST_STAGE_MULTIPLIERS: Dict[int, float] = {
    0: 1.0,
    1: 1.0,
    2: 2.0,
    3: 4.0,
    4: 8.0,
    5: 15.0,
}
_QUEST_STAGE_MAX_MULT: float = 25.0

def _get_quest_multiplier(board_stage: int) -> float:
    return QUEST_STAGE_MULTIPLIERS.get(board_stage, _QUEST_STAGE_MAX_MULT)

# Day 1-7 login rewards.  free_spin=True resets last_free_spin_time so the
# /wheel/spin free-daily gate opens immediately.
VAULT_BOOSTS = {
    "boost_cashout_2x": {
        "name":         "2x Cashout Multiplier",
        "description":  "All cashouts pay double for 30 minutes.",
        "cost_gold":    8_000,
        "duration_min": 30,
    },
    "boost_board_shield": {
        "name":         "Board Shield",
        "description":  "Blocks the next cursed tile spawn.",
        "cost_gold":    5_000,
        "duration_min": 60,
    },
    "boost_lucky_drop": {
        "name":         "Lucky Drop",
        "description":  "Next 10 tile spawns are guaranteed max tier.",
        "cost_gold":    12_000,
        "duration_min": 45,
    },
}

DAILY_REWARDS = {
    # Values mirror VAULT_REWARDS in main_menu.gd -- both must stay in sync.
    1: {"gold":   500, "gems":  0, "free_spin": False,
        "label": "+$500 Gold"},
    2: {"gold":     0, "gems": 10, "free_spin": False,
        "label": "+10 Gems"},
    3: {"gold": 2_500, "gems":  0, "free_spin": False,
        "label": "+$2,500 Gold"},
    4: {"gold": 1_000, "gems":  0, "free_spin": True,
        "label": "Free Spin + $1,000"},
    5: {"gold":     0, "gems": 25, "free_spin": False,
        "label": "+25 Gems"},
    6: {"gold": 5_000, "gems":  0, "free_spin": False,
        "label": "+$5,000 Gold"},
    # Day 7: GRAND PRIZE -- Week-1 gem total = 35, gold total = 14,000.
    7: {"gold": 5_000, "gems": 50, "free_spin": False,
        "label": "GRAND PRIZE: +$5,000 + 50 Gems"},
}


def get_connection():
    """Return a DB connection. PostgreSQL via pool when DATABASE_URL is set; SQLite otherwise."""
    if _USE_POSTGRES and _PG_POOL:
        return _PgConn(_PG_POOL.getconn())
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def column_exists(cursor, table_name: str, column_name: str) -> bool:
    """Check if a column exists. Uses information_schema for PG, PRAGMA for SQLite."""
    if _USE_POSTGRES:
        # Bypass _PgCursor translation — this is raw PostgreSQL-dialect SQL.
        raw = cursor._cur if isinstance(cursor, _PgCursor) else cursor
        raw.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = %s",
            (table_name, column_name),
        )
        return raw.fetchone() is not None
    cursor.execute(f"PRAGMA table_info({table_name})")
    return column_name in [row[1] for row in cursor.fetchall()]


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller TEXT,
            item_level INTEGER,
            profit_made REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_prices (
            item_level INTEGER PRIMARY KEY,
            current_price REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            total_money INTEGER DEFAULT 0
        )
    """)

    if not column_exists(cursor, "players", "max_unlocked_tier"):
        cursor.execute("ALTER TABLE players ADD COLUMN max_unlocked_tier INTEGER DEFAULT 4")

    if not column_exists(cursor, "players", "board_size"):
        cursor.execute("ALTER TABLE players ADD COLUMN board_size INTEGER DEFAULT 4")

    if not column_exists(cursor, "players", "tutorial_completed"):
        cursor.execute("ALTER TABLE players ADD COLUMN tutorial_completed INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "lifetime_cash_earned"):
        cursor.execute("ALTER TABLE players ADD COLUMN lifetime_cash_earned INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "best_survival_time"):
        cursor.execute("ALTER TABLE players ADD COLUMN best_survival_time INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "best_cashouts_run"):
        cursor.execute("ALTER TABLE players ADD COLUMN best_cashouts_run INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "best_combo"):
        cursor.execute("ALTER TABLE players ADD COLUMN best_combo INTEGER DEFAULT 1")

    if not column_exists(cursor, "players", "cursed_tiles_removed"):
        cursor.execute("ALTER TABLE players ADD COLUMN cursed_tiles_removed INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "total_runs"):
        cursor.execute("ALTER TABLE players ADD COLUMN total_runs INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "total_merges"):
        cursor.execute("ALTER TABLE players ADD COLUMN total_merges INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "board_stage"):
        cursor.execute("ALTER TABLE players ADD COLUMN board_stage INTEGER DEFAULT 4")
        cursor.execute("""
            UPDATE players SET board_stage = CASE
                WHEN board_size >= 6 THEN 8
                WHEN board_size >= 5 THEN 6
                ELSE 4
            END
        """)

    if not column_exists(cursor, "players", "gems_balance"):
        cursor.execute("ALTER TABLE players ADD COLUMN gems_balance INTEGER DEFAULT 10")

    if not column_exists(cursor, "players", "piggy_balance"):
        cursor.execute("ALTER TABLE players ADD COLUMN piggy_balance INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "active_modifier"):
        cursor.execute("ALTER TABLE players ADD COLUMN active_modifier TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "last_free_spin_time"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_free_spin_time DATETIME DEFAULT NULL")

    if not column_exists(cursor, "players", "login_streak"):
        cursor.execute("ALTER TABLE players ADD COLUMN login_streak INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "last_login_date"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_login_date TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "unlocked_golden"):
        cursor.execute("ALTER TABLE players ADD COLUMN unlocked_golden INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "unlocked_catalyst"):
        cursor.execute("ALTER TABLE players ADD COLUMN unlocked_catalyst INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "unlocked_wildcard"):
        cursor.execute("ALTER TABLE players ADD COLUMN unlocked_wildcard INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "has_insurance"):
        cursor.execute("ALTER TABLE players ADD COLUMN has_insurance INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "lucky_drop_lvl"):
        cursor.execute("ALTER TABLE players ADD COLUMN lucky_drop_lvl INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "piggy_mastery_lvl"):
        cursor.execute("ALTER TABLE players ADD COLUMN piggy_mastery_lvl INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "tool_discount_lvl"):
        cursor.execute("ALTER TABLE players ADD COLUMN tool_discount_lvl INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "vault_slot_0_unlocked"):
        cursor.execute("ALTER TABLE players ADD COLUMN vault_slot_0_unlocked INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "vault_slot_1_unlocked"):
        cursor.execute("ALTER TABLE players ADD COLUMN vault_slot_1_unlocked INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "vault_slot_0_gem"):
        cursor.execute("ALTER TABLE players ADD COLUMN vault_slot_0_gem INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "vault_slot_1_gem"):
        cursor.execute("ALTER TABLE players ADD COLUMN vault_slot_1_gem INTEGER DEFAULT 0")

    # Rescue deduplication flag.  Set to 1 the moment the rescue window is opened;
    # cleared on success, decline, or automatic 10-second expiry.
    if not column_exists(cursor, "players", "is_rescue_active"):
        cursor.execute("ALTER TABLE players ADD COLUMN is_rescue_active INTEGER DEFAULT 0")

    # Timestamp for when the rescue window was opened; used to auto-expire stuck flags
    # after a server restart (is_rescue_active is never cleared by an in-process task).
    if not column_exists(cursor, "players", "rescue_active_since"):
        cursor.execute("ALTER TABLE players ADD COLUMN rescue_active_since TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "last_piggy_smash"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_piggy_smash TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "piggy_smashes_today"):
        cursor.execute("ALTER TABLE players ADD COLUMN piggy_smashes_today INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "total_piggy_smashes"):
        cursor.execute("ALTER TABLE players ADD COLUMN total_piggy_smashes INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "total_piggy_earnings"):
        cursor.execute("ALTER TABLE players ADD COLUMN total_piggy_earnings INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "last_piggy_reset"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_piggy_reset TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "spins_today"):
        cursor.execute("ALTER TABLE players ADD COLUMN spins_today INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "last_spin_reset"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_spin_reset TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "spin_pity_counter"):
        cursor.execute("ALTER TABLE players ADD COLUMN spin_pity_counter INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "mastery_state"):
        cursor.execute("ALTER TABLE players ADD COLUMN mastery_state TEXT DEFAULT '{}'")

    if not column_exists(cursor, "players", "pending_modifiers"):
        cursor.execute("ALTER TABLE players ADD COLUMN pending_modifiers TEXT DEFAULT '{}'")

    if not column_exists(cursor, "players", "last_session_time"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_session_time TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "daily_doubled"):
        cursor.execute("ALTER TABLE players ADD COLUMN daily_doubled INTEGER DEFAULT 0")

    if not column_exists(cursor, "players", "has_seen_starter_pack"):
        cursor.execute("ALTER TABLE players ADD COLUMN has_seen_starter_pack INTEGER DEFAULT 0")

    # --- Push notification bookkeeping (retention) ---
    if not column_exists(cursor, "players", "piggy_full_since"):
        cursor.execute("ALTER TABLE players ADD COLUMN piggy_full_since TEXT DEFAULT NULL")
    if not column_exists(cursor, "players", "piggy_full_notified"):
        cursor.execute("ALTER TABLE players ADD COLUMN piggy_full_notified INTEGER DEFAULT 0")
    if not column_exists(cursor, "players", "last_rush_reminder_date"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_rush_reminder_date TEXT DEFAULT NULL")

    # --- COPPA age bracket: NULL = unknown, 0 = 13+, 1 = under 13 (child-directed) ---
    if not column_exists(cursor, "players", "age_under_13"):
        cursor.execute("ALTER TABLE players ADD COLUMN age_under_13 INTEGER DEFAULT NULL")

    if not column_exists(cursor, "players", "unlocked_cosmetics"):
        cursor.execute("ALTER TABLE players ADD COLUMN unlocked_cosmetics TEXT DEFAULT '[]'")

    if not column_exists(cursor, "players", "active_cosmetic_id"):
        cursor.execute("ALTER TABLE players ADD COLUMN active_cosmetic_id TEXT DEFAULT ''")

    if not column_exists(cursor, "players", "last_first_cashout_bonus_date"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_first_cashout_bonus_date TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "last_welcome_back_date"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_welcome_back_date TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "last_rush_date"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_rush_date TEXT DEFAULT NULL")

    # telemetry_logs must exist before any ALTER TABLE checks against it.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id  TEXT NOT NULL,
            event_name TEXT NOT NULL,
            event_data TEXT NOT NULL DEFAULT '{}',
            timestamp  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_player ON telemetry_logs (player_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_event  ON telemetry_logs (event_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_ts     ON telemetry_logs (timestamp)")

    if not column_exists(cursor, "telemetry_logs", "session_id"):
        cursor.execute("ALTER TABLE telemetry_logs ADD COLUMN session_id TEXT DEFAULT NULL")

    if not column_exists(cursor, "players", "vault_pass_active"):
        cursor.execute("ALTER TABLE players ADD COLUMN vault_pass_active INTEGER DEFAULT 0")
    if not column_exists(cursor, "players", "vault_pass_expiry"):
        cursor.execute("ALTER TABLE players ADD COLUMN vault_pass_expiry TEXT DEFAULT NULL")
    if not column_exists(cursor, "players", "shards_balance"):
        cursor.execute("ALTER TABLE players ADD COLUMN shards_balance INTEGER DEFAULT 0")
    if not column_exists(cursor, "players", "last_vault_pass_drip"):
        cursor.execute("ALTER TABLE players ADD COLUMN last_vault_pass_drip TEXT DEFAULT NULL")

    # --- P6-S2: BI telemetry enrichment columns ---
    if not column_exists(cursor, "players", "install_date"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN install_date TEXT DEFAULT NULL"
        )
        # Backfill existing players: use their oldest telemetry timestamp
        # as a best-effort install date. If none exists, use today.
        cursor.execute("""
            UPDATE players SET install_date = COALESCE(
                (SELECT MIN(timestamp) FROM telemetry_logs
                 WHERE telemetry_logs.player_id = players.player_id),
                CURRENT_TIMESTAMP
            ) WHERE install_date IS NULL
        """)

    if not column_exists(cursor, "players", "client_platform"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN client_platform TEXT DEFAULT 'unknown'"
        )

    if not column_exists(cursor, "players", "client_version"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN client_version TEXT DEFAULT '1.0.0'"
        )

    if not column_exists(cursor, "players", "country_code"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN country_code TEXT DEFAULT 'XX'"
        )

    if not column_exists(cursor, "telemetry_logs", "client_platform"):
        cursor.execute(
            "ALTER TABLE telemetry_logs ADD COLUMN client_platform TEXT DEFAULT NULL"
        )

    if not column_exists(cursor, "telemetry_logs", "client_version"):
        cursor.execute(
            "ALTER TABLE telemetry_logs ADD COLUMN client_version TEXT DEFAULT NULL"
        )

    if not column_exists(cursor, "telemetry_logs", "country_code"):
        cursor.execute(
            "ALTER TABLE telemetry_logs ADD COLUMN country_code TEXT DEFAULT NULL"
        )

    # --- P6-S3: daily free-gem cap tracking ---
    if not column_exists(cursor, "players", "daily_gems_earned"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN daily_gems_earned INTEGER DEFAULT 0"
        )
    if not column_exists(cursor, "players", "last_gem_cap_reset"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN last_gem_cap_reset TEXT DEFAULT NULL"
        )

    # --- P6-S3: Vault Boost active-boost tracking ---
    if not column_exists(cursor, "players", "boost_active_until"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN boost_active_until TEXT DEFAULT NULL"
        )
    if not column_exists(cursor, "players", "boost_type"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN boost_type TEXT DEFAULT NULL"
        )

    # JSON blob keyed by offer_id; value is 1 when the offer has been shown.
    # e.g. {"board_stage4_wall": 1}
    if not column_exists(cursor, "players", "seen_offers"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN seen_offers TEXT DEFAULT '{}'"
        )

    # --- Sprint 7.3: Achievement expansion stats ---
    _new_ach_cols = {
        "high_tier_merges":    "INTEGER DEFAULT 0",
        "best_session_merges": "INTEGER DEFAULT 0",
        "best_single_cashout": "INTEGER DEFAULT 0",
        "peak_wallet_balance": "INTEGER DEFAULT 0",
        "diagonal_cashouts":   "INTEGER DEFAULT 0",
        "multi_line_cashouts": "INTEGER DEFAULT 0",
        "login_days":          "INTEGER DEFAULT 0",
        "best_login_streak":   "INTEGER DEFAULT 0",
        "perfect_cleanses":    "INTEGER DEFAULT 0",
        "hammers_used":        "INTEGER DEFAULT 0",
        "total_spins":         "INTEGER DEFAULT 0",
        "player_xp":           "INTEGER DEFAULT 0",
        "player_level":        "INTEGER DEFAULT 1",
        "max_tier_reached":    "INTEGER DEFAULT 0",
    }
    for _col, _defn in _new_ach_cols.items():
        if not column_exists(cursor, "players", _col):
            cursor.execute(f"ALTER TABLE players ADD COLUMN {_col} {_defn}")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS achievements (
            player_id TEXT,
            achievement_id TEXT,
            unlocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, achievement_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS quest_claims (
            player_id TEXT,
            quest_id  TEXT,
            claim_day TEXT,
            PRIMARY KEY (player_id, quest_id, claim_day)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS achievement_claims (
            player_id TEXT,
            ach_id    TEXT,
            tier      INT,
            PRIMARY KEY (player_id, ach_id, tier)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_quests (
            player_id TEXT PRIMARY KEY,
            quest_id TEXT NOT NULL,
            progress INTEGER DEFAULT 0,
            target INTEGER NOT NULL,
            reward INTEGER NOT NULL,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Structured event stream with allowlist validation.  Separate from the legacy
    # telemetry_logs table so BI queries can target a clean, typed schema.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_events (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id        TEXT    NOT NULL,
            event_type       TEXT    NOT NULL,
            params_json      TEXT    NOT NULL DEFAULT '{}',
            client_timestamp REAL    DEFAULT NULL,
            server_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_tevents_player "
        "ON telemetry_events (player_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_tevents_type "
        "ON telemetry_events (event_type)"
    )

    # One active run state per player.  Upserted on every cashout/tool use;
    # deleted on clean game-over.  The JSON blob mirrors _serialize_run_state().
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS run_states (
            player_id TEXT PRIMARY KEY,
            state_json TEXT NOT NULL,
            saved_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # --- P6-S5: A/B variant assignment persistence ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ab_assignments (
            player_id   TEXT NOT NULL,
            flag_key    TEXT NOT NULL,
            variant     TEXT NOT NULL,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, flag_key)
        )
    """)

    # --- P6-S5: lifetime IAP revenue tracking ---
    if not column_exists(cursor, "players", "lifetime_iap_usd"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN lifetime_iap_usd REAL DEFAULT 0.0"
        )

    # --- Sprint 7.2: first-purchase 10x bonus flag ---
    if not column_exists(cursor, "players", "first_iap_done"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN first_iap_done INTEGER DEFAULT 0"
        )

    if not column_exists(cursor, "players", "seen_day7_offer"):
        cursor.execute(
            "ALTER TABLE players ADD COLUMN seen_day7_offer INTEGER DEFAULT 0"
        )

    # --- P6-S4: GDPR deletion audit log (never purged) ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gdpr_deletion_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id  TEXT NOT NULL,
            deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            ip_address TEXT DEFAULT NULL
        )
    """)

    # --- P6-S4: Push notification device tokens ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS device_tokens (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id  TEXT NOT NULL,
            token      TEXT NOT NULL,
            platform   TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, platform)
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_device_tokens_player "
        "ON device_tokens (player_id)"
    )

    # --- Push delivery audit log ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS push_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id  TEXT NOT NULL,
            push_type  TEXT,
            tokens     INTEGER DEFAULT 0,
            sent       INTEGER DEFAULT 0,
            failed     INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_push_log_player ON push_log (player_id)"
    )

    # --- IAP receipt idempotency log ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iap_receipts (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id      TEXT    NOT NULL,
            transaction_id TEXT    NOT NULL UNIQUE,
            product_id     TEXT    NOT NULL,
            platform       TEXT    NOT NULL,
            usd_amount     REAL    NOT NULL DEFAULT 0.0,
            created_at     TEXT    NOT NULL
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_iap_receipts_player "
        "ON iap_receipts (player_id)"
    )

    for lvl, cfg in MARKET_CONFIG.items():
        cursor.execute(_SQL_INSERT_MARKET_PRICE, (lvl, cfg["base"]))

    conn.commit()
    conn.close()


def extract_auth(request: Request):
    """Decode and validate the Bearer JWT.  Returns (player_id, raw_token_string)."""
    auth_header = request.headers.get("Authorization", "").strip()
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header")
    token = auth_header[7:].strip()
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired -- please re-register the device")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    player_id = payload.get("player_id", "").strip()
    if not player_id:
        raise HTTPException(status_code=401, detail="Token is missing the player_id claim")
    return player_id, token


def extract_player_id(request: Request) -> str:
    """Thin shim over extract_auth for the 30+ endpoints that only need the player_id."""
    player_id, _ = extract_auth(request)
    return player_id


async def _verify_financial_signature(request: Request, raw_token: str) -> None:
    """Verify that X-Signature == HMAC-SHA256(key=jwt_string, msg=raw_request_body).
    Called only on the four financial endpoints.  Raises 403 on any mismatch.
    Starlette caches the body after the first read, so callers may safely re-read
    it via await request.body() or await request.json() after this call returns."""
    sig_header = request.headers.get("X-Signature", "").strip().lower()
    if not sig_header:
        raise HTTPException(status_code=403, detail="Missing X-Signature header")
    body_bytes = await request.body()
    expected   = hmac.new(raw_token.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig_header):
        raise HTTPException(status_code=403, detail="Invalid request signature")


# ---------------------------------------------------------------------------
# Dual-dialect SQL constants
#
# SQLite uses ? placeholders and INSERT OR IGNORE / INSERT OR REPLACE.
# PostgreSQL uses %s and ON CONFLICT clauses with explicit conflict targets.
# INSERT OR REPLACE semantics differ by table:
#   - "ignore"  tables want ON CONFLICT DO NOTHING
#   - "replace" tables want ON CONFLICT (...) DO UPDATE SET ...
#
# Naming: _SQL_<LABEL>_SQ = SQLite form   _SQL_<LABEL>_PG = PostgreSQL form
#         _SQL_<LABEL>    = the one that is actually used at runtime
# ---------------------------------------------------------------------------

_SQL_UPSERT_PLAYER_SQ = """
    INSERT OR IGNORE INTO players
    (player_id, total_money, max_unlocked_tier, board_size, board_stage, tutorial_completed,
     lifetime_cash_earned, best_survival_time, best_cashouts_run, best_combo,
     cursed_tiles_removed, total_runs, total_merges, gems_balance, piggy_balance,
     active_modifier, last_free_spin_time)
    VALUES (?, 2000, 2, 3, 1, 0, 0, 0, 0, 1, 0, 0, 0, 20, 0, NULL, NULL)
"""
_SQL_UPSERT_PLAYER_PG = """
    INSERT INTO players
    (player_id, total_money, max_unlocked_tier, board_size, board_stage, tutorial_completed,
     lifetime_cash_earned, best_survival_time, best_cashouts_run, best_combo,
     cursed_tiles_removed, total_runs, total_merges, gems_balance, piggy_balance,
     active_modifier, last_free_spin_time)
    VALUES (%s, 2000, 2, 3, 1, 0, 0, 0, 0, 1, 0, 0, 0, 20, 0, NULL, NULL)
    ON CONFLICT (player_id) DO NOTHING
"""
_SQL_UPSERT_PLAYER = _SQL_UPSERT_PLAYER_PG if _USE_POSTGRES else _SQL_UPSERT_PLAYER_SQ

_SQL_INSERT_MARKET_PRICE_SQ = """
    INSERT OR IGNORE INTO market_prices (item_level, current_price)
    VALUES (?, ?)
"""
_SQL_INSERT_MARKET_PRICE_PG = """
    INSERT INTO market_prices (item_level, current_price)
    VALUES (%s, %s)
    ON CONFLICT (item_level) DO NOTHING
"""
_SQL_INSERT_MARKET_PRICE = _SQL_INSERT_MARKET_PRICE_PG if _USE_POSTGRES else _SQL_INSERT_MARKET_PRICE_SQ

# device_tokens: INSERT OR REPLACE -- must UPDATE the token when the row exists,
# not silently skip it.  Conflict target mirrors the UNIQUE(player_id, platform) constraint.
_SQL_UPSERT_DEVICE_TOKEN_SQ = (
    "INSERT OR REPLACE INTO device_tokens "
    "(player_id, token, platform) VALUES (?, ?, ?)"
)
_SQL_UPSERT_DEVICE_TOKEN_PG = (
    "INSERT INTO device_tokens "
    "(player_id, token, platform) VALUES (%s, %s, %s) "
    "ON CONFLICT (player_id, platform) DO UPDATE SET token = EXCLUDED.token"
)
_SQL_UPSERT_DEVICE_TOKEN = _SQL_UPSERT_DEVICE_TOKEN_PG if _USE_POSTGRES else _SQL_UPSERT_DEVICE_TOKEN_SQ

# ab_assignments: INSERT OR IGNORE -- once a variant is assigned, never overwrite it.
_SQL_INSERT_AB_ASSIGNMENT_SQ = (
    "INSERT OR IGNORE INTO ab_assignments "
    "(player_id, flag_key, variant) VALUES (?, ?, ?)"
)
_SQL_INSERT_AB_ASSIGNMENT_PG = (
    "INSERT INTO ab_assignments "
    "(player_id, flag_key, variant) VALUES (%s, %s, %s) "
    "ON CONFLICT (player_id, flag_key) DO NOTHING"
)
_SQL_INSERT_AB_ASSIGNMENT = _SQL_INSERT_AB_ASSIGNMENT_PG if _USE_POSTGRES else _SQL_INSERT_AB_ASSIGNMENT_SQ

# achievements: INSERT OR IGNORE -- an already-unlocked achievement must not be re-awarded.
_SQL_INSERT_ACHIEVEMENT_SQ = """
    INSERT OR IGNORE INTO achievements (player_id, achievement_id)
    VALUES (?, ?)
"""
_SQL_INSERT_ACHIEVEMENT_PG = """
    INSERT INTO achievements (player_id, achievement_id)
    VALUES (%s, %s)
    ON CONFLICT (player_id, achievement_id) DO NOTHING
"""
_SQL_INSERT_ACHIEVEMENT = _SQL_INSERT_ACHIEVEMENT_PG if _USE_POSTGRES else _SQL_INSERT_ACHIEVEMENT_SQ

# player_quests: INSERT OR REPLACE -- must overwrite all columns when re-assigning a quest.
# Conflict target is the player_id PRIMARY KEY.
_SQL_ASSIGN_QUEST_SQ = """
    INSERT OR REPLACE INTO player_quests
    (player_id, quest_id, progress, target, reward, assigned_at)
    VALUES (?, ?, 0, ?, ?, CURRENT_TIMESTAMP)
"""
_SQL_ASSIGN_QUEST_PG = """
    INSERT INTO player_quests
    (player_id, quest_id, progress, target, reward, assigned_at)
    VALUES (%s, %s, 0, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (player_id) DO UPDATE SET
        quest_id    = EXCLUDED.quest_id,
        progress    = 0,
        target      = EXCLUDED.target,
        reward      = EXCLUDED.reward,
        assigned_at = EXCLUDED.assigned_at
"""
_SQL_ASSIGN_QUEST = _SQL_ASSIGN_QUEST_PG if _USE_POSTGRES else _SQL_ASSIGN_QUEST_SQ


def get_or_create_player(player_id: str, cursor) -> None:
    cursor.execute(_SQL_UPSERT_PLAYER, (player_id,))


def calculate_adjusted_prices(board_data: List[int]):
    if len(board_data) == 0:
        return {}
    if len(board_data) > 64:
        raise HTTPException(status_code=400, detail="board_data exceeds maximum supported board size")

    # Serve market price rows from Redis when available (5s TTL).
    # JSON round-trip turns int keys to strings; restore them with int(k).
    market_raw = None
    if _REDIS is not None:
        try:
            _cached = _REDIS.get("market_prices_cache")
            if _cached:
                market_raw = {int(k): v for k, v in json.loads(_cached).items()}
        except Exception:
            pass  # Redis fault: fall through to DB

    if market_raw is None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT item_level, current_price FROM market_prices")
        market_raw = dict(cursor.fetchall())
        conn.close()
        if _REDIS is not None:
            try:
                _REDIS.setex("market_prices_cache", 5, json.dumps(market_raw))
            except Exception:
                pass  # Redis fault: prices served from DB, cache skipped

    counts = {lvl: board_data.count(lvl) for lvl in MARKET_CONFIG.keys()}
    adjusted_prices = {}

    # Count all non-zero cells (wildcards, golden tiles included) for pressure math
    filled_cells = len([cell for cell in board_data if cell not in (0,)])
    total_cells = len(board_data)
    board_fill_rate = filled_cells / total_cells if total_cells > 0 else 0

    if board_fill_rate >= 0.95:
        board_pressure_penalty = 0.35
    elif board_fill_rate >= 0.85:
        board_pressure_penalty = 0.20
    elif board_fill_rate >= 0.75:
        board_pressure_penalty = 0.10
    else:
        board_pressure_penalty = 0.0

    for lvl, base_market_price in market_raw.items():
        cfg = MARKET_CONFIG[lvl]
        count = counts[lvl]

        hoarding_penalty = max(0, count - cfg["threshold"]) * cfg["penalty"]

        shadow_penalty = 0
        if lvl > 1:
            lower_lvl_count = counts[lvl - 1]
            lower_cfg = MARKET_CONFIG[lvl - 1]
            shadow_penalty = max(0, lower_lvl_count - (lower_cfg["threshold"] - 1)) * (cfg["penalty"] * 0.7)

        price_after_penalties = base_market_price - hoarding_penalty - shadow_penalty
        final_price = price_after_penalties * (1 - board_pressure_penalty)

        adjusted_prices[lvl] = int(max(cfg["min"], min(cfg["max"], final_price)))

    return adjusted_prices


def update_global_market(item_level, impact_type="sale", is_player_sale: bool = False):
    if item_level not in MARKET_CONFIG:
        return

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT current_price FROM market_prices WHERE item_level = ?", (item_level,))
    row = cursor.fetchone()

    if row is None:
        conn.close()
        return

    current_price = row[0]
    cfg = MARKET_CONFIG[item_level]

    if impact_type == "sale":
        drop = 3.5 if is_player_sale else 1.2
        new_price = max(cfg["min"], current_price - drop)
    else:
        recovery = 0.5
        new_price = min(cfg["max"], current_price + recovery)

    cursor.execute(
        "UPDATE market_prices SET current_price = ? WHERE item_level = ?",
        (new_price, item_level)
    )

    conn.commit()
    conn.close()

    if _REDIS is not None:
        try:
            _REDIS.delete("market_prices_cache")
        except Exception:
            pass  # Redis fault: stale cache expires naturally within 5s TTL


def calculate_tool_costs(board_data: List[int], elapsed_time: float = 0.0,
                         board_rows: int = 0, board_cols: int = 0,
                         max_unlocked_tier: int = 1):
    """
    Production-ready tool pricing model.

    Design goals:
    - Hammer is the emergency decision tool; cost scales hard with board danger.
    - Catch-up Crystal is a gold-sink: base price scales exponentially with tier
      so it stays meaningful at every stage of progression.
    - Prices scale with board pressure but never explode to irrelevance.
    """
    if len(board_data) == 0:
        return {}
    if len(board_data) > 64:
        raise HTTPException(status_code=400, detail="board_data exceeds maximum supported board size")
    black_tiles = board_data.count(-1)
    filled_cells = len([cell for cell in board_data if cell != 0])
    total_cells = len(board_data)
    fill_rate = filled_cells / total_cells if total_cells > 0 else 0

    if board_rows > 0 and board_cols > 0:
        effective_size = max(board_rows, board_cols)
    else:
        effective_size = max(2, int(total_cells ** 0.5)) if total_cells > 0 else 4

    # Controlled pressure curve.
    pressure_multiplier = 1.0
    if fill_rate >= 0.92:
        pressure_multiplier = 1.85
    elif fill_rate >= 0.82:
        pressure_multiplier = 1.55
    elif fill_rate >= 0.70:
        pressure_multiplier = 1.30
    elif fill_rate >= 0.58:
        pressure_multiplier = 1.12

    # Time scaling: noticeable in long runs, not punishing immediately.
    time_stage = int(elapsed_time // 60)

    # Black-tile quadratic pressure, capped to prevent runaway costs.
    black_pressure = min(black_tiles ** 2, 16)

    hammer_cost = int(
        (
            300
            + (effective_size * 80)
            + (black_tiles * 150)
            + (black_pressure * 35)
            + (time_stage * 55)
        )
        * pressure_multiplier
    )

    # Exponential tier-based pricing for the Catch-up Crystal (spawns tier N-1).
    # base_cost * 2.5^(tier-1) anchors the price to progression, then a
    # mild pressure modifier makes it slightly more expensive under board danger.
    crystal_tier = max(1, max_unlocked_tier - 1)
    BASE_CRYSTAL_COST = 80
    tier_base = int(BASE_CRYSTAL_COST * (2.5 ** (crystal_tier - 1)))
    buy_crystal_cost = int(
        (
            tier_base
            + int(fill_rate * 180)
            + (black_tiles * 40)
            + (time_stage * 25)
        )
        * (1.0 + ((pressure_multiplier - 1.0) * 0.40))
    )

    return {
        "hammer": max(350, hammer_cost),
        "buy_crystal": max(80, buy_crystal_cost),
        "crystal_tier": crystal_tier,
        "black_tiles": black_tiles,
        "fill_rate": round(fill_rate, 3),
        "pressure_multiplier": round(pressure_multiplier, 2),
        "time_stage": time_stage,
    }

# Indexes must stay in sync with PRIZE_CATALOG in LuckySpin.gd.
# "pending_mod" prizes are stored in pending_modifiers JSON and injected onto
# the board at the start of the NEXT run; they are NOT active_modifier values.
WHEEL_PRIZES = [
    {"id": "gems_50",          "name": "50 Purple Gems",        "gold": 0,    "gems": 50,  "modifier": None,             "pending_mod": None,             "rarity": "uncommon"},  # 0
    {"id": "gold_1000",        "name": "1,000 Gold",            "gold": 1000, "gems": 0,   "modifier": None,             "pending_mod": None,             "rarity": "common"},    # 1
    {"id": "hammer_2",         "name": "2 Hammers",             "gold": 0,    "gems": 0,   "modifier": None,             "pending_mod": "free_hammers",   "rarity": "rare"},      # 2
    {"id": "gold_2500",        "name": "2,500 Gold",            "gold": 2500, "gems": 0,   "modifier": None,             "pending_mod": None,             "rarity": "uncommon"},  # 3
    {"id": "gems_jackpot",     "name": "100 Purple Gems",       "gold": 0,    "gems": 100, "modifier": None,             "pending_mod": None,             "rarity": "jackpot"},   # 4
    {"id": "golden_x4",        "name": "X4 Golden Diamond",     "gold": 0,    "gems": 0,   "modifier": None,             "pending_mod": "golden_x4",      "rarity": "epic"},      # 5
    {"id": "golden_cleanser",  "name": "Rock Cleanser Diamond", "gold": 0,    "gems": 0,   "modifier": None,             "pending_mod": "golden_cleanser","rarity": "epic"},      # 6
    {"id": "gold_500",         "name": "500 Gold",              "gold": 500,  "gems": 0,   "modifier": None,             "pending_mod": None,             "rarity": "common"},    # 7
    {"id": "free_rescue",      "name": "Free Rescue",           "gold": 0,    "gems": 0,   "modifier": "free_rescue",    "pending_mod": None,             "rarity": "uncommon"},  # 8
    {"id": "gems_10",          "name": "10 Purple Gems",        "gold": 0,    "gems": 10,  "modifier": None,             "pending_mod": None,             "rarity": "common"},    # 9
    {"id": "boost_cashout_2x", "name": "2x Cashout Boost",      "gold": 0,    "gems": 0,   "modifier": "boost_cashout_2x","pending_mod": None,            "rarity": "epic"},      # 10 NEW
    {"id": "gold_5000",        "name": "5,000 Gold",            "gold": 5000, "gems": 0,   "modifier": None,             "pending_mod": None,             "rarity": "uncommon"},  # 11 NEW
]
# Total weight = 100 (clean per-%% read). Jackpot (idx 4) = 1/100 = 1.00%%.
# Rarity mix: common 48%% / uncommon 36%% / rare 7%% / epic 8%% / jackpot 1%%.
# Two common slots (1, 7) reduced from 25 to 22 to accommodate the two new prizes.
WHEEL_WEIGHTS = [9, 20, 7, 12, 1, 3, 2, 20, 6, 8, 3, 9]

# After PITY_THRESHOLD consecutive common-rarity outcomes the next spin is drawn
# exclusively from the non-common pool (uncommon / rare / epic / jackpot).
# This prevents worst-case runs of 20+ low-value results.
PITY_THRESHOLD: int = 7
_COMMON_RARITIES: set = {"common"}

import math as _math
_WHEEL_SLICE_ANGLE: float = 2.0 * _math.pi / len(WHEEL_PRIZES)

# Escalating re-spin costs (indexed by spins already done today).
# Index 0 = first spin (free). Index 3+ = 40 Gems.
RESPIN_COSTS = [0, 10, 20, 40]


def _get_spin_cost(spins_today: int) -> int:
    idx = min(spins_today, len(RESPIN_COSTS) - 1)
    return RESPIN_COSTS[idx]

# GemBoost permanent upgrades -- progressive cost curve.
# Index N-1 = gem cost to reach Level N (e.g. index 0 is the Lvl1 purchase price).
# Levels 1-2 are deliberately cheap to hook players into the upgrade loop.
# Levels 4-5 are steep gem sinks targeting engaged/paying players.
_MASTERY_COST_TABLE = {
    "lucky_drop":    [ 10,  20,  50,  90, 150, 250],            # staggered T3/T4/T5 boost (6 levels)
    "piggy_mastery": [  5,  10,  15,  25,  40,  60, 85, 115, 150, 200],  # +1..+10 gold/merge (10 levels)
    "efficiency":    [ 10,  22,  48, 106, 233],                 # -3/-6/-10/-15/-20% cursed-tile chance
}

MASTERY_CONFIG = {
    "lucky_drop":    {"col": "lucky_drop_lvl",    "costs": _MASTERY_COST_TABLE["lucky_drop"],    "max": 6},
    "piggy_mastery": {"col": "piggy_mastery_lvl", "costs": _MASTERY_COST_TABLE["piggy_mastery"], "max": 10},
    "efficiency":    {"col": "tool_discount_lvl",  "costs": _MASTERY_COST_TABLE["efficiency"],    "max": 5},
}
# Parameterized equivalents of the f-string queries in /shop/upgrade_mastery.
# Keyed by the col value from MASTERY_CONFIG so the endpoint never interpolates column names.
_MASTERY_SELECT_SQL = {
    "lucky_drop_lvl":    "SELECT gems_balance, lucky_drop_lvl    FROM players WHERE player_id = ?",
    "piggy_mastery_lvl": "SELECT gems_balance, piggy_mastery_lvl FROM players WHERE player_id = ?",
    "tool_discount_lvl": "SELECT gems_balance, tool_discount_lvl FROM players WHERE player_id = ?",
}
_MASTERY_UPDATE_SQL = {
    "lucky_drop_lvl":    "UPDATE players SET gems_balance = gems_balance - ?, lucky_drop_lvl    = ? WHERE player_id = ?",
    "piggy_mastery_lvl": "UPDATE players SET gems_balance = gems_balance - ?, piggy_mastery_lvl = ? WHERE player_id = ?",
    "tool_discount_lvl": "UPDATE players SET gems_balance = gems_balance - ?, tool_discount_lvl = ? WHERE player_id = ?",
}

# Mastery Lab -- gold-purchased permanent progression tree.
# cost per upgrade = base_cost * (cost_multiplier ^ current_level)
MASTERY_TREE = {
    # ---- Luck branch ----
    "luck_lv2_drop": {
        "branch":          "Luck",
        "display_name":    "Luck Boost",
        "description":     "Increases Lv2 gem spawn chance",
        "max_level":       5,
        "base_cost":       2000,
        "cost_multiplier": 2.0,
        "dependency":      None,
    },
    # ---- Economy branch ----
    "econ_piggy_cap": {
        "branch":          "Economy",
        "display_name":    "Piggy Capacity",
        "description":     "Increases max piggy bank gold cap",
        "max_level":       5,
        "base_cost":       2500,
        "cost_multiplier": 2.0,
        "dependency":      None,
    },
    "econ_cashout_bonus": {
        "branch":          "Economy",
        "display_name":    "Cashout Bonus",
        "description":     "Adds +5% gold to every Piggy Bank cashout per level (max +15%).",
        "max_level":       3,
        "base_cost":       4000,
        "cost_multiplier": 2.0,
        "dependency":      "econ_piggy_cap",
    },
    # ---- Board branch ----
    "board_hammer_cost": {
        "branch":          "Board",
        "display_name":    "Hammer Discount",
        "description":     "Reduces Hammer tool price by 5% per level (max 25%).",
        "max_level":       5,
        "base_cost":       3000,
        "cost_multiplier": 2.0,
        "dependency":      None,
    },
}

# IAP product catalog — mirrors RoyalMarket.gd CATALOG constant.
# 100 Gems = $0.99 baseline; bundle_welcome includes a Gold bonus to sweeten the offer.
# Sprint 7.2: Gem bundle product catalog — single source of truth for the new
# /iap/validate/* endpoints and the /shop/iap_catalog display endpoint.
# product_id values must match the Store Console SKU identifiers exactly.
GEM_BUNDLES: List[Dict[str, Any]] = [
    {"product_id": "gems_50",        "gems":   50, "gold":    0, "usd":  0.99, "label": "Gem Pouch"},
    {"product_id": "bundle_welcome", "gems":  120, "gold": 1000, "usd":  1.99, "label": "Royal Starter Kit",  "badge": "WELCOME BUNDLE"},
    {"product_id": "gems_350",       "gems":  350, "gold":    0, "usd":  4.99, "label": "Gem Chest"},
    {"product_id": "gems_1500",      "gems": 1500, "gold":    0, "usd":  9.99, "label": "Royal Vault"},
    {"product_id": "gems_1800",      "gems": 1800, "gold":    0, "usd": 19.99, "label": "Royal Hoard",        "badge": "MOST POPULAR"},
    {"product_id": "gems_6000",      "gems": 6000, "gold":    0, "usd": 49.99, "label": "Titan Reserve",      "badge": "BEST VALUE"},
]
_GEM_BUNDLES_BY_ID: Dict[str, Dict[str, Any]] = {b["product_id"]: b for b in GEM_BUNDLES}

IAP_CATALOG = {
    # --- Base & Mid Tiers (Nerfed for better whale conversion) ---
    "gems_50": {"gems": 50, "gold": 0, "price_usd": "0.99"},
    "bundle_welcome": {"gems": 120, "gold": 1_000, "price_usd": "1.99"},
    "gems_350": {"gems": 350, "gold": 0, "price_usd": "4.99"},
    "gems_1500": {"gems": 1500, "gold": 0, "price_usd": "9.99"},

    # --- Whale SKUs (Aligned with the new client IDs) ---
    # שמרתי לך את הקוסמטיקה המיוחדת שהייתה לך בשרת, אבל עדכנתי את המזהים והכמויות
    "gems_1800": {"gems": 1800, "gold": 0, "price_usd": "19.99",
                  "exclusive_cosmetic": "founder_vault_frame"},
    "gems_6000": {"gems": 6000, "gold": 0, "price_usd": "49.99",
                  "exclusive_cosmetic": "titan_board_skin"},

    # (Optional) $99 tier - Client doesn't show it yet, but safe to keep in server for the future
    "gems_22000_apex": {"gems": 22000, "gold": 200_000, "price_usd": "99.99",
                        "exclusive_cosmetic": "apex_animated_bg"},

    # --- Vault Pass subscription ---
    # תוקן ל-4.99$ כדי שיתאים לטקסט שמוצג לשחקן במשחק עצמו
    "vault_pass_30": {"gems": 0, "gold": 0, "price_usd": "4.99",
                      "vault_pass": True, "days": 30},
}

@app.get("/health")
def health():
    checks = {
        "db":         "sqlite" if not _USE_POSTGRES else "postgres",
        "redis":      "connected" if _REDIS is not None else "disabled",
        "jwt_secret": "default_UNSAFE" if _JWT_USING_DEFAULT else "configured",
        "push":  "configured" if (
            os.environ.get("FIREBASE_CREDENTIALS_JSON") or
            os.environ.get("APNS_KEY_P8")
        ) else "disabled",
        "iap_validation": (
            "bypassed" if os.environ.get("BYPASS_RECEIPT_VALIDATION", "").lower() == "true"
            else "configured" if (
                os.environ.get("APPLE_SHARED_SECRET") or
                os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
            ) else "unconfigured"
        ),
    }
    return {
        "status":  "ok",
        "version": "phase-6",
        "checks":  checks,
    }


@app.get("/health_security")
def health_security():
    """
    Returns the JWT secret status without exposing the secret value.
    Safe to call from any monitoring tool or CI pipeline.
    """
    return {
        "jwt_secret_status": "default_UNSAFE" if _JWT_USING_DEFAULT else "configured",
        "env":               _ENV,
    }


# ---------------------------------------------------------------------------
# GDPR Article 17: Right to Erasure
# ---------------------------------------------------------------------------

_GDPR_DELETE_SQL = [
    "DELETE FROM run_states         WHERE player_id = ?",
    "DELETE FROM quest_claims       WHERE player_id = ?",
    "DELETE FROM achievement_claims WHERE player_id = ?",
    "DELETE FROM player_quests      WHERE player_id = ?",
    "DELETE FROM achievements       WHERE player_id = ?",
    "DELETE FROM sales_log          WHERE player_id = ?",
    "DELETE FROM telemetry_logs     WHERE player_id = ?",
    "DELETE FROM device_tokens      WHERE player_id = ?",
    "DELETE FROM players            WHERE player_id = ?",
]

@app.delete("/player/data")
async def delete_player_data(request: Request):
    """
    GDPR Article 17: Right to Erasure.
    Deletes all personal data for the authenticated player.
    Logs the deletion event to gdpr_deletion_log (retained for
    legal compliance -- this table is never purged).
    """
    player_id = extract_player_id(request)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        for sql in _GDPR_DELETE_SQL:
            try:
                cursor.execute(sql, (player_id,))
            except Exception as _del_err:
                print(f"[gdpr] WARN: {sql[:40]}... failed: {_del_err}")
        client_ip = request.client.host if request.client else "unknown"
        cursor.execute(
            "INSERT INTO gdpr_deletion_log (player_id, ip_address) "
            "VALUES (?, ?)", (player_id, client_ip)
        )
        conn.commit()
    finally:
        conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "deleted", "player_id": player_id}


# ---------------------------------------------------------------------------
# Push notification: device token registration
# ---------------------------------------------------------------------------

@app.post("/device/register")
async def register_device_token(request: Request):
    """Store FCM (Android) or APNs (iOS) push token for this player."""
    player_id = extract_player_id(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    token    = str(body.get("token",    "")).strip()
    platform = str(body.get("platform", "")).strip().lower()
    if not token or platform not in ("fcm", "apns"):
        raise HTTPException(400, "token and platform (fcm|apns) are required")
    if len(token) > 512:
        raise HTTPException(400, "token too long")
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(_SQL_UPSERT_DEVICE_TOKEN, (player_id, token, platform))
        conn.commit()
    finally:
        conn.close()
    return {"status": "registered"}


@app.post("/auth/register")
async def register_device(request: Request):
    """
    Trust bootstrap -- no prior auth required.
    Accepts a player UUID, creates the player row if absent, and returns a signed JWT.
    The client stores the JWT in user://player_auth.save and attaches it as
    'Authorization: Bearer <token>' on every subsequent request.
    """
    try:
        body      = await request.json()
        player_id = str(body.get("player_id", "")).strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")

    conn = get_connection()
    try:
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        conn.commit()

        # Set install_date only on first registration (don't overwrite returning players).
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cursor.execute(
            "UPDATE players SET install_date = ? "
            "WHERE player_id = ? AND install_date IS NULL",
            (now_iso, player_id)
        )

        # Read platform/version from the registration body if provided.
        client_platform = str(body.get("platform",    "unknown"))[:32]
        client_version  = str(body.get("app_version", "1.0.0"))[:16]
        cursor.execute(
            "UPDATE players SET client_platform = ?, client_version = ? "
            "WHERE player_id = ?",
            (client_platform, client_version, player_id)
        )
        conn.commit()
    finally:
        conn.close()

    # Geo-gate: block new registrations from outside soft-launch countries.
    # Existing authenticated players are never affected (this is /auth/register only).
    if _SOFT_LAUNCH_COUNTRIES:
        client_ip = request.client.host if request.client else ""
        country   = await _get_country_from_ip(client_ip)
        if country not in _SOFT_LAUNCH_COUNTRIES and country != "XX":
            raise HTTPException(
                status_code=451,
                detail={
                    "error":   "geo_restricted",
                    "message": "Hostile Merge is not yet available in your region.",
                    "country": country,
                }
            )

    import secrets
    server_session_id = secrets.token_hex(16)

    now     = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "player_id": player_id,
        "iat":       int(now.timestamp()),
        # 30-day expiry -- balances security and UX; use POST /auth/refresh to renew.
        "exp":       int((now + datetime.timedelta(days=30)).timestamp()),
    }
    token = jwt.encode(payload, _JWT_SECRET, algorithm=_JWT_ALGORITHM)
    # PyJWT >= 2.0 returns str; earlier versions return bytes.
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return {"jwt_token": token, "server_session_id": server_session_id}


@app.post("/auth/refresh")
async def refresh_token(request: Request):
    """Exchange a valid (non-expired) JWT for a new 30-day token."""
    player_id, _ = extract_auth(request)
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "player_id": player_id,
        "iat":       int(now.timestamp()),
        "exp":       int((now + datetime.timedelta(days=30)).timestamp()),
    }
    token = jwt.encode(payload, _JWT_SECRET, algorithm=_JWT_ALGORITHM)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return {"jwt_token": token}


# ---------------------------------------------------------------------------
# A/B test flag assignment  (persistent variant per player)
# ---------------------------------------------------------------------------

@app.get("/config/flags")
async def get_config_flags(request: Request):
    player_id = extract_player_id(request)

    FLAG_DEFINITIONS: Dict[str, list] = {
        "starter_pack_price":    ["standard", "discounted"],
        "game_over_offer_style": ["modal", "banner"],
        "daily_reward_style":    ["calendar", "wheel"],
        "vault_pass_price":      ["6.99", "4.99", "9.99"],
    }
    FLAG_DEFAULTS: Dict[str, str] = {
        "starter_pack_price":    "standard",
        "game_over_offer_style": "modal",
        "daily_reward_style":    "calendar",
        "vault_pass_price":      "6.99",
    }
    active_raw  = os.environ.get("ACTIVE_FLAGS", "").strip()
    active_keys = {k.strip() for k in active_raw.split(",") if k.strip()}

    conn = get_connection()
    try:
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)

        cursor.execute(
            "SELECT flag_key, variant FROM ab_assignments WHERE player_id = ?",
            (player_id,)
        )
        assignments = {row[0]: row[1] for row in cursor.fetchall()}

        new_assignments = []
        result_flags    = dict(FLAG_DEFAULTS)

        for key in active_keys:
            if key not in FLAG_DEFINITIONS:
                continue
            if key in assignments:
                result_flags[key] = assignments[key]
            else:
                # Deterministic assignment via hash so the variant is stable
                # even across server restarts and before the first DB write.
                import hashlib as _hl
                h      = int(_hl.md5(f"{player_id}:{key}".encode()).hexdigest(), 16)
                opts   = FLAG_DEFINITIONS[key]
                chosen = opts[h % len(opts)]
                result_flags[key] = chosen
                new_assignments.append((player_id, key, chosen))

        for (pid, fk, var) in new_assignments:
            cursor.execute(_SQL_INSERT_AB_ASSIGNMENT, (pid, fk, var))
            try:
                cursor.execute(
                    "INSERT INTO telemetry_logs "
                    "(player_id, event_name, event_data, session_id) "
                    "VALUES (?, 'ab_assigned', ?, 'server')",
                    (pid, json.dumps({"flag_key": fk, "variant": var}))
                )
            except Exception:
                pass
        if new_assignments:
            conn.commit()
    finally:
        conn.close()

    result_flags["offer_segment"] = _elite.offer_segment(player_id)  # Sprint 7 monetization
    return {"status": "ok", "flags": result_flags}


@app.on_event("startup")
async def startup_event():
    init_db()
    _ps = _push_status()
    print("[push] startup: FCM=%s  APNs=%s"
          % ("on" if _ps["fcm"] else "OFF", "on" if _ps["apns"] else "OFF"))
    # Belt-and-suspenders: the module-level check already raises on import,
    # but this catches any edge case where the guard was bypassed (e.g. test runner patching).
    if os.environ.get("APP_ENV", "development").lower() == "production" and _JWT_INSECURE:
        raise RuntimeError(
            "FATAL: JWT_SECRET is missing or insecure. "
            "Set a strong random secret via the JWT_SECRET environment variable before deploying."
        )
    if os.environ.get("APP_ENV", "development").lower() == "production":
        if _IAP_SANDBOX_MODE:
            raise RuntimeError(
                "FATAL: IAP_SANDBOX or BYPASS_RECEIPT_VALIDATION is enabled in production. "
                "Unset IAP_SANDBOX and BYPASS_RECEIPT_VALIDATION before deploying."
            )
        if not _receipt_validator_available:
            raise RuntimeError(
                "FATAL: receipt_validator.py is missing from the deployment. "
                "All IAP purchases will be rejected until it is installed."
            )
    asyncio.create_task(market_recovery_loop())
    asyncio.create_task(push_reminder_loop())


async def market_recovery_loop():
    while True:
        await asyncio.sleep(6)
        try:
            for lvl in MARKET_CONFIG.keys():
                update_global_market(lvl, impact_type="recovery")
        except asyncio.CancelledError:
            raise  # clean shutdown — do not swallow
        except Exception as _loop_err:
            print(f"[market_recovery_loop] ERROR: {_loop_err} -- retrying in 10 s")
            await asyncio.sleep(10)


# ---------------------------------------------------------------------------
# Retention push sweep.  Runs every few minutes and dispatches:
#   * Piggy-full nudge   -- once the player has been idle past a grace window
#                           (we never push while they're mid-session).
#   * Daily Rush ready   -- when The Rush has reset and they were recently active.
# Sends route through push_service (_push_player), which no-ops when push is not
# configured, so this loop is safe in every environment.
# ---------------------------------------------------------------------------
_PUSH_SWEEP_INTERVAL_SECS = 300
_PIGGY_IDLE_GRACE_MINS    = 40    # wait after fill before nudging (avoid mid-run)
_RUSH_REENGAGE_MAX_DAYS   = 7     # don't nag players who already churned
_PUSH_SWEEP_BATCH         = 200   # cap rows touched per pass


async def push_reminder_loop():
    while True:
        await asyncio.sleep(_PUSH_SWEEP_INTERVAL_SECS)
        try:
            await asyncio.to_thread(_run_push_sweep)
        except asyncio.CancelledError:
            raise  # clean shutdown -- do not swallow
        except Exception as _err:
            print(f"[push_reminder_loop] ERROR: {_err}")


def _run_push_sweep() -> None:
    """Synchronous DB sweep; always invoked via asyncio.to_thread so the event
    loop is never stalled by the blocking DB / push network calls."""
    now       = datetime.datetime.utcnow()
    today_str = now.date().isoformat()
    cutoff_7d = (now.date() - datetime.timedelta(days=_RUSH_REENGAGE_MAX_DAYS)).isoformat()

    piggy_to_push: list = []
    piggy_to_clear: list = []
    rush_to_push: list = []

    conn = get_connection()
    try:
        cursor = conn.cursor()

        # --- 1) Piggy Bank full (candidates stamped by add_to_piggy) ---
        cursor.execute(
            "SELECT player_id, piggy_balance, mastery_state, piggy_full_since "
            "FROM players WHERE piggy_full_notified = 0 AND piggy_full_since IS NOT NULL "
            "LIMIT ?",
            (_PUSH_SWEEP_BATCH,)
        )
        for pid, piggy_balance, mastery_state, full_since in cursor.fetchall():
            cap = get_piggy_cap(mastery_state)
            if int(piggy_balance or 0) < cap:
                piggy_to_clear.append(pid)   # already smashed -> reset, no push
                continue
            try:
                filled_at = datetime.datetime.fromisoformat(str(full_since))
            except (ValueError, TypeError):
                filled_at = now
            if (now - filled_at).total_seconds() >= _PIGGY_IDLE_GRACE_MINS * 60:
                piggy_to_push.append(pid)

        # --- 2) Daily Rush ready (reset since their last play, recently active) ---
        cursor.execute(
            "SELECT player_id FROM players "
            "WHERE last_rush_date IS NOT NULL AND last_rush_date < ? AND last_rush_date >= ? "
            "AND (last_rush_reminder_date IS NULL OR last_rush_reminder_date < ?) "
            "LIMIT ?",
            (today_str, cutoff_7d, today_str, _PUSH_SWEEP_BATCH)
        )
        rush_to_push = [r[0] for r in cursor.fetchall()]

        # Mark state BEFORE sending so a slow/failed send can't cause re-spam.
        for pid in piggy_to_clear:
            cursor.execute(
                "UPDATE players SET piggy_full_since = NULL, piggy_full_notified = 0 "
                "WHERE player_id = ?", (pid,)
            )
        for pid in piggy_to_push:
            cursor.execute(
                "UPDATE players SET piggy_full_notified = 1 WHERE player_id = ?", (pid,)
            )
        for pid in rush_to_push:
            cursor.execute(
                "UPDATE players SET last_rush_reminder_date = ? WHERE player_id = ?",
                (today_str, pid)
            )
        conn.commit()
    finally:
        conn.close()

    # Dispatch OUTSIDE the DB transaction (each _push_player opens its own conn).
    for pid in piggy_to_push:
        res = _push_player(
            pid,
            "Your Piggy Bank is full \U0001F437",
            "Crack it open now before it overflows!",
            {"type": "piggy_full"},
        )
        _log_push(pid, "piggy_full", res)
    for pid in rush_to_push:
        res = _push_player(
            pid,
            "The Rush is ready! ⏳",
            "Beat the clock for 3x Payouts.",
            {"type": "rush_ready"},
        )
        _log_push(pid, "rush_ready", res)


def _log_push(player_id: str, push_type: str, result: dict) -> None:
    """Record a push dispatch in push_log for delivery auditing.  Best-effort."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO push_log (player_id, push_type, tokens, sent, failed, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (player_id, push_type,
             int(result.get("tokens", 0)), int(result.get("sent", 0)),
             int(result.get("failed", 0)), datetime.datetime.utcnow().isoformat())
        )
        conn.commit()
        conn.close()
    except Exception as _e:
        print(f"[push_log] failed to record {push_type} for {player_id}: {_e}")


@app.post("/admin/push_test")
async def admin_push_test(request: Request):
    """Admin-only: send a test push to a player_id.  Guarded by the
    ADMIN_PUSH_SECRET env var (must match the X-Admin-Secret header).
    Returns the transport config + per-token send result for diagnosis."""
    secret = os.environ.get("ADMIN_PUSH_SECRET", "")
    if not secret or request.headers.get("X-Admin-Secret", "") != secret:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        body   = await request.json()
        target = str(body.get("player_id", "")).strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not target:
        raise HTTPException(status_code=400, detail="player_id is required")
    result = _push_player(target, "Hostile Merge",
                          "Admin test push -- delivery check.",
                          {"type": "admin_test"})
    _log_push(target, "admin_test", result)
    return {"status": "ok", "config": _push_status(), "result": result}


@app.post("/player/set_age")
async def set_age(request: Request):
    """Record the COPPA age bracket chosen at the first-launch age gate.
    Body: {"under_13": bool}.  Idempotent; client also persists locally."""
    player_id = extract_player_id(request)
    try:
        body     = await request.json()
        under_13 = 1 if bool(body.get("under_13", False)) else 0
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "UPDATE players SET age_under_13 = ? WHERE player_id = ?",
        (under_13, player_id)
    )
    conn.commit()
    conn.close()
    return {"status": "ok", "age_under_13": under_13}


@app.post("/quests/claim")
async def claim_quest(request: Request, quest_id: str):
    player_id = extract_player_id(request)
    if quest_id not in DAILY_QUEST_GEM_REWARDS:
        return {"status": "error", "message": "Unknown quest_id"}
    purple_gems = DAILY_QUEST_GEM_REWARDS[quest_id]
    if purple_gems <= 0:
        return {"status": "ok", "gems_awarded": 0}
    today_day = str(int(datetime.datetime.utcnow().timestamp() // 86400))
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    try:
        cursor.execute(
            "INSERT INTO quest_claims (player_id, quest_id, claim_day) VALUES (?, ?, ?)",
            (player_id, quest_id, today_day)
        )
    except Exception:
        conn.close()
        return {"status": "error", "message": "Quest already claimed today"}
    today_str_q = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    actual_quest_gems = _award_free_gems(cursor, player_id, purple_gems, today_str_q)
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    row = cursor.fetchone()
    new_gems = int(row[0]) if row and row[0] is not None else 0
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {
        "status":          "success",
        "gems_awarded":    actual_quest_gems,
        "new_gems_balance": new_gems,
        "cap_reached":     actual_quest_gems < purple_gems,
    }


@app.post("/achievements/claim")
async def claim_achievement_tier(request: Request, ach_id: str, tier: int):
    player_id = extract_player_id(request)
    if ach_id not in ACHIEVEMENT_CONFIG:
        return {"status": "error", "message": "Unknown achievement"}
    gems_list = ACHIEVEMENT_CONFIG[ach_id].get("gems_reward", [0])
    if tier < 0 or tier >= len(gems_list):
        return {"status": "error", "message": "Invalid tier"}
    gems_award = int(gems_list[tier])
    if gems_award <= 0:
        return {"status": "ok", "gems_awarded": 0}
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    # Server-side eligibility validation for known achievement types
    validation = _ACHIEVEMENT_VALIDATION.get(ach_id)
    if validation:
        target = validation["targets"][tier]
        cursor.execute(_ACH_STAT_SELECT_SQL[validation["stat_col"]], (player_id,))
        stat_row = cursor.fetchone()
        stat_val = int(stat_row[0]) if stat_row and stat_row[0] is not None else 0
        if stat_val < target:
            conn.close()
            return {"status": "error", "message": "Achievement target not yet reached"}
    try:
        cursor.execute(
            "INSERT INTO achievement_claims (player_id, ach_id, tier) VALUES (?, ?, ?)",
            (player_id, ach_id, tier)
        )
    except Exception:
        conn.close()
        return {"status": "error", "message": "Achievement tier already claimed"}
    today_str_a = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    actual_ach_gems = _award_free_gems(cursor, player_id, gems_award, today_str_a)
    xp_info_a = _grant_xp(cursor, player_id, 15)
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    row = cursor.fetchone()
    new_gems = int(row[0]) if row and row[0] is not None else 0
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    resp_a = {
        "status":           "success",
        "gems_awarded":     actual_ach_gems,
        "new_gems_balance": new_gems,
        "cap_reached":      actual_ach_gems < gems_award,
    }
    resp_a.update(xp_info_a)
    return resp_a


@app.post("/prices")
async def get_adjusted_prices(board: List[int] = Body(...)):
    prices = calculate_adjusted_prices(board)
    return {
        "status": "success",
        "prices": {f"Level_{k}": v for k, v in prices.items()}
    }



@app.post("/tools/prices")
async def get_tool_prices(payload: Dict[str, Any] = Body(...)):
    board              = payload.get("board", [])
    elapsed_time       = float(payload.get("elapsed_time", 0.0))
    board_rows         = int(payload.get("board_rows", 0))
    board_cols         = int(payload.get("board_cols", 0))
    max_unlocked_tier  = int(payload.get("max_unlocked_tier", 1))

    if not isinstance(board, list):
        return {"status": "error", "message": "board must be a list"}

    costs = calculate_tool_costs(board, elapsed_time, board_rows, board_cols, max_unlocked_tier)

    return {
        "status": "success",
        "costs": costs
    }


@app.post("/player/add_piggy")
async def add_to_piggy(request: Request, amount: int):
    player_id = extract_player_id(request)
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be positive")
    # Rush Mode (Ember Blitz) plays a dense 6x6 board where multiple cashout
    # lines can resolve in a single move -- piggy_add = 50 * number_of_lines
    # + merge delta legitimately exceeds the old 150 cap (e.g. 4 simultaneous
    # lines = 200), which previously bounced as HTTP 400. Raised to 500 to
    # cover Rush's higher combo density without removing the anti-cheat cap.
    if amount > 500:
        raise HTTPException(status_code=400, detail="amount exceeds per-call maximum of 500")
    # Merges (and combo cashouts) legitimately fire several add_piggy calls
    # per second during a chain -- the old 1-call-per-3s limit silently
    # dropped most of them (HTTP 429, "request dropped, no retry" client-side)
    # while the client had ALREADY applied its optimistic local credit. The
    # next call to land would then return the server's true (much lower)
    # total, snapping the displayed balance back down -- the "increases then
    # drops" bug. 30 calls / 10s comfortably covers real merge cadence
    # (per-call amount is still hard-capped at 150) without opening the door
    # to abuse.
    if not _check_burst_limit(player_id, "add_piggy", 30, 10.0):
        raise HTTPException(
            status_code=429,
            detail={"status": "rate_limited",
                    "message": "Too many add_piggy requests -- please slow down."}
        )
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT piggy_balance, mastery_state FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    current_piggy = int(row[0]) if row and row[0] is not None else 0
    cap = get_piggy_cap(row[1] if row else None)
    if current_piggy >= cap:
        conn.close()
        return {"status": "capped", "piggy_balance": current_piggy, "cap": cap}
    new_piggy = min(current_piggy + amount, cap)
    cursor.execute(
        "UPDATE players SET piggy_balance = ? WHERE player_id = ?",
        (new_piggy, player_id)
    )
    # Stamp the moment the bank FILLS (transition into cap).  We do NOT push here:
    # the piggy fills mid-run, and "come crack it open" while the player is already
    # playing is bad UX (and risks Play policy on misleading notifications).  The
    # reminder sweep dispatches the push once the player has gone idle.
    if current_piggy < cap and new_piggy >= cap:
        cursor.execute(
            "UPDATE players SET piggy_full_since = ?, piggy_full_notified = 0 "
            "WHERE player_id = ?",
            (datetime.datetime.utcnow().isoformat(), player_id)
        )
    conn.commit()
    conn.close()
    return {"status": "success", "piggy_balance": new_piggy, "cap": cap}



@app.get("/rush/status")
async def get_rush_status(request: Request):
    """
    Returns whether The Rush daily challenge is available for this player today
    and how many seconds remain until the next midnight reset (UTC).

    Response:
        is_available          bool -- True if player has not played The Rush today.
        seconds_until_midnight int  -- Client uses this to drive a local countdown
                                       without polling. Re-fetch on expiry.
        last_rush_date        str | None -- ISO date of last attempt, for debugging.
    Always 200; never reveals private data.
    """
    player_id = extract_player_id(request)
    now_utc   = datetime.datetime.utcnow()
    today_str = now_utc.date().isoformat()

    # Seconds from right now until the next UTC midnight.
    midnight_utc        = datetime.datetime(now_utc.year, now_utc.month, now_utc.day) \
                          + datetime.timedelta(days=1)
    seconds_until_reset = max(0, int((midnight_utc - now_utc).total_seconds()))

    conn = get_connection()
    try:
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        cursor.execute(
            "SELECT last_rush_date FROM players WHERE player_id = ?",
            (player_id,)
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    last_rush_date = row[0] if row else None
    is_available   = (last_rush_date is None) or (last_rush_date != today_str)

    return {
        "is_available":           is_available,
        "seconds_until_midnight": seconds_until_reset,
        "last_rush_date":         last_rush_date,
    }


@app.post("/rush/start")
async def start_rush(request: Request):
    """
    Called the moment the player enters The Rush scene.
    Stamps last_rush_date = today (UTC) so the button locks immediately.
    Returns the same payload as /rush/status so the client can sync in one round-trip.
    Idempotent: safe to call twice (stamps the same date, returns already-played).
    """
    player_id = extract_player_id(request)
    now_utc   = datetime.datetime.utcnow()
    today_str = now_utc.date().isoformat()

    midnight_utc        = datetime.datetime(now_utc.year, now_utc.month, now_utc.day) \
                          + datetime.timedelta(days=1)
    seconds_until_reset = max(0, int((midnight_utc - now_utc).total_seconds()))

    conn = get_connection()
    try:
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        cursor.execute(
            "UPDATE players SET last_rush_date = ? WHERE player_id = ?",
            (today_str, player_id)
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "is_available":           False,
        "seconds_until_midnight": seconds_until_reset,
        "last_rush_date":         today_str,
    }


@app.post("/player/claim_daily")
async def claim_daily_reward(request: Request):
    player_id     = extract_player_id(request)
    today_utc     = datetime.datetime.utcnow().date().isoformat()
    yesterday_utc = (datetime.datetime.utcnow().date() - datetime.timedelta(days=1)).isoformat()

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT login_streak, last_login_date FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    streak    = int(row[0]) if row[0] is not None else 0
    last_date = str(row[1]) if row[1] is not None else ""

    if last_date == today_utc:
        conn.close()
        return {"status": "error", "message": "already_claimed"}

    day_before_yesterday_utc = (datetime.datetime.utcnow().date() - datetime.timedelta(days=2)).isoformat()

    # Consecutive day -> increment.  Missed exactly ONE day -> grace period, still increment.
    # Missed two or more consecutive days -> reset to 1.
    # After a full 7-day cycle the streak wraps back to Day 1.
    if last_date in (yesterday_utc, day_before_yesterday_utc):
        if streak >= 7:
            new_streak = 1
        else:
            new_streak = streak + 1
    else:
        new_streak = 1

    reward     = DAILY_REWARDS[new_streak]
    gold_grant = reward["gold"]
    gems_grant = reward["gems"]
    free_spin  = reward["free_spin"]
    today_str  = today_utc  # already "%Y-%m-%d" isoformat

    # Update login streak and bump login_days + best_login_streak
    cursor.execute(
        "SELECT login_days, best_login_streak FROM players WHERE player_id = ?",
        (player_id,)
    )
    _ld_row = cursor.fetchone()
    new_login_days       = (int(_ld_row[0]) if _ld_row and _ld_row[0] is not None else 0) + 1
    new_best_streak      = max(int(_ld_row[1]) if _ld_row and _ld_row[1] is not None else 0, new_streak)
    cursor.execute(
        """UPDATE players SET
               total_money        = total_money + ?,
               login_streak       = ?,
               last_login_date    = ?,
               daily_doubled      = 0,
               login_days         = ?,
               best_login_streak  = ?
           WHERE player_id = ?""",
        (gold_grant, new_streak, today_utc, new_login_days, new_best_streak, player_id)
    )
    actual_gems = _award_free_gems(cursor, player_id, gems_grant, today_str)
    if free_spin:
        cursor.execute(
            "UPDATE players SET last_free_spin_time = NULL WHERE player_id = ?",
            (player_id,)
        )

    xp_info_d = _grant_xp(cursor, player_id, 25)

    cursor.execute(
        "SELECT total_money, gems_balance, seen_day7_offer, first_iap_done FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    _seen_day7       = int(updated[2]) if updated[2] is not None else 0
    _first_iap       = int(updated[3]) if updated[3] is not None else 0
    show_day7_offer  = (new_streak == 7 and _seen_day7 == 0 and _first_iap == 0)

    resp_d = {
        "status":           "success",
        "day":              new_streak,
        "streak":           new_streak,
        "gold_gained":      gold_grant,
        "gems_gained":      actual_gems,
        "gems_awarded":     actual_gems,
        "gem_cap_hit":      actual_gems < gems_grant,
        "free_spin":        free_spin,
        "label":            reward["label"],
        "new_balance":      int(updated[0]),
        "new_gems":         int(updated[1]),
        "show_day7_offer":  show_day7_offer,
    }
    resp_d.update(xp_info_d)
    return resp_d


@app.post("/player/mark_day7_offer_seen")
def mark_day7_offer_seen(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE players SET seen_day7_offer = 1 WHERE player_id = ?",
        (player_id,)
    )
    conn.commit()
    conn.close()
    return {"status": "success"}


@app.post("/shop/vault_champion_bundle")
def purchase_vault_champion_bundle(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute(
        "SELECT seen_day7_offer, first_iap_done FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    seen_day7  = int(row[0]) if row and row[0] is not None else 0
    first_iap  = int(row[1]) if row and row[1] is not None else 0

    gems_granted = 350
    gold_granted = 5_000

    cursor.execute("""
        UPDATE players
        SET total_money    = total_money    + ?,
            gems_balance   = gems_balance   + ?,
            seen_day7_offer = 1,
            first_iap_done  = 1
        WHERE player_id = ?
    """, (gold_granted, gems_granted, player_id))

    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":       "success",
        "gems_granted": gems_granted,
        "gold_granted": gold_granted,
        "new_balance":  int(updated[0]),
        "new_gems":     int(updated[1]),
        "already_owned": bool(seen_day7 or first_iap),
    }


@app.post("/player/open_piggy")
async def open_piggy(request: Request):
    player_id = extract_player_id(request)
    if not _check_burst_limit(player_id, "open_piggy", 5, 60.0):
        raise HTTPException(status_code=429, detail={"status": "rate_limited", "message": "Too many requests."})
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT gems_balance, piggy_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems  = int(row[0]) if row[0] is not None else 0
    piggy = int(row[1]) if row[1] is not None else 0
    if piggy <= 0:
        conn.close()
        return {"status": "error", "message": "Piggy Bank is empty"}
    required_cost = get_piggy_gem_cost(piggy)
    if gems < required_cost:
        conn.close()
        return {"status": "error", "message": f"Need {required_cost} Gems to break the Piggy (balance-scaled)"}
    cursor.execute(
        """UPDATE players SET
               total_money   = total_money + ?,
               gems_balance  = gems_balance - ?,
               piggy_balance = 0
           WHERE player_id = ?""",
        (piggy, required_cost, player_id)
    )
    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    return {
        "status":               "success",
        "vault_dollars_gained": piggy,
        "gem_cost_paid":        required_cost,
        "new_balance":          int(updated[0]),
        "new_gems":             int(updated[1]),
        "new_piggy":            0
    }


# Kept for backward compatibility. Your current main_game no longer needs right-click selling.
@app.post("/sell")
async def sell_item(request: Request, item_level: int, board: List[int] = Body(...)):
    player_id = extract_player_id(request)
    current_prices = calculate_adjusted_prices(board)

    if item_level not in current_prices:
        return {"status": "error", "message": "Invalid item level"}

    profit = current_prices[item_level]

    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute(
        "UPDATE players SET total_money = total_money + ? WHERE player_id = ?",
        (profit, player_id)
    )

    cursor.execute(
        "INSERT INTO sales_log (seller, item_level, profit_made) VALUES (?, ?, ?)",
        (player_id, item_level, profit)
    )

    cursor.execute("SELECT total_money FROM players WHERE player_id = ?", (player_id,))
    balance = cursor.fetchone()[0]

    conn.commit()
    conn.close()

    update_global_market(item_level, impact_type="sale", is_player_sale=True)

    return {
        "status": "BANKRUPT" if balance < 0 else "SUCCESS",
        "profit": profit,
        "new_balance": balance
    }


@app.post("/cashout")
async def cashout(request: Request):
    """
    SERVER-DERIVED cashout payout (replaces client-declared deposits for the main
    gold source).  The client reports the EVENT FACTS; the server computes the
    reward from its own board-adjusted market prices + multipliers, so a modded
    client cannot inflate the amount.

    Body: {
      "board":        [int],   # PRE-CLEAR board snapshot (for board-adjusted pricing)
      "tier":         int,     # cashout_target_tier
      "line_lengths": [int],   # length of each cashout line (one entry per line)
      "golden_tile":  bool,    # whether a golden tile was part of the cashout
      "combo_value":  int      # cascade combo counter (-> combo bonus pct)
    }
    Mirrors node_2d.calculate_cashout_reward + the combo/golden/bonus math exactly.
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    if not _check_burst_limit(player_id, "cashout", 20, 60.0):
        raise HTTPException(
            status_code=429,
            detail={"status": "rate_limited", "message": "Too many cashouts -- slow down."}
        )

    try:
        body         = await request.json()
        board        = body.get("board", [])
        tier         = int(body.get("tier", 0))
        line_lengths = body.get("line_lengths", [])
        golden       = bool(body.get("golden_tile", False))
        combo_value  = int(body.get("combo_value", 1))
        unique_tiles = body.get("unique_tiles", None)
        unique_tiles = int(unique_tiles) if unique_tiles is not None else None
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # --- Input validation ---
    if not isinstance(board, list) or not (0 < len(board) <= 64):
        raise HTTPException(status_code=400, detail="invalid board")
    if not isinstance(line_lengths, list) or not (0 < len(line_lengths) <= _MAX_CASHOUT_LINES):
        raise HTTPException(status_code=400, detail="invalid line_lengths")
    if not (1 <= tier <= 99):
        raise HTTPException(status_code=400, detail="invalid tier")
    safe_lengths = []
    for L in line_lengths:
        try:
            Li = int(L)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="invalid line length")
        if not (2 <= Li <= _MAX_CASHOUT_LINE_LEN):
            raise HTTPException(status_code=400, detail="invalid line length")
        safe_lengths.append(Li)

    # Anti-cheat: the board must actually hold enough tier (or golden) tiles to back
    # the claimed lines.  Bounds fabricated multi-line claims without a full re-detect.
    # NOTE: combo lines (row/column/diagonal) can cross at shared cells, so summing
    # line_lengths over-counts overlaps -- use the client's de-duplicated cleared-cell
    # count when present (it mirrors the server-side unique-index set exactly), and
    # fall back to the longest single line (a safe lower bound) for older clients.
    if unique_tiles is not None and 0 < unique_tiles <= sum(safe_lengths):
        needed = unique_tiles
    else:
        needed = max(safe_lengths)
    have = board.count(tier) + (board.count(GOLDEN_TILE_VALUE) if golden else 0)
    if have < needed:
        logging.warning("ANTICHEAT cashout_claim_exceeds_board player=%s need=%s have=%s",
                        player_id, needed, have)
        raise HTTPException(status_code=400, detail="claim exceeds board tiles")

    # --- Authoritative, board-adjusted price (same fn the client polls via /prices) ---
    prices     = calculate_adjusted_prices(board)
    base_price = max(1, int(prices.get(tier, 1)))

    # --- Reproduce node_2d payout math EXACTLY ---
    def _length_mult(n: int) -> float:
        if n == 4: return 1.6
        if n == 5: return 2.4
        if n >= 6: return 3.5
        return 1.0

    base_reward = 0
    for L in safe_lengths:
        base_reward += int(base_price * L * _length_mult(L))

    num_lines        = len(safe_lengths)
    combo_multiplier = 3 if num_lines >= 3 else (2 if num_lines == 2 else 1)
    total            = base_reward * combo_multiplier
    if golden:
        total *= GOLDEN_TILE_CASHOUT_MULT

    if   combo_value >= 7: bonus_pct = 0.15
    elif combo_value >= 4: bonus_pct = 0.10
    elif combo_value >= 2: bonus_pct = 0.05
    else:                  bonus_pct = 0.0
    if bonus_pct > 0.0:
        total = int(total * (1.0 + bonus_pct))

    total = max(0, total)
    if total > _MAX_CASHOUT_PAYOUT:
        logging.warning("ANTICHEAT cashout_over_cap player=%s total=%s", player_id, total)
        total = _MAX_CASHOUT_PAYOUT

    # --- Credit + Vault Pass 1.5x (same rule as /bank/deposit) ---
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    vault_pass_bonus = 0
    if total > 0:
        cursor.execute(
            "SELECT vault_pass_active, vault_pass_expiry FROM players WHERE player_id = ?",
            (player_id,)
        )
        _vp = cursor.fetchone()
        _vp_on  = bool(int(_vp[0]) if _vp and _vp[0] is not None else 0)
        _vp_exp = str(_vp[1]) if _vp and _vp[1] is not None else ""
        if _vp_on and _vp_exp:
            try:
                if datetime.datetime.utcnow() < datetime.datetime.fromisoformat(_vp_exp):
                    vault_pass_bonus = int(total * 0.5)
                    total += vault_pass_bonus
            except (ValueError, OverflowError):
                pass

    cursor.execute(
        "UPDATE players SET total_money = total_money + ? WHERE player_id = ?",
        (total, player_id)
    )
    cursor.execute("SELECT total_money FROM players WHERE player_id = ?", (player_id,))
    new_balance = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":           "success",
        "granted":          total,
        "vault_pass_bonus": vault_pass_bonus,
        "new_balance":      new_balance,
    }


@app.post("/bank/deposit")
async def deposit_money(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    # 20 deposits per 60-second rolling window per player.
    # Legitimate gameplay (cashout + tool-spend) peaks at ~10 calls/min on a very
    # active board; 20 gives a 2x headroom before blocking.
    if not _check_burst_limit(player_id, "deposit", 20, 60.0):
        raise HTTPException(
            status_code=429,
            detail={"status": "rate_limited",
                    "message": "Too many deposit requests -- please slow down."}
        )
    # Amount now lives in the JSON body so it can be HMAC-signed.
    # The body is cached by Starlette after _verify_financial_signature read it.
    raw_body = await request.body()
    try:
        body   = json.loads(raw_body) if raw_body else {}
        amount = int(body.get("amount", 0))
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # Anti-injection ceiling: positive deposits only.  Negative amounts are
    # tool-spend withdrawals (server-priced on the client) and are left alone --
    # a cheater gains nothing by removing their own gold.
    if amount > _MAX_DEPOSIT_PER_CALL:
        logging.warning(
            "ANTICHEAT deposit_over_cap player=%s amount=%s cap=%s",
            player_id, amount, _MAX_DEPOSIT_PER_CALL,
        )
        raise HTTPException(
            status_code=400,
            detail={"status": "amount_rejected",
                    "message": "Deposit exceeds the per-transaction limit."}
        )

    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    # Vault Pass 1.5x gold multiplier on all positive cashout deposits.
    vault_pass_bonus = 0
    if amount > 0:
        cursor.execute(
            "SELECT vault_pass_active, vault_pass_expiry FROM players WHERE player_id = ?",
            (player_id,)
        )
        _vp_row = cursor.fetchone()
        _vp_on  = bool(int(_vp_row[0]) if _vp_row and _vp_row[0] is not None else 0)
        _vp_exp = str(_vp_row[1]) if _vp_row and _vp_row[1] is not None else ""
        if _vp_on and _vp_exp:
            try:
                _now_check = datetime.datetime.utcnow()
                _exp_check = datetime.datetime.fromisoformat(_vp_exp)
                if _now_check < _exp_check:
                    _bonus_amount = int(amount * 0.5)
                    vault_pass_bonus = _bonus_amount
                    amount = amount + _bonus_amount
            except (ValueError, OverflowError):
                pass

    cursor.execute(
        "UPDATE players SET total_money = total_money + ? WHERE player_id = ?",
        (amount, player_id)
    )

    # First-cashout-of-the-day bonus: +5 gems, positive deposits only.
    first_cashout_bonus_gems = 0
    if amount > 0:
        today_dep = datetime.datetime.utcnow().date().isoformat()
        cursor.execute(
            "SELECT last_first_cashout_bonus_date FROM players WHERE player_id = ?",
            (player_id,)
        )
        bonus_row = cursor.fetchone()
        last_bonus_date = str(bonus_row[0]) if bonus_row and bonus_row[0] is not None else ""
        if last_bonus_date != today_dep:
            first_cashout_bonus_gems = 5
            cursor.execute(
                "UPDATE players SET gems_balance = COALESCE(gems_balance, 0) + 5, "
                "last_first_cashout_bonus_date = ? WHERE player_id = ?",
                (today_dep, player_id)
            )

    cursor.execute("SELECT total_money FROM players WHERE player_id = ?", (player_id,))
    balance = cursor.fetchone()[0]

    # XP grant: +1 per 100 gold deposited, capped at 50 XP per call to prevent abuse.
    xp_info: Dict[str, Any] = {}
    if amount > 0:
        deposit_xp = min(amount // 100, 50)
        if deposit_xp > 0:
            xp_info = _grant_xp(cursor, player_id, deposit_xp)

    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    resp = {"status": "success", "amount": amount, "new_balance": balance}
    if first_cashout_bonus_gems > 0:
        resp["first_cashout_bonus_gems"] = first_cashout_bonus_gems
    if vault_pass_bonus > 0:
        resp["vault_pass_bonus"] = vault_pass_bonus
    resp.update(xp_info)
    return resp


@app.get("/bank/balance")
def get_balance(request: Request):
    player_id  = extract_player_id(request)
    _cache_key = f"bal:{player_id}"

    # --- Redis cache read (5-second TTL) ------------------------------------
    if _REDIS is not None:
        try:
            _cached = _REDIS.get(_cache_key)
            if _cached:
                return json.loads(_cached)
        except Exception:
            pass  # Redis fault: fall through to DB

    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    # FOR UPDATE acquires a row-level lock in PostgreSQL to prevent concurrent
    # one-time-bonus double-credits.  SQLite WAL does not support this syntax.
    _lock_clause = " FOR UPDATE" if _USE_POSTGRES else ""
    cursor.execute(f"""
        SELECT total_money, max_unlocked_tier, board_stage, gems_balance, piggy_balance,
               active_modifier, last_free_spin_time, login_streak, last_login_date,
               unlocked_golden, unlocked_catalyst, has_insurance, unlocked_wildcard,
               vault_slot_0_unlocked, vault_slot_1_unlocked, vault_slot_0_gem, vault_slot_1_gem,
               lucky_drop_lvl, piggy_mastery_lvl, tool_discount_lvl,
               pending_modifiers, mastery_state, last_session_time,
               has_seen_starter_pack, active_cosmetic_id, last_welcome_back_date,
               vault_pass_active, vault_pass_expiry, shards_balance, last_vault_pass_drip,
               is_rescue_active, rescue_active_since,
               install_date,
               boost_active_until, boost_type,
               total_runs
        FROM players
        WHERE player_id = ?{_lock_clause}
    """, (player_id,))

    row = cursor.fetchone()

    # ------------------------------------------------------------------
    # Setup shared values used by both welcome-back and idle-piggy blocks.
    # ------------------------------------------------------------------
    now_utc             = datetime.datetime.utcnow()
    today_bal           = now_utc.date().isoformat()
    board_stage_idx_row = int(row[2]) if row[2] is not None else 0
    idle_piggy_gold     = 0
    current_piggy       = int(row[4]) if row[4] is not None else 0
    mastery_state_js    = row[21] if row[21] is not None else "{}"
    piggy_cap           = get_piggy_cap(mastery_state_js)
    last_session_str    = row[22]

    # ------------------------------------------------------------------
    # Welcome-Back Reward: 3-30 days absence -> days_away * 200 gold.
    # Must run BEFORE last_session_time is updated so the gap is accurate.
    # ------------------------------------------------------------------
    welcome_back_gold = 0
    welcome_back_days = 0
    last_wb_date      = str(row[25]) if row[25] is not None else ""
    if last_session_str is not None and last_wb_date != today_bal:
        try:
            last_sess_dt_wb = datetime.datetime.fromisoformat(str(last_session_str))
            days_away = (now_utc.date() - last_sess_dt_wb.date()).days
            if 3 <= days_away <= 30:
                welcome_back_gold = days_away * 200
                welcome_back_days = days_away
                cursor.execute(
                    "UPDATE players SET total_money = total_money + ?, last_welcome_back_date = ? WHERE player_id = ?",
                    (welcome_back_gold, today_bal, player_id)
                )
        except (ValueError, OverflowError):
            pass

    # ------------------------------------------------------------------
    # Idle Piggy Bank: award passive gold for time spent offline.
    # Rate scales with board stage: 100 * max(1, board_stage) gold/hour.
    # Capped at IDLE_PIGGY_MAX_HOURS and the player's dynamic piggy cap.
    # Always update last_session_time so the next login measures correctly.
    # ------------------------------------------------------------------
    stage_multiplier = max(1, board_stage_idx_row)
    idle_gold_rate   = IDLE_PIGGY_GOLD_PER_HOUR * stage_multiplier

    if last_session_str is not None and current_piggy < piggy_cap:
        try:
            last_sess_dt    = datetime.datetime.fromisoformat(str(last_session_str))
            elapsed_secs    = max(0.0, (now_utc - last_sess_dt).total_seconds())
            offline_hours   = min(int(elapsed_secs // 3600), IDLE_PIGGY_MAX_HOURS)
            raw_idle_gold   = offline_hours * idle_gold_rate
            idle_piggy_gold = min(raw_idle_gold, piggy_cap - current_piggy)
        except (ValueError, OverflowError):
            idle_piggy_gold = 0

    if idle_piggy_gold > 0:
        current_piggy += idle_piggy_gold
        cursor.execute(
            "UPDATE players SET piggy_balance = ?, last_session_time = ? WHERE player_id = ?",
            (current_piggy, now_utc.isoformat(), player_id)
        )
    else:
        cursor.execute(
            "UPDATE players SET last_session_time = ? WHERE player_id = ?",
            (now_utc.isoformat(), player_id)
        )

    # ------------------------------------------------------------------
    # Vault Pass daily drip: +10 Gems once per calendar day while active.
    # row[26]=vault_pass_active, row[27]=vault_pass_expiry,
    # row[28]=shards_balance,    row[29]=last_vault_pass_drip
    # ------------------------------------------------------------------
    vault_pass_daily_gems = 0
    vp_raw_active  = bool(int(row[26]) if row[26] is not None else 0)
    vp_expiry_str  = str(row[27]) if row[27] is not None else ""
    vp_days_left   = 0
    vp_is_active   = False
    if vp_raw_active and vp_expiry_str:
        try:
            vp_expiry_dt = datetime.datetime.fromisoformat(vp_expiry_str)
            if now_utc < vp_expiry_dt:
                vp_is_active = True
                vp_days_left = max(0, (vp_expiry_dt.date() - now_utc.date()).days)
                last_vp_drip = str(row[29]) if row[29] is not None else ""
                if last_vp_drip != today_bal:
                    vault_pass_daily_gems = 10
                    cursor.execute(
                        "UPDATE players SET gems_balance = gems_balance + 10, "
                        "last_vault_pass_drip = ? WHERE player_id = ?",
                        (today_bal, player_id)
                    )
            else:
                # Pass expired -- persist the deactivation so subsequent reads
                # don't re-evaluate the expiry date on every call.
                cursor.execute(
                    "UPDATE players SET vault_pass_active = 0 WHERE player_id = ?",
                    (player_id,)
                )
        except (ValueError, OverflowError):
            pass

    # ------------------------------------------------------------------
    # Rescue flag auto-expiry: if is_rescue_active was set but the server
    # restarted before the in-process asyncio task could clear it, the flag
    # stays stuck forever.  Clear it here if >30 s have elapsed since it
    # was opened (rescue_active_since, row[31]).
    # row[30]=is_rescue_active, row[31]=rescue_active_since
    # ------------------------------------------------------------------
    _rescue_raw   = int(row[30]) if row[30] is not None else 0
    _rescue_since = str(row[31]) if row[31] is not None else ""
    if _rescue_raw == 1 and _rescue_since:
        try:
            _rescue_dt  = datetime.datetime.fromisoformat(_rescue_since)
            _rescue_age = (now_utc - _rescue_dt).total_seconds()
            if _rescue_age > 30:
                cursor.execute(
                    "UPDATE players SET is_rescue_active = 0, rescue_active_since = NULL "
                    "WHERE player_id = ?",
                    (player_id,)
                )
                _rescue_raw = 0
        except (ValueError, OverflowError):
            pass

    # Read XP data before closing connection
    cursor.execute(
        "SELECT player_xp, player_level FROM players WHERE player_id = ?",
        (player_id,)
    )
    _xp_row    = cursor.fetchone()
    _bal_xp    = int(_xp_row[0]) if _xp_row and _xp_row[0] is not None else 0
    _bal_level = int(_xp_row[1]) if _xp_row and _xp_row[1] is not None else 1
    _bal_xp_bar = _xp_bar_info(_bal_xp, _bal_level)

    conn.commit()
    conn.close()

    # row[32] = install_date   row[33] = boost_active_until   row[34] = boost_type
    install_date_str   = str(row[32]) if row[32] is not None else None
    days_since_install = 0
    if install_date_str:
        try:
            install_dt = datetime.datetime.fromisoformat(install_date_str)
            if install_dt.tzinfo is None:
                install_dt = install_dt.replace(tzinfo=datetime.timezone.utc)
            days_since_install = max(0, (now_utc.replace(tzinfo=datetime.timezone.utc) - install_dt).days)
        except (ValueError, TypeError):
            pass

    _boost_until_str = str(row[33]) if row[33] is not None else ""
    _boost_type_str  = str(row[34]) if row[34] is not None else ""
    _active_boost    = None
    _boost_secs_left = 0
    if _boost_until_str and _boost_type_str:
        try:
            _boost_exp = datetime.datetime.fromisoformat(_boost_until_str)
            if _boost_exp.tzinfo is None:
                _boost_exp = _boost_exp.replace(tzinfo=datetime.timezone.utc)
            _now_aware = now_utc.replace(tzinfo=datetime.timezone.utc)
            if _now_aware < _boost_exp:
                _active_boost    = _boost_type_str
                _boost_secs_left = int((_boost_exp - _now_aware).total_seconds())
        except (ValueError, OverflowError):
            pass

    stage = get_stage(int(row[2]))
    cashout_target_tier = int(row[1])

    today_utc = now_utc.date().isoformat()
    free_spin_available = True
    last_spin = row[6]
    if last_spin is not None:
        try:
            last_date = datetime.datetime.fromisoformat(str(last_spin)).date().isoformat()
            free_spin_available = (last_date < today_utc)
        except Exception:
            free_spin_available = True

    login_streak       = int(row[7]) if row[7] is not None else 0
    last_login_date_v  = str(row[8]) if row[8] is not None else ""
    daily_available    = (last_login_date_v != today_utc)

    max_tier        = int(row[1]) if row[1] is not None else 2
    board_stage_idx = int(row[2]) if row[2] is not None else 0

    # Next purchasable tier (one entry is enough for affordability checks)
    next_tier = max_tier + 1
    tier_catalog_mini = []
    if next_tier in TIER_UNLOCK_COSTS:
        tier_catalog_mini.append({
            "tier":   next_tier,
            "cost":   TIER_UNLOCK_COSTS[next_tier],
            "status": "available",
        })

    # Next purchasable board stage (one entry is enough for affordability checks)
    next_board_idx = board_stage_idx + 1
    board_catalog_mini = []
    if next_board_idx < len(BOARD_STAGES):
        ns = BOARD_STAGES[next_board_idx]
        board_catalog_mini.append({
            "stage":  next_board_idx,
            "rows":   ns["rows"],
            "cols":   ns["cols"],
            "cost":   ns["cost"],
            "status": "available",
        })

    _bal_result = {
        "balance":             row[0],
        "total_money":         int(row[0]) if row[0] is not None else 0,
        "max_unlocked_tier":   row[1],
        "board_stage":         row[2],
        "board_rows":          stage["rows"],
        "board_cols":          stage["cols"],
        "cashout_target_tier": cashout_target_tier,
        "gems_balance":        int(row[3]) if row[3] is not None else 10,
        "piggy_balance":       current_piggy,
        # THE TREE-scaled ceiling (econ_piggy_cap mastery level -> get_piggy_cap);
        # ships on every balance response so the gameplay HUD adopts the same
        # dynamic cap the main-menu Piggy already gets via /piggy/state, instead
        # of clamping to a hardcoded Tier-0 default.
        "piggy_cap":           piggy_cap,
        "idle_piggy_gold":     idle_piggy_gold,
        "active_modifier":     str(row[5]) if row[5] is not None else "",
        "free_spin_available": free_spin_available,
        "extra_time":          0,
        "login_streak":        login_streak,
        "daily_available":     daily_available,
        "unlocked_golden":       int(row[9])  if row[9]  is not None else 0,
        "unlocked_catalyst":     int(row[10]) if row[10] is not None else 0,
        "has_insurance":         int(row[11]) if row[11] is not None else 0,
        "unlocked_wildcard":     int(row[12]) if row[12] is not None else 0,
        "vault_slot_0_unlocked": int(row[13]) if row[13] is not None else 0,
        "vault_slot_1_unlocked": int(row[14]) if row[14] is not None else 0,
        "vault_slot_0_gem":      int(row[15]) if row[15] is not None else 0,
        "vault_slot_1_gem":      int(row[16]) if row[16] is not None else 0,
        "lucky_drop_lvl":        int(row[17]) if row[17] is not None else 0,
        "piggy_mastery_lvl":     int(row[18]) if row[18] is not None else 0,
        "tool_discount_lvl":     int(row[19]) if row[19] is not None else 0,
        "pending_modifiers":     json.loads(row[20]) if row[20] else {},
        "has_seen_starter_pack":  int(row[23]) if row[23] is not None else 0,
        "active_cosmetic_id":     str(row[24]) if row[24] is not None else "",
        "tier_catalog":           tier_catalog_mini,
        "board_catalog":          board_catalog_mini,
        "welcome_back_gold":      welcome_back_gold,
        "welcome_back_days":      welcome_back_days,
        "idle_gold_rate":         idle_gold_rate,
        "vault_pass_active":      vp_is_active,
        "vault_pass_days_left":   vp_days_left,
        "vault_pass_daily_gems":  vault_pass_daily_gems,
        "shards_balance":         int(row[28]) if row[28] is not None else 0,
        "pass_tier":              next(
            (i - 1 for i, t in enumerate(VAULT_PASS_TIERS)
             if t > (int(row[28]) if row[28] is not None else 0)),
            len(VAULT_PASS_TIERS) - 1
        ),
        "is_rescue_active":       _rescue_raw,
        "days_since_install":     days_since_install,
        "active_boost":           _active_boost,
        "boost_seconds_left":     _boost_secs_left,
        "player_xp":              _bal_xp,
        "player_level":           _bal_level,
        "xp_in_level":            _bal_xp_bar["xp_in_level"],
        "xp_to_next_level":       _bal_xp_bar["xp_to_next_level"],
        "total_runs":             int(row[35]) if row[35] is not None else 0,
    }

    # --- Redis cache write ---------------------------------------------------
    # Skip caching when one-time grant events fired this call (vault drip,
    # welcome-back bonus, idle piggy) so the animation toast is not replayed
    # from stale cache on the next /bank/balance call within the TTL window.
    _no_events = (vault_pass_daily_gems == 0 and welcome_back_gold == 0 and idle_piggy_gold == 0)
    if _REDIS is not None and _no_events:
        try:
            _REDIS.setex(_cache_key, 5, json.dumps(_bal_result, default=str))
        except Exception:
            pass  # Redis fault: silently skip caching

    return _bal_result


@app.get("/player/progression")
def get_player_progression(request: Request):
    return get_balance(request)


@app.get("/player/stats")
def get_player_stats(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    cursor.execute("""
        SELECT total_money, max_unlocked_tier, board_stage,
               lifetime_cash_earned, total_merges, best_survival_time,
               best_cashouts_run, best_combo, cursed_tiles_removed, total_runs,
               gems_balance, total_piggy_smashes, best_single_cashout,
               total_piggy_earnings,
               COALESCE(max_tier_reached, 0)
        FROM players
        WHERE player_id = ?
    """, (player_id,))

    row = cursor.fetchone()

    cursor.execute("""
        SELECT achievement_id FROM achievements WHERE player_id = ?
    """, (player_id,))
    unlocked_ids = set(r[0] for r in cursor.fetchall())
    conn.close()

    if row is None:
        return {"status": "error", "message": "Player not found"}

    board_stage = int(row[2])
    stage = get_stage(board_stage)

    all_achievements = []
    for ach_id, ach_meta in ACHIEVEMENT_CONFIG.items():
        all_achievements.append({
            "id": ach_id,
            "name": ach_meta["title"],
            "description": ach_meta["description"],
            "unlocked": ach_id in unlocked_ids,
        })

    return {
        "status": "success",
        "profile": {
            "board_stage": board_stage,
            "board_rows": stage["rows"],
            "board_cols": stage["cols"],
            "max_unlocked_tier": int(row[1]),
            "total_runs": int(row[9]),
        },
        "records": {
            "lifetime_cash_earned": int(row[3]),
            "total_merges":         int(row[4]),
            # row[5] = best_survival_time, row[6] = best_cashouts_run (were in SELECT but
            # silently omitted from the response -- the stats screen showed 0 for these).
            "best_survival_time":   int(row[5]) if row[5] is not None else 0,
            "best_cashouts_run":    int(row[6]) if row[6] is not None else 0,
            "best_combo":           int(row[7]),
            "cursed_tiles_removed": int(row[8]),
            "total_piggy_smashes":  int(row[11]) if row[11] is not None else 0,
            "best_single_cashout":  int(row[12]) if row[12] is not None else 0,
            "total_piggy_earnings": int(row[13]) if row[13] is not None else 0,
            # row[14] = max_tier_reached (highest gem tier ever produced by a merge).
            "max_tier_reached":     int(row[14]) if row[14] is not None else 0,
        },
        "achievements": all_achievements,
        "gems_balance": int(row[10]) if row[10] is not None else 0,
    }


@app.get("/tutorial/status")
def get_tutorial_status(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    cursor.execute("""
        SELECT tutorial_completed
        FROM players
        WHERE player_id = ?
    """, (player_id,))

    row = cursor.fetchone()
    conn.close()

    return {
        "tutorial_completed": bool(row[0]) if row else False
    }


@app.post("/tutorial/complete")
async def complete_tutorial(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute("""
        UPDATE players
        SET tutorial_completed = 1
        WHERE player_id = ?
    """, (player_id,))

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "tutorial_completed": True
    }



# gems_reward tiers mirror the ACHIEVEMENTS const in rewards_center.gd.
# Index 0 = Tier 1, index 1 = Tier 2, index 2 = Tier 3.
# Claim is triggered client-side; this is the server-authoritative reference.
# ============================================================================
#  Sprint 7.3: XP / Level system
# ============================================================================

# _XP_TABLE[n-1] = cumulative XP required to reach level n+1  (1-indexed levels).
# Level n requires floor(100 * n^1.6) total cumulative XP.
_XP_TABLE: List[int] = [int(math.floor(100 * (n ** 1.6))) for n in range(1, 101)]


def _xp_to_level(total_xp: int) -> int:
    """Return player level (1-100) given cumulative XP."""
    level = 1
    for threshold in _XP_TABLE:
        if total_xp >= threshold:
            level += 1
        else:
            break
    return min(level, 100)


def _xp_bar_info(total_xp: int, level: int) -> Dict[str, int]:
    prev = _XP_TABLE[level - 2] if level >= 2 else 0
    nxt  = _XP_TABLE[level - 1] if level <= 99 else _XP_TABLE[99]
    xp_in   = total_xp - prev
    xp_span = nxt - prev
    return {"xp_in_level": xp_in, "xp_to_next_level": xp_span}


def _grant_xp(cursor, player_id: str, amount: int) -> Dict[str, Any]:
    """Grant XP to a player, compute level-up, credit reward gems. Returns info dict."""
    cursor.execute(
        "SELECT player_xp, player_level FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        return {}
    cur_xp    = int(row[0]) if row[0] is not None else 0
    cur_level = int(row[1]) if row[1] is not None else 1

    new_xp    = cur_xp + amount
    new_level = _xp_to_level(new_xp)

    reward_gems = 0
    if new_level > cur_level:
        for lv in range(cur_level + 1, new_level + 1):
            reward_gems += lv * 3
        cursor.execute(
            "UPDATE players SET player_xp = ?, player_level = ?, "
            "gems_balance = gems_balance + ? WHERE player_id = ?",
            (new_xp, new_level, reward_gems, player_id)
        )
    else:
        cursor.execute(
            "UPDATE players SET player_xp = ? WHERE player_id = ?",
            (new_xp, player_id)
        )

    info = {"player_xp": new_xp, "player_level": new_level}
    info.update(_xp_bar_info(new_xp, new_level))
    if new_level > cur_level:
        info["level_up"]    = True
        info["new_level"]   = new_level
        info["reward_gems"] = reward_gems
    return info


ACHIEVEMENT_CONFIG = {
    # ---- Legacy IDs kept for backward compatibility ----
    "first_combo": {
        "title": "Combo Apprentice", "category": "combo",
        "description": "Trigger your first combo.",
        "tiers": [1], "gems_reward": [0],
    },
    "survive_3_min": {
        "title": "Still Brewing", "category": "veteran",
        "description": "Survive for 3 minutes in one run.",
        "tiers": [180], "gems_reward": [0],
    },
    "survive_5_min": {
        "title": "Arcane Survivor", "category": "veteran",
        "description": "Survive for 5 minutes in one run.",
        "tiers": [300], "gems_reward": [0],
    },
    "earn_10k": {
        "title": "Gold Spark", "category": "wealthy",
        "description": "Earn 10,000 lifetime cash.",
        "tiers": [10_000], "gems_reward": [0],
    },
    "earn_100k": {
        "title": "Alchemy Tycoon", "category": "wealthy",
        "description": "Earn 100,000 lifetime cash.",
        "tiers": [100_000], "gems_reward": [0],
    },
    "run_10_cashouts": {
        "title": "Line Architect", "category": "wealthy",
        "description": "Complete 10 cashouts in one run.",
        "tiers": [10], "gems_reward": [0],
    },
    "remove_10_cursed": {
        "title": "First Cleanser", "category": "curse_breaker",
        "description": "Remove 10 cursed tiles.",
        "tiers": [10], "gems_reward": [5],
    },
    # ---- Category A: MERGER ----
    "first_merge": {
        "title": "First Merge", "category": "merger",
        "description": "Complete gem merges.",
        "tiers": [1, 50, 500, 2500, 10000],
        "gems_reward": [2, 5, 15, 30, 60],
    },
    "tier_chaser": {
        "title": "Tier Chaser", "category": "merger",
        "description": "Merge tier-5 gems or higher.",
        "tiers": [1, 25, 100, 500, 2000],
        "gems_reward": [3, 8, 20, 40, 80],
    },
    "speed_merger": {
        "title": "Speed Merger", "category": "merger",
        "description": "Achieve a high merge count in a single session.",
        "tiers": [10, 50, 200, 1000],
        "gems_reward": [2, 6, 18, 50],
    },
    # ---- Category B: WEALTHY ----
    "first_cashout": {
        "title": "First Cashout", "category": "wealthy",
        "description": "Earn lifetime cashout gold.",
        "tiers": [1, 1000, 10000, 100000, 1000000],
        "gems_reward": [2, 5, 15, 35, 75],
    },
    "high_roller": {
        "title": "High Roller", "category": "wealthy",
        "description": "Achieve a record single cashout.",
        "tiers": [500, 2500, 10000, 50000],
        "gems_reward": [3, 10, 25, 60],
    },
    "vault_hoarder": {
        "title": "Vault Hoarder", "category": "wealthy",
        "description": "Accumulate a peak wallet balance.",
        "tiers": [100, 1000, 10000, 100000],
        "gems_reward": [2, 8, 20, 50],
    },
    "wealth_builder": {
        "title": "Wealth Builder", "category": "wealthy",
        "description": "Earn lifetime cashout gold.",
        "tiers": [50000, 500000, 5000000],
        "gems_reward": [20, 40, 75],
    },
    # ---- Category C: COMBO KING ----
    "combo_master": {
        "title": "Combo Master", "category": "combo",
        "description": "Achieve a high cashout combo.",
        "tiers": [3, 7, 15],
        "gems_reward": [5, 15, 40],
    },
    "diagonal_ace": {
        "title": "Diagonal Ace", "category": "combo",
        "description": "Cash out via a diagonal line.",
        "tiers": [1, 10, 50],
        "gems_reward": [5, 15, 35],
    },
    "multi_line": {
        "title": "Multi-Line Master", "category": "combo",
        "description": "Cash out with a double-or-better combo.",
        "tiers": [1, 20, 100],
        "gems_reward": [5, 20, 50],
    },
    # ---- Category D: VETERAN ----
    "day_one": {
        "title": "Day One", "category": "veteran",
        "description": "Log in for multiple days.",
        "tiers": [1, 7, 30, 100, 365],
        "gems_reward": [2, 5, 20, 50, 150],
    },
    "run_veteran": {
        "title": "Run Veteran", "category": "veteran",
        "description": "Complete game runs.",
        "tiers": [1, 10, 50, 200, 1000],
        "gems_reward": [2, 5, 15, 40, 100],
    },
    "streak_keeper": {
        "title": "Streak Keeper", "category": "veteran",
        "description": "Maintain a login streak.",
        "tiers": [3, 7, 14, 30],
        "gems_reward": [3, 8, 20, 60],
    },
    # ---- Category E: CURSE BREAKER ----
    "curse_breaker": {
        "title": "Curse Breaker", "category": "curse_breaker",
        "description": "Remove cursed tiles from the board.",
        "tiers": [50, 250, 1000],
        "gems_reward": [5, 15, 40],
    },
    "clean_sweep": {
        "title": "Clean Sweep", "category": "curse_breaker",
        "description": "Clear all cursed tiles from the board in one run.",
        "tiers": [1, 10, 50],
        "gems_reward": [5, 20, 60],
    },
    # ---- Category F: TOOL MASTER ----
    "hammer_time": {
        "title": "Hammer Time", "category": "tool_master",
        "description": "Use the Hammer tool.",
        "tiers": [1, 25, 100, 500],
        "gems_reward": [2, 5, 15, 40],
    },
    "board_master": {
        "title": "Board Master", "category": "tool_master",
        "description": "Complete gem merges.",
        "tiers": [500, 2500, 10000],
        "gems_reward": [5, 15, 40],
    },
    "spinner": {
        "title": "Lucky Spinner", "category": "tool_master",
        "description": "Spin the Lucky Wheel.",
        "tiers": [1, 7, 30, 100],
        "gems_reward": [2, 5, 15, 40],
    },
}

# Server-authoritative purple gem amounts per quest_id.
# Must stay in sync with DAILY_POOL in rewards_center.gd.
DAILY_QUEST_GEM_REWARDS: Dict[str, int] = {
    "daily_cashout":   0,
    "daily_cursed":    0,
    "daily_runs":      0,
    "daily_combo":     0,
    "daily_survival":  0,
    "daily_merges":    0,
    "daily_highscore": 2,
    "daily_merges2":   2,
    "daily_cashout2":  3,
    "daily_survival2": 3,
    "daily_combo2":    4,
    "daily_runs3":     5,
}

# Stat column + tier targets used to validate AND auto-unlock achievements.
# stat_col must match the exact players table column name.
_ACHIEVEMENT_VALIDATION: Dict[str, Any] = {
    # Category A - Merger
    "first_merge":    {"stat_col": "total_merges",        "targets": [1, 50, 500, 2500, 10000]},
    "tier_chaser":    {"stat_col": "high_tier_merges",    "targets": [1, 25, 100, 500, 2000]},
    "speed_merger":   {"stat_col": "best_session_merges", "targets": [10, 50, 200, 1000]},
    # Category B - Wealthy
    "first_cashout":  {"stat_col": "total_money",         "targets": [1, 1000, 10000, 100000, 1000000]},
    "high_roller":    {"stat_col": "best_single_cashout", "targets": [500, 2500, 10000, 50000]},
    "vault_hoarder":  {"stat_col": "peak_wallet_balance", "targets": [100, 1000, 10000, 100000]},
    "wealth_builder": {"stat_col": "total_money",         "targets": [50000, 500000, 5000000]},
    # Category C - Combo
    "combo_master":   {"stat_col": "best_combo",          "targets": [3, 7, 15]},
    "diagonal_ace":   {"stat_col": "diagonal_cashouts",   "targets": [1, 10, 50]},
    "multi_line":     {"stat_col": "multi_line_cashouts", "targets": [1, 20, 100]},
    # Category D - Veteran
    "day_one":        {"stat_col": "login_days",          "targets": [1, 7, 30, 100, 365]},
    "run_veteran":    {"stat_col": "total_runs",          "targets": [1, 10, 50, 200, 1000]},
    "streak_keeper":  {"stat_col": "best_login_streak",   "targets": [3, 7, 14, 30]},
    # Category E - Curse Breaker
    "curse_breaker":  {"stat_col": "cursed_tiles_removed","targets": [50, 250, 1000]},
    "clean_sweep":    {"stat_col": "perfect_cleanses",    "targets": [1, 10, 50]},
    # Category F - Tool Master
    "hammer_time":    {"stat_col": "hammers_used",        "targets": [1, 25, 100, 500]},
    "board_master":   {"stat_col": "total_merges",        "targets": [500, 2500, 10000]},
    "spinner":        {"stat_col": "total_spins",         "targets": [1, 7, 30, 100]},
}

# Parameterized SELECT for each stat_col (must match DB column names exactly).
_ACH_STAT_SELECT_SQL: Dict[str, str] = {
    "total_merges":         "SELECT total_merges         FROM players WHERE player_id = ?",
    "high_tier_merges":     "SELECT high_tier_merges     FROM players WHERE player_id = ?",
    "best_session_merges":  "SELECT best_session_merges  FROM players WHERE player_id = ?",
    "total_money":          "SELECT total_money          FROM players WHERE player_id = ?",
    "best_single_cashout":  "SELECT best_single_cashout  FROM players WHERE player_id = ?",
    "peak_wallet_balance":  "SELECT peak_wallet_balance  FROM players WHERE player_id = ?",
    "best_combo":           "SELECT best_combo           FROM players WHERE player_id = ?",
    "diagonal_cashouts":    "SELECT diagonal_cashouts    FROM players WHERE player_id = ?",
    "multi_line_cashouts":  "SELECT multi_line_cashouts  FROM players WHERE player_id = ?",
    "login_days":           "SELECT login_days           FROM players WHERE player_id = ?",
    "total_runs":           "SELECT total_runs           FROM players WHERE player_id = ?",
    "best_login_streak":    "SELECT best_login_streak    FROM players WHERE player_id = ?",
    "cursed_tiles_removed": "SELECT cursed_tiles_removed FROM players WHERE player_id = ?",
    "perfect_cleanses":     "SELECT perfect_cleanses     FROM players WHERE player_id = ?",
    "hammers_used":         "SELECT hammers_used         FROM players WHERE player_id = ?",
    "total_spins":          "SELECT total_spins          FROM players WHERE player_id = ?",
}


def unlock_achievement(cursor, player_id: str, achievement_id: str):
    cursor.execute(_SQL_INSERT_ACHIEVEMENT, (player_id, achievement_id))


def check_and_unlock_achievements(cursor, player_id: str) -> List[Dict[str, Any]]:
    """
    Auto-check every achievement in ACHIEVEMENT_CONFIG against the player's current
    stats.  For each newly crossed tier threshold, insert an achievement_claim and
    grant the gem reward immediately.  Returns a list of newly awarded entries so
    the caller can include them in the API response.
    """
    # 1. Collect all stat values needed in one pass.
    needed_cols = set(v["stat_col"] for v in _ACHIEVEMENT_VALIDATION.values())
    # SELECT only the columns we have SQL for to avoid missing-column crashes.
    stat_values: Dict[str, int] = {}
    for col in needed_cols:
        if col in _ACH_STAT_SELECT_SQL:
            try:
                cursor.execute(_ACH_STAT_SELECT_SQL[col], (player_id,))
                row = cursor.fetchone()
                stat_values[col] = int(row[0]) if row and row[0] is not None else 0
            except Exception:
                stat_values[col] = 0

    # 2. Fetch already-claimed tiers so we never double-award.
    cursor.execute(
        "SELECT ach_id, tier FROM achievement_claims WHERE player_id = ?",
        (player_id,)
    )
    already_claimed: set = set((r[0], r[1]) for r in cursor.fetchall())

    today_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    newly_awarded: List[Dict[str, Any]] = []

    for ach_id, validation in _ACHIEVEMENT_VALIDATION.items():
        col     = validation["stat_col"]
        targets = validation["targets"]
        cur_val = stat_values.get(col, 0)
        config  = ACHIEVEMENT_CONFIG.get(ach_id, {})
        gems_list = config.get("gems_reward", [])

        for tier_idx, target in enumerate(targets):
            if cur_val < target:
                break  # targets are sorted ascending; no point checking further
            if (ach_id, tier_idx) in already_claimed:
                continue
            # This tier is newly reached.
            try:
                cursor.execute(
                    "INSERT INTO achievement_claims (player_id, ach_id, tier) VALUES (?, ?, ?)",
                    (player_id, ach_id, tier_idx)
                )
            except Exception:
                continue  # race or duplicate -- skip silently

            # Wrap secondary operations so a failure here cannot propagate out
            # and roll back the achievement_claims INSERT above.
            try:
                unlock_achievement(cursor, player_id, ach_id)
                gems = int(gems_list[tier_idx]) if tier_idx < len(gems_list) else 0
                if gems > 0:
                    _award_free_gems(cursor, player_id, gems, today_str)
            except Exception:
                gems = 0  # gem award failed silently; the claim was recorded

            newly_awarded.append({
                "id":           ach_id,
                "title":        config.get("title", ach_id),
                "tier":         tier_idx,
                "gems_awarded": gems,
            })

    return newly_awarded


def evaluate_achievements(cursor, player_id: str, run_stats: Dict[str, Any], lifetime_cash: int, cursed_removed_total: int):
    unlocked_before_query = cursor.execute("""
        SELECT achievement_id FROM achievements WHERE player_id = ?
    """, (player_id,))
    unlocked_before = set(row[0] for row in unlocked_before_query.fetchall())

    cashouts = int(run_stats.get("cashouts", 0))
    survival_time = int(run_stats.get("survival_time", 0))
    best_combo = int(run_stats.get("best_combo", 1))

    candidates = []

    if cashouts >= 1:
        candidates.append("first_cashout")

    if best_combo >= 2:
        candidates.append("first_combo")

    if best_combo >= 3:
        candidates.append("combo_master")

    if survival_time >= 180:
        candidates.append("survive_3_min")

    if survival_time >= 300:
        candidates.append("survive_5_min")

    if lifetime_cash >= 10000:
        candidates.append("earn_10k")

    if lifetime_cash >= 100000:
        candidates.append("earn_100k")

    if cursed_removed_total >= 10:
        candidates.append("remove_10_cursed")

    if cashouts >= 10:
        candidates.append("run_10_cashouts")

    newly_unlocked = []

    for achievement_id in candidates:
        unlock_achievement(cursor, player_id, achievement_id)
        if achievement_id not in unlocked_before:
            meta = ACHIEVEMENT_CONFIG.get(achievement_id, {})
            newly_unlocked.append({
                "id": achievement_id,
                "title": meta.get("title", achievement_id),
                "description": meta.get("description", "")
            })

    return newly_unlocked



def get_or_assign_quest(cursor, player_id: str) -> dict:
    cursor.execute("""
        SELECT quest_id, progress, target, reward
        FROM player_quests WHERE player_id = ?
    """, (player_id,))
    row = cursor.fetchone()
    if row:
        quest_meta = _QUEST_MAP.get(row[0], QUEST_POOL[0])
        return {
            "quest_id":   row[0],
            "title":      quest_meta["title"],
            "desc":       quest_meta["desc"],
            "type":       quest_meta["type"],
            "progress":   int(row[1]),
            "target":     int(row[2]),
            "reward":     int(row[3]),
            "gems_reward": int(quest_meta.get("gems_reward", 0)),
        }
    # Assign a fresh quest.
    quest = random.choice(QUEST_POOL)
    cursor.execute("""
        INSERT INTO player_quests (player_id, quest_id, progress, target, reward)
        VALUES (?, ?, 0, ?, ?)
    """, (player_id, quest["id"], quest["target"], quest["reward"]))
    return {
        "quest_id":   quest["id"],
        "title":      quest["title"],
        "desc":       quest["desc"],
        "type":       quest["type"],
        "progress":   0,
        "target":     quest["target"],
        "reward":     quest["reward"],
        "gems_reward": int(quest.get("gems_reward", 0)),
    }


def evaluate_and_advance_quest(cursor, player_id: str, run_stats: Dict[str, Any], board_stage: int = 0) -> dict:
    quest = get_or_assign_quest(cursor, player_id)
    stat_value = int(run_stats.get(quest["type"], 0))
    new_progress = max(quest["progress"], stat_value)

    reward_granted      = 0
    gems_reward_granted = 0
    _quest_mult = _get_quest_multiplier(board_stage)
    if new_progress >= quest["target"]:
        reward_granted = int(quest["reward"] * _quest_mult)
        # Gold grant (scaled by board stage)
        cursor.execute("""
            UPDATE players SET total_money = total_money + ? WHERE player_id = ?
        """, (reward_granted, player_id))
        # Gem grant (look up live from QUEST_MAP so DB rows don't need a gems column)
        gems_reward_granted = int(_QUEST_MAP.get(quest["quest_id"], {}).get("gems_reward", 0))
        if gems_reward_granted > 0:
            cursor.execute("""
                UPDATE players SET gems_balance = gems_balance + ? WHERE player_id = ?
            """, (gems_reward_granted, player_id))
        # Assign a new quest (prefer a different one when pool is large enough).
        choices = [q for q in QUEST_POOL if q["id"] != quest["quest_id"]] or QUEST_POOL
        next_quest = random.choice(choices)
        cursor.execute(_SQL_ASSIGN_QUEST, (player_id, next_quest["id"], next_quest["target"], next_quest["reward"]))
        return {
            "completed":           True,
            "reward_granted":      reward_granted,
            "base_reward":         quest["reward"],
            "gems_reward_granted": gems_reward_granted,
            "old_quest": quest,
            "new_quest": {
                "quest_id":      next_quest["id"],
                "title":         next_quest["title"],
                "desc":          next_quest["desc"],
                "type":          next_quest["type"],
                "progress":      0,
                "target":        next_quest["target"],
                "reward":        next_quest["reward"],
                "scaled_reward": int(next_quest["reward"] * _quest_mult),
                "gems_reward":   int(next_quest.get("gems_reward", 0)),
            },
        }

    cursor.execute("""
        UPDATE player_quests SET progress = ? WHERE player_id = ?
    """, (new_progress, player_id))
    quest["progress"]      = new_progress
    quest["scaled_reward"] = int(quest["reward"] * _quest_mult)
    return {
        "completed":          False,
        "reward_granted":     0,
        "gems_reward_granted": 0,
        "quest": quest,
    }


@app.get("/quest/state")
def get_quest_state(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    quest = get_or_assign_quest(cursor, player_id)
    cursor.execute("SELECT board_stage FROM players WHERE player_id = ?", (player_id,))
    _stage_row   = cursor.fetchone()
    _board_stage = int(_stage_row[0]) if _stage_row and _stage_row[0] is not None else 0
    conn.commit()
    conn.close()
    quest["base_reward"]   = quest["reward"]
    quest["scaled_reward"] = int(quest["reward"] * _get_quest_multiplier(_board_stage))
    return {"status": "success", "quest": quest}


@app.get("/stats/state")
def get_stats_state(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    cursor.execute("""
        SELECT lifetime_cash_earned, best_survival_time, best_cashouts_run,
               best_combo, cursed_tiles_removed, total_runs
        FROM players
        WHERE player_id = ?
    """, (player_id,))

    row = cursor.fetchone()

    cursor.execute("""
        SELECT achievement_id, unlocked_at
        FROM achievements
        WHERE player_id = ?
        ORDER BY unlocked_at DESC
    """, (player_id,))

    achievements = []
    for achievement_id, unlocked_at in cursor.fetchall():
        meta = ACHIEVEMENT_CONFIG.get(achievement_id, {})
        achievements.append({
            "id": achievement_id,
            "title": meta.get("title", achievement_id),
            "description": meta.get("description", ""),
            "unlocked_at": unlocked_at
        })

    conn.close()

    if row is None:
        return {"status": "error", "message": "Player not found"}

    return {
        "status": "success",
        "stats": {
            "lifetime_cash_earned": row[0],
            "best_survival_time": row[1],
            "best_cashouts_run": row[2],
            "best_combo": row[3],
            "cursed_tiles_removed": row[4],
            "total_runs": row[5],
        },
        "achievements": achievements
    }


@app.post("/stats/submit_run")
async def submit_run_stats(request: Request, payload: Dict[str, Any] = Body(...)):
    player_id = extract_player_id(request)

    # Rate limit: prevent rapid repeat submissions (accidental double-fire or replay attack).
    if not _check_rate_limit(player_id, "submit_run", min_interval_secs=5.0):
        return {"status": "error", "message": "Too many requests. Please wait before submitting again."}

    # Clamp all incoming values to their theoretical per-run maximums.
    # This is the primary anti-cheat layer: a legitimate run cannot exceed these bounds.
    survival_time   = min(max(0, int(payload.get("survival_time",   0))), _MAX_SURVIVAL_SECS)
    cashouts        = min(max(0, int(payload.get("cashouts",        0))), _MAX_CASHOUTS_PER_RUN)
    cash_earned     = min(max(0, int(payload.get("cash_earned",     0))), _MAX_CASH_PER_RUN)
    best_combo      = min(max(1, int(payload.get("best_combo",      1))), _MAX_COMBO_VALUE)
    cursed_removed  = min(max(0, int(payload.get("cursed_removed",  0))), _MAX_CURSED_REMOVED)
    run_merges      = min(max(0, int(payload.get("run_merges",      0))), _MAX_MERGES_PER_RUN)
    run_combo_count = min(max(0, int(payload.get("run_combo_count", 0))), _MAX_COMBO_COUNT_RUN)

    # Additional run-level payload fields (clamped for anti-cheat).
    _MAX_HIGH_TIER_MERGES = 500
    _MAX_SINGLE_CASHOUT   = _MAX_CASH_PER_RUN
    high_tier_merges_run = min(max(0, int(payload.get("high_tier_merges",  0))), _MAX_HIGH_TIER_MERGES)
    best_cashout_run     = min(max(0, int(payload.get("best_cashout_run",  0))), _MAX_SINGLE_CASHOUT)
    # Highest gem tier actually produced by a merge this run (client tracks as _run_max_tier_merged).
    max_tier_merged_run  = min(max(0, int(payload.get("max_tier_merged",   0))), 11)
    is_new_pb_cashout    = False

    # The whole DB section is wrapped: any unexpected error (schema drift,
    # bad payload, etc.) is logged and answered with a graceful JSON error
    # instead of an unhandled 500 -- an unhandled exception here previously
    # left the connection (or pooled PG connection) open/leaked, which could
    # cascade into every other endpoint failing with connection errors.
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)

        cursor.execute("""
            SELECT lifetime_cash_earned, best_survival_time, best_cashouts_run,
                   best_combo, cursed_tiles_removed, total_runs, total_merges,
                   high_tier_merges, best_session_merges, best_single_cashout,
                   peak_wallet_balance, total_money, board_stage,
                   COALESCE(max_tier_reached, 0)
            FROM players
            WHERE player_id = ?
        """, (player_id,))

        row = cursor.fetchone()

        if row is None:
            return {"status": "error", "message": "Player not found"}

        lifetime_cash        = int(row[0]) + cash_earned
        best_survival_time   = max(int(row[1]), survival_time)
        best_cashouts_run    = max(int(row[2]), cashouts)
        best_combo_overall   = max(int(row[3]), best_combo)
        cursed_removed_total = int(row[4]) + cursed_removed
        total_runs           = int(row[5]) + 1
        total_merges         = int(row[6]) + run_merges
        high_tier_merges_new = int(row[7] or 0) + high_tier_merges_run
        best_session_merges  = max(int(row[8] or 0), run_merges)
        prev_best_cashout    = int(row[9] or 0)
        best_single_cashout  = max(prev_best_cashout, best_cashout_run)
        if best_cashout_run > prev_best_cashout:
            is_new_pb_cashout = True
        new_total_money      = int(row[11] or 0) + cash_earned
        peak_wallet_balance  = max(int(row[10] or 0), new_total_money)
        board_stage_for_quest = int(row[12]) if row[12] is not None else 0
        max_tier_reached_new  = max(int(row[13] or 0), max_tier_merged_run)

        cursor.execute("""
            UPDATE players
            SET lifetime_cash_earned = ?,
                best_survival_time = ?,
                best_cashouts_run = ?,
                best_combo = ?,
                cursed_tiles_removed = ?,
                total_runs = ?,
                total_merges = ?,
                high_tier_merges = ?,
                best_session_merges = ?,
                best_single_cashout = ?,
                peak_wallet_balance = ?,
                max_tier_reached = ?,
                is_rescue_active = 0
            WHERE player_id = ?
        """, (
            lifetime_cash,
            best_survival_time,
            best_cashouts_run,
            best_combo_overall,
            cursed_removed_total,
            total_runs,
            total_merges,
            high_tier_merges_new,
            best_session_merges,
            best_single_cashout,
            peak_wallet_balance,
            max_tier_reached_new,
            player_id
        ))

        newly_unlocked = check_and_unlock_achievements(cursor, player_id)

        quest_result = evaluate_and_advance_quest(cursor, player_id, {
            "survival_time":   survival_time,
            "cashouts":        cashouts,
            "cash_earned":     cash_earned,
            "run_merges":      run_merges,
            "run_combo_count": run_combo_count,
        }, board_stage=board_stage_for_quest)

        combo_gems_awarded = min(run_combo_count // 3, 10)
        if combo_gems_awarded > 0:
            cursor.execute(
                "UPDATE players SET gems_balance = COALESCE(gems_balance, 0) + ? WHERE player_id = ?",
                (combo_gems_awarded, player_id)
            )

        # XP grant: +10 base, +2 per merge, +5 per cashout, +20 if new personal best cashout.
        xp_grant = 10 + (run_merges * 2) + (cashouts * 5) + (20 if is_new_pb_cashout else 0)
        xp_info  = _grant_xp(cursor, player_id, xp_grant)

        cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
        _gems_row = cursor.fetchone()
        new_gems_balance = int(_gems_row[0]) if _gems_row and _gems_row[0] is not None else 0

        conn.commit()
    except Exception as _run_err:
        print(f"[submit_run] HTTP 500 averted -- {type(_run_err).__name__}: {_run_err}")
        traceback.print_exc()
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return {"status": "error", "message": "Run stats could not be saved due to a server error. Your run was still recorded locally."}
    finally:
        if conn is not None:
            conn.close()

    # Elite meta accrual: advance event + chapter progress from the CLAMPED
    # (anti-cheat) run stats into the durable EliteStore. Pure Redis -- no DB
    # dependency. Wrapped so a meta hiccup can never break run submission.
    try:
        _elite.accrue_run(player_id, {
            "cashouts":             cashouts,
            "run_merges":           run_merges,
            "run_combo_count":      run_combo_count,
            "cursed_removed":       cursed_removed,
            "best_cashout_run":     best_cashout_run,
            "weekly_cashout_total": int(payload.get("weekly_cashout_total", 0)),
        })
    except Exception as _acc_err:
        print(f"[submit_run] elite accrue_run skipped: {_acc_err}")

    resp = {
        "status": "success",
        "stats": {
            "lifetime_cash_earned": lifetime_cash,
            "best_survival_time":   best_survival_time,
            "best_cashouts_run":    best_cashouts_run,
            "best_combo":           best_combo_overall,
            "cursed_tiles_removed": cursed_removed_total,
            "total_runs":           total_runs,
            "total_merges":         total_merges,
        },
        "new_achievements":  newly_unlocked,
        "quest":             quest_result,
        "combo_gems_awarded": combo_gems_awarded,
        "new_gems_balance":  new_gems_balance,
        "xp_granted":        xp_grant,
    }
    resp.update(xp_info)
    return resp



@app.get("/shop/state")
def get_shop_state(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    cursor.execute("""
        SELECT total_money, max_unlocked_tier, board_stage, unlocked_golden, unlocked_catalyst,
               has_insurance, gems_balance, unlocked_wildcard,
               vault_slot_0_unlocked, vault_slot_1_unlocked, vault_slot_0_gem, vault_slot_1_gem,
               lucky_drop_lvl, piggy_mastery_lvl, tool_discount_lvl
        FROM players
        WHERE player_id = ?
    """, (player_id,))

    row = cursor.fetchone()
    balance, max_tier, board_stage = row[0], row[1], row[2]
    unlocked_golden    = int(row[3])  if row[3]  is not None else 0
    unlocked_catalyst  = int(row[4])  if row[4]  is not None else 0
    has_insurance      = int(row[5])  if row[5]  is not None else 0
    gems_balance       = int(row[6])  if row[6]  is not None else 0
    unlocked_wildcard  = int(row[7])  if row[7]  is not None else 0
    vault_s0_unlocked  = int(row[8])  if row[8]  is not None else 0
    vault_s1_unlocked  = int(row[9])  if row[9]  is not None else 0
    vault_s0_gem       = int(row[10]) if row[10] is not None else 0
    vault_s1_gem       = int(row[11]) if row[11] is not None else 0
    lucky_drop_lvl     = int(row[12]) if row[12] is not None else 0
    piggy_mastery_lvl  = int(row[13]) if row[13] is not None else 0
    tool_discount_lvl  = int(row[14]) if row[14] is not None else 0
    conn.close()

    current_stage = get_stage(board_stage)
    cashout_target_tier = max_tier

    # --- Full board catalog: all 9 stages with ownership status ---
    board_catalog = []
    for i, stage in enumerate(BOARD_STAGES):
        if i < board_stage:
            status = "owned"
        elif i == board_stage:
            status = "owned"
        elif i == board_stage + 1:
            status = "available"
        else:
            status = "locked"
        board_catalog.append({
            "stage": i,
            "rows": stage["rows"],
            "cols": stage["cols"],
            "cost": stage["cost"],
            "cashout_tier": stage["cashout_tier"],
            "status": status,
        })

    # --- Full tier catalog: all 11 tiers with ownership status ---
    tier_catalog = []
    for tier in range(1, 12):
        cost = 0 if tier in BASE_TIERS else TIER_UNLOCK_COSTS.get(tier, 0)
        if tier <= max_tier:
            status = "owned"
        elif tier == max_tier + 1:
            status = "available"
        else:
            status = "locked"
        tier_catalog.append({
            "tier": tier,
            "name": TILE_NAMES.get(tier, f"Tier {tier}"),
            "cost": cost,
            "status": status,
        })

    special_catalog = [
        {
            "id":          "wildcard_core",
            "name":        "Wildcard Gem",
            "description": "A wildcard gem auto-spawns and merges with any normal gem.",
            "cost":        20,
            "currency":    "gems",
            "status":      "owned" if unlocked_wildcard else "available",
        },
        {
            "id":          "golden_license",
            "name":        "Golden Crystal",
            "description": "A golden crystal auto-spawns and multiplies your cashout by x4.",
            "cost":        80,
            "currency":    "gems",
            "status":      "owned" if unlocked_golden else "available",
        },
        {
            "id":          "golden_license_2",
            "name":        "Rock Cleanser",
            "description": "Hammer the Rock Cleanser to instantly destroy ALL Cursed Tiles at once.",
            "cost":        100,
            "currency":    "gems",
            "status":      "owned" if (unlocked_golden >= 2) else "available",
        },
        {
            "id":          "catalyst_core",
            "name":        "Fusion Catalyst",
            "description": "Smash with a Hammer to upgrade all adjacent gems by +1!",
            "cost":        120,
            "currency":    "gems",
            "status":      "owned" if unlocked_catalyst else "available",
        },
        {
            "id":          "board_insurance",
            "name":        "Board Insurance",
            "description": "One-time shield: when your board fills up, destroys 2 Cursed Tiles and saves your run.",
            "cost":        150,
            "currency":    "gems",
            "status":      "owned" if has_insurance else "available",
        },
    ]

    return {
        "balance":           balance,
        "gems_balance":      gems_balance,
        "max_unlocked_tier": max_tier,
        "board_stage":       board_stage,
        "board_rows":        current_stage["rows"],
        "board_cols":        current_stage["cols"],
        "cashout_target_tier": cashout_target_tier,
        "board_catalog":     board_catalog,
        "tier_catalog":      tier_catalog,
        "special_catalog":   special_catalog,
        "unlocked_golden":       unlocked_golden,
        "unlocked_catalyst":     unlocked_catalyst,
        "has_insurance":         has_insurance,
        "unlocked_wildcard":     unlocked_wildcard,
        "vault_slot_0_unlocked": vault_s0_unlocked,
        "vault_slot_1_unlocked": vault_s1_unlocked,
        "vault_slot_0_gem":      vault_s0_gem,
        "vault_slot_1_gem":      vault_s1_gem,
        "lucky_drop_lvl":        lucky_drop_lvl,
        "piggy_mastery_lvl":     piggy_mastery_lvl,
        "tool_discount_lvl":     tool_discount_lvl,
    }


@app.post("/shop/upgrade_mastery")
async def upgrade_mastery(request: Request, payload: Dict[str, Any] = Body(...)):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    upgrade_id = str(payload.get("upgrade_id", ""))
    if upgrade_id not in MASTERY_CONFIG:
        return {"status": "error", "message": "Unknown upgrade_id: " + upgrade_id}
    cfg     = MASTERY_CONFIG[upgrade_id]
    col     = cfg["col"]
    costs   = cfg["costs"]   # per-level cost list; index = current_lvl before upgrade
    max_lvl = cfg["max"]

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(_MASTERY_SELECT_SQL[col], (player_id,))
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    gems        = int(row[0]) if row[0] is not None else 0
    current_lvl = int(row[1]) if row[1] is not None else 0

    if current_lvl >= max_lvl:
        conn.close()
        return {"status": "error", "message": "Already at max level"}

    # Lucky Drop: each pair of levels requires the player to have unlocked the target tier.
    _LUCKY_DROP_TIER_REQS = [3, 3, 4, 4, 5, 5]
    if upgrade_id == "lucky_drop" and current_lvl < len(_LUCKY_DROP_TIER_REQS):
        req_tier = _LUCKY_DROP_TIER_REQS[current_lvl]
        cursor.execute("SELECT max_unlocked_tier FROM players WHERE player_id = ?", (player_id,))
        tier_row = cursor.fetchone()
        player_tier = int(tier_row[0]) if tier_row and tier_row[0] is not None else 1
        if player_tier < req_tier:
            conn.close()
            return {"status": "error", "message": f"Requires Tier {req_tier} unlocked"}

    cost    = costs[current_lvl]   # costs[0]=Lvl1 price
    new_lvl = current_lvl + 1

    if gems < cost:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost, "gems": gems}

    cursor.execute(_MASTERY_UPDATE_SQL[col], (cost, new_lvl, player_id))
    cursor.execute(
        "SELECT lucky_drop_lvl, piggy_mastery_lvl, tool_discount_lvl, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    r2 = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":           "success",
        "upgrade_id":       upgrade_id,
        "new_level":        new_lvl,
        "cost":             cost,
        "lucky_drop_lvl":   int(r2[0]) if r2[0] is not None else 0,
        "piggy_mastery_lvl": int(r2[1]) if r2[1] is not None else 0,
        "tool_discount_lvl": int(r2[2]) if r2[2] is not None else 0,
        "new_gems":         int(r2[3]) if r2[3] is not None else 0,
    }


@app.get("/mastery/state")
async def mastery_state(request: Request):
    player_id = extract_player_id(request)
    conn      = get_connection()
    cursor    = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT mastery_state FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    conn.close()
    raw   = (row[0] if row and row[0] else "{}") or "{}"
    state = _safe_parse_mastery(raw)
    return {"status": "ok", "mastery_state": state, "mastery_tree": MASTERY_TREE}


@app.post("/mastery/upgrade")
async def mastery_upgrade(request: Request, payload: Dict[str, Any] = Body(...)):
    player_id = extract_player_id(request)
    node_id   = str(payload.get("node_id", ""))

    if node_id not in MASTERY_TREE:
        return {"status": "error", "message": "Unknown node_id: " + node_id}

    cfg        = MASTERY_TREE[node_id]
    max_level  = int(cfg["max_level"])
    base_cost  = float(cfg["base_cost"])
    multiplier = float(cfg["cost_multiplier"])
    dependency = cfg["dependency"]

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT mastery_state, total_money FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    raw_state    = (row[0] if row[0] else "{}") or "{}"
    gold_balance = int(row[1]) if row[1] is not None else 0
    state        = _safe_parse_mastery(raw_state)

    current_level = int(state.get(node_id, 0))

    if current_level >= max_level:
        conn.close()
        return {"status": "error", "message": "Already at max level"}

    if dependency is not None:
        dep_level = int(state.get(dependency, 0))
        if dep_level < 1:
            conn.close()
            return {"status": "error", "message": "Dependency not met: " + dependency}

    cost = int(base_cost * (multiplier ** current_level))

    if gold_balance < cost:
        conn.close()
        return {
            "status":  "error",
            "message": "Not enough gold",
            "cost":    cost,
            "balance": gold_balance,
        }

    state[node_id] = current_level + 1
    new_state_json = json.dumps(state)

    cursor.execute(
        "UPDATE players SET mastery_state = ?, total_money = total_money - ? WHERE player_id = ?",
        (new_state_json, cost, player_id)
    )
    cursor.execute(
        "SELECT total_money FROM players WHERE player_id = ?",
        (player_id,)
    )
    r2 = cursor.fetchone()
    conn.commit()
    conn.close()

    return {
        "status":        "success",
        "node_id":       node_id,
        "new_level":     state[node_id],
        "cost":          cost,
        "mastery_state": state,
        "new_balance":   int(r2[0]) if r2 and r2[0] is not None else 0,
    }


@app.post("/shop/purchase_vault_slot")
async def purchase_vault_slot(request: Request, slot: int):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    if slot not in (0, 1):
        return {"status": "error", "message": "Invalid slot index"}
    cost = 50
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    if slot == 0:
        cursor.execute(
            "SELECT gems_balance, vault_slot_0_unlocked FROM players WHERE player_id = ?",
            (player_id,)
        )
    else:
        cursor.execute(
            "SELECT gems_balance, vault_slot_1_unlocked FROM players WHERE player_id = ?",
            (player_id,)
        )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems          = int(row[0]) if row[0] is not None else 0
    already_owned = int(row[1]) if row[1] is not None else 0
    if already_owned:
        conn.close()
        return {"status": "error", "message": "Slot already unlocked"}
    if gems < cost:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost, "gems": gems}
    if slot == 0:
        cursor.execute(
            "UPDATE players SET gems_balance = gems_balance - ?, vault_slot_0_unlocked = 1 WHERE player_id = ?",
            (cost, player_id)
        )
    else:
        cursor.execute(
            "UPDATE players SET gems_balance = gems_balance - ?, vault_slot_1_unlocked = 1 WHERE player_id = ?",
            (cost, player_id)
        )
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    new_gems = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "slot": slot, "cost": cost, "new_gems": new_gems}


@app.post("/player/sync_vault")
async def sync_vault(request: Request, payload: Dict[str, Any] = Body(...)):
    player_id = extract_player_id(request)
    vault = payload.get("vault", [0, 0])
    if not isinstance(vault, list) or len(vault) < 2:
        return {"status": "error", "message": "vault must be an array of length 2"}
    gem_0 = max(0, int(vault[0]))
    gem_1 = max(0, int(vault[1]))
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "UPDATE players SET vault_slot_0_gem = ?, vault_slot_1_gem = ? WHERE player_id = ?",
        (gem_0, gem_1, player_id)
    )
    conn.commit()
    conn.close()
    return {"status": "success", "vault_slot_0_gem": gem_0, "vault_slot_1_gem": gem_1}


@app.post("/shop/unlock_tier")
async def unlock_tier(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute("""
        SELECT total_money, max_unlocked_tier
        FROM players
        WHERE player_id = ?
    """, (player_id,))

    balance, current_max_tier = cursor.fetchone()
    next_tier = current_max_tier + 1

    if next_tier not in TIER_UNLOCK_COSTS:
        conn.close()
        return {"status": "error", "message": "No more tiers to unlock"}

    cost = TIER_UNLOCK_COSTS[next_tier]

    if balance < cost:
        conn.close()
        return {
            "status": "error",
            "message": "Not enough money",
            "cost": cost,
            "balance": balance
        }

    cursor.execute("""
        UPDATE players
        SET total_money = total_money - ?, max_unlocked_tier = ?
        WHERE player_id = ?
    """, (cost, next_tier, player_id))

    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status": "success",
        "unlocked_tier": next_tier,
        "cost": cost,
        "new_balance": balance - cost
    }


@app.post("/shop/expand_board")
async def expand_board(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute("""
        SELECT total_money, board_stage
        FROM players
        WHERE player_id = ?
    """, (player_id,))

    balance, current_stage_index = cursor.fetchone()
    next_stage_index = current_stage_index + 1

    if next_stage_index >= len(BOARD_STAGES):
        conn.close()
        return {"status": "error", "message": "Board is already at maximum size"}

    next_stage = BOARD_STAGES[next_stage_index]
    cost = next_stage["cost"]

    if balance < cost:
        conn.close()
        return {
            "status": "error",
            "message": "Not enough money",
            "cost": cost,
            "balance": balance
        }

    cursor.execute("""
        UPDATE players
        SET total_money = total_money - ?, board_stage = ?,
            board_size = ?
        WHERE player_id = ?
    """, (cost, next_stage_index, max(next_stage["rows"], next_stage["cols"]), player_id))

    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status": "success",
        "board_stage": next_stage_index,
        "board_rows": next_stage["rows"],
        "board_cols": next_stage["cols"],
        "cost": cost,
        "new_balance": balance - cost
    }


@app.post("/shop/unlock_golden")
async def unlock_golden(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    cost_gems = 80
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT gems_balance, unlocked_golden FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems, already_owned = int(row[0]) if row[0] is not None else 0, int(row[1]) if row[1] is not None else 0
    if already_owned:
        conn.close()
        return {"status": "error", "message": "Already unlocked"}
    if gems < cost_gems:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost_gems, "gems": gems}
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance - ?, unlocked_golden = 1 WHERE player_id = ?",
        (cost_gems, player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "unlocked": "golden_license", "cost": cost_gems, "currency": "gems", "new_gems": gems - cost_gems}


@app.post("/shop/unlock_golden_2")
async def unlock_golden_2(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    cost_gems = 100
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT gems_balance, unlocked_golden FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems       = int(row[0]) if row[0] is not None else 0
    golden_lvl = int(row[1]) if row[1] is not None else 0
    if golden_lvl < 1:
        conn.close()
        return {"status": "error", "message": "Golden Crystal Lv 1 must be unlocked first"}
    if golden_lvl >= 2:
        conn.close()
        return {"status": "error", "message": "Already unlocked"}
    if gems < cost_gems:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost_gems, "gems": gems}
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance - ?, unlocked_golden = 2 WHERE player_id = ?",
        (cost_gems, player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "unlocked": "golden_license_2", "cost": cost_gems, "currency": "gems", "new_gems": gems - cost_gems}


@app.post("/shop/unlock_wildcard")
async def unlock_wildcard(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    cost_gems = 20
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT gems_balance, unlocked_wildcard FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems          = int(row[0]) if row[0] is not None else 0
    already_owned = int(row[1]) if row[1] is not None else 0
    if already_owned:
        conn.close()
        return {"status": "error", "message": "Already unlocked"}
    if gems < cost_gems:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost_gems, "gems": gems}
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance - ?, unlocked_wildcard = 1 WHERE player_id = ?",
        (cost_gems, player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "unlocked": "wildcard_core", "cost": cost_gems, "currency": "gems", "new_gems": gems - cost_gems}


@app.post("/shop/unlock_catalyst")
async def unlock_catalyst(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    cost_gems = 120
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT gems_balance, unlocked_catalyst FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems, already_owned = int(row[0]) if row[0] is not None else 0, int(row[1]) if row[1] is not None else 0
    if already_owned:
        conn.close()
        return {"status": "error", "message": "Already unlocked"}
    if gems < cost_gems:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost_gems, "gems": gems}
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance - ?, unlocked_catalyst = 1 WHERE player_id = ?",
        (cost_gems, player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "unlocked": "catalyst_core", "cost": cost_gems, "currency": "gems", "new_gems": gems - cost_gems}


@app.post("/wheel/spin")
async def spin_wheel(request: Request):
    player_id = extract_player_id(request)

    # Rate limit: 2 s minimum between spin requests prevents double-tap race conditions.
    if not _check_rate_limit(player_id, "wheel_spin", min_interval_secs=2.0):
        return {"status": "error", "message": "Spin request too fast. Please wait."}
    if not _check_burst_limit(player_id, "wheel_spin", 10, 60.0):
        return {"status": "error", "message": "Too many spin requests. Please slow down."}

    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute("""
        SELECT gems_balance, piggy_balance, total_money, last_free_spin_time,
               spins_today, last_spin_reset, spin_pity_counter
        FROM players WHERE player_id = ?
    """, (player_id,))
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    gems           = int(row[0]) if row[0] is not None else 0
    piggy          = int(row[1]) if row[1] is not None else 0
    total_money    = int(row[2]) if row[2] is not None else 0
    last_spin      = row[3]
    spins_raw      = int(row[4]) if row[4] is not None else 0
    last_reset_str = row[5]
    pity_counter   = int(row[6]) if row[6] is not None else 0

    # Calendar-day reset for spin counter (same pattern as piggy smashes).
    now_utc    = datetime.datetime.utcnow()
    today_date = now_utc.date()
    effective_spins = spins_raw
    reset_needed    = False
    if last_reset_str is not None:
        try:
            last_reset_date = datetime.datetime.fromisoformat(str(last_reset_str)).date()
            if last_reset_date < today_date:
                effective_spins = 0
                reset_needed    = True
        except Exception:
            effective_spins = 0
            reset_needed    = True
    else:
        effective_spins = 0
        reset_needed    = True

    spin_cost = _get_spin_cost(effective_spins)
    is_free   = (spin_cost == 0)

    if spin_cost > 0:
        if gems < spin_cost:
            conn.close()
            return {
                "status":          "error",
                "message":         f"Need {spin_cost} Gems to spin again today",
                "spin_cost":       spin_cost,
                "next_spin_cost":  spin_cost,
            }
        gems -= spin_cost
        cursor.execute(
            "UPDATE players SET gems_balance = ? WHERE player_id = ?",
            (gems, player_id)
        )

    # ------------------------------------------------------------------ RNG draw
    # Must happen BEFORE the DB write so new_pity is defined when we UPDATE.
    # Pity system: if pity_counter >= threshold, draw only from non-common pool.
    pity_activated = pity_counter >= PITY_THRESHOLD
    if pity_activated:
        pity_weights = [
            (w if WHEEL_PRIZES[i]["rarity"] not in _COMMON_RARITIES else 0)
            for i, w in enumerate(WHEEL_WEIGHTS)
        ]
        # Safety: if all weights are 0 (should never happen), fall back to full pool.
        if sum(pity_weights) == 0:
            pity_weights = WHEEL_WEIGHTS
        winner_index = random.choices(range(len(WHEEL_PRIZES)), weights=pity_weights)[0]
    else:
        winner_index = random.choices(range(len(WHEEL_PRIZES)), weights=WHEEL_WEIGHTS)[0]
    prize = WHEEL_PRIZES[winner_index]

    # Pity counter: reset on any non-common outcome, increment on common.
    new_pity = 0 if prize["rarity"] not in _COMMON_RARITIES else pity_counter + 1

    # ------------------------------------------------------------------ DB write
    now_iso          = now_utc.isoformat()
    new_spins_today  = effective_spins + 1
    new_reset_str    = now_iso if reset_needed else last_reset_str
    cursor.execute(
        """UPDATE players SET last_free_spin_time = ?,
               spins_today = ?, last_spin_reset = ?, spin_pity_counter = ?
           WHERE player_id = ?""",
        (now_iso, new_spins_today, new_reset_str, new_pity, player_id)
    )

    # Deterministic target angle for client-side replay and anti-cheat audit logs.
    # Represents the wheel-local angle of the winner's slice center (0 = 3 o'clock on the disc).
    target_angle: float = (winner_index + 0.5) * _WHEEL_SLICE_ANGLE

    # Immediate credit: gold and gems
    if prize["gold"] > 0:
        total_money += prize["gold"]
        cursor.execute(
            "UPDATE players SET total_money = ? WHERE player_id = ?",
            (total_money, player_id)
        )
    if prize["gems"] > 0:
        gems += prize["gems"]
        cursor.execute(
            "UPDATE players SET gems_balance = ? WHERE player_id = ?",
            (gems, player_id)
        )

    # Active run modifier (applied at next run start via active_modifier column)
    if prize["modifier"] is not None:
        cursor.execute(
            "UPDATE players SET active_modifier = ? WHERE player_id = ?",
            (prize["modifier"], player_id)
        )

    # Vault Boost prize: activate timed boost columns so /bank/balance returns it.
    boost_activated: bool = False
    if prize["id"] == "boost_cashout_2x":
        boost_cfg  = VAULT_BOOSTS.get("boost_cashout_2x", {})
        dur_min    = int(boost_cfg.get("duration_min", 30))
        expires_at = (now_utc + datetime.timedelta(minutes=dur_min)).isoformat()
        cursor.execute(
            "UPDATE players SET boost_type = ?, boost_active_until = ? WHERE player_id = ?",
            ("boost_cashout_2x", expires_at, player_id)
        )
        boost_activated = True

    # Pending board modifier (Golden Diamonds + free hammers -- injected at next run)
    if prize["pending_mod"] is not None:
        cursor.execute(
            "SELECT pending_modifiers FROM players WHERE player_id = ?",
            (player_id,)
        )
        pm_row = cursor.fetchone()
        existing = {}
        if pm_row and pm_row[0]:
            try:
                existing = json.loads(pm_row[0])
            except Exception:
                existing = {}
        # free_hammers always grants exactly 2; all other pending mods increment by 1
        grant = 2 if prize["pending_mod"] == "free_hammers" else 1
        existing[prize["pending_mod"]] = existing.get(prize["pending_mod"], 0) + grant
        cursor.execute(
            "UPDATE players SET pending_modifiers = ? WHERE player_id = ?",
            (json.dumps(existing), player_id)
        )

    cursor.execute("""
        SELECT total_money, gems_balance, piggy_balance
        FROM players WHERE player_id = ?
    """, (player_id,))
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    next_spin_cost = _get_spin_cost(new_spins_today)
    return {
        "status":          "success",
        "winner_index":    winner_index,
        "prize_id":        prize["id"],
        "prize_name":      prize["name"],
        "prize_rarity":    prize["rarity"],
        "is_jackpot":      prize["rarity"] == "jackpot",
        "is_free":         is_free,
        "spin_cost":       spin_cost,
        "next_spin_cost":  next_spin_cost,
        "spins_today":     new_spins_today,
        "new_balance":     int(updated[0]),
        "new_gems":        int(updated[1]),
        "new_piggy":       int(updated[2]),
        "active_modifier": prize["modifier"] if prize["modifier"] is not None else "",
        "pending_mod":     prize["pending_mod"] if prize["pending_mod"] is not None else "",
        "target_angle":    round(target_angle, 6),
        "pity_counter":    new_pity,
        "pity_activated":  pity_activated,
        "boost_activated": boost_activated,
    }


@app.post("/player/claim_piggy_modifier")
async def claim_piggy_modifier(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT piggy_balance, active_modifier FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    piggy    = int(row[0]) if row[0] is not None else 0
    modifier = row[1]
    if modifier != "piggy_smash":
        conn.close()
        return {"status": "error", "message": "No piggy smash modifier active"}
    cursor.execute(
        """UPDATE players SET
               total_money    = total_money + ?,
               piggy_balance  = 0,
               active_modifier = NULL
           WHERE player_id = ?""",
        (piggy, player_id)
    )
    cursor.execute("SELECT total_money FROM players WHERE player_id = ?", (player_id,))
    new_bal = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    return {"status": "success", "vault_gained": piggy, "new_balance": new_bal, "new_piggy": 0}


@app.post("/player/clear_modifier")
async def clear_modifier(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE players SET active_modifier = NULL WHERE player_id = ?",
        (player_id,)
    )
    conn.commit()
    conn.close()
    return {"status": "success"}


@app.post("/player/consume_modifier")
async def consume_modifier(request: Request):
    """Decrement a pending board modifier after the board has successfully injected it."""
    player_id = extract_player_id(request)
    try:
        body = await request.json()
        mod_key = str(body.get("modifier", "")).strip()
    except Exception:
        return {"status": "error", "message": "Invalid request body"}
    if not mod_key:
        return {"status": "error", "message": "modifier key required"}

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT pending_modifiers FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    mods = {}
    if row and row[0]:
        try:
            mods = json.loads(row[0])
        except Exception:
            mods = {}

    if mod_key in mods:
        mods[mod_key] -= 1
        if mods[mod_key] <= 0:
            del mods[mod_key]

    cursor.execute(
        "UPDATE players SET pending_modifiers = ? WHERE player_id = ?",
        (json.dumps(mods), player_id)
    )
    conn.commit()
    conn.close()
    return {"status": "ok", "pending_modifiers": mods}


@app.post("/shop/buy_insurance")
async def buy_insurance(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    cost_gems = 150
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT gems_balance, has_insurance FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gems      = int(row[0]) if row[0] is not None else 0
    owned     = int(row[1]) if row[1] is not None else 0
    if owned:
        conn.close()
        return {"status": "error", "message": "Already insured"}
    if gems < cost_gems:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost_gems, "gems": gems}
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance - ?, has_insurance = 1 WHERE player_id = ?",
        (cost_gems, player_id)
    )
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    new_gems = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "unlocked": "board_insurance", "cost": cost_gems, "new_gems": new_gems}


# ---------------------------------------------------------------------------
# Vault Boosts -- gold-sink consumable shop
# ---------------------------------------------------------------------------

@app.get("/shop/boosts")
async def get_boosts(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT total_money, boost_active_until, boost_type "
        "FROM players WHERE player_id = ?", (player_id,)
    )
    row = cursor.fetchone()
    conn.close()
    gold    = int(row[0]) if row and row[0] else 0
    b_until = str(row[1]) if row and row[1] else ""
    b_type  = str(row[2]) if row and row[2] else ""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    active_boost = None
    seconds_left = 0
    if b_until and b_type:
        try:
            exp = datetime.datetime.fromisoformat(b_until)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=datetime.timezone.utc)
            if now_utc < exp:
                active_boost = b_type
                seconds_left = int((exp - now_utc).total_seconds())
        except ValueError:
            pass
    return {
        "status":       "ok",
        "gold_balance": gold,
        "active_boost": active_boost,
        "seconds_left": seconds_left,
        "boosts":       [
            {**v, "id": k, "affordable": gold >= v["cost_gold"]}
            for k, v in VAULT_BOOSTS.items()
        ],
    }


@app.post("/shop/buy_boost")
async def buy_boost(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    if not _check_rate_limit(player_id, "buy_boost", 5.0):
        raise HTTPException(status_code=429, detail="Too many requests")
    try:
        body     = await request.json()
        boost_id = str(body.get("boost_id", ""))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if boost_id not in VAULT_BOOSTS:
        raise HTTPException(status_code=400, detail="Unknown boost_id")
    boost = VAULT_BOOSTS[boost_id]
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT total_money FROM players WHERE player_id = ?", (player_id,)
    )
    row  = cursor.fetchone()
    gold = int(row[0]) if row and row[0] else 0
    if gold < boost["cost_gold"]:
        conn.close()
        return {"status": "error", "message": "Insufficient gold"}
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    expires  = now_utc + datetime.timedelta(minutes=boost["duration_min"])
    cursor.execute(
        "UPDATE players SET total_money = total_money - ?, "
        "boost_active_until = ?, boost_type = ? "
        "WHERE player_id = ?",
        (boost["cost_gold"], expires.isoformat(), boost_id, player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {
        "status":       "success",
        "boost_id":     boost_id,
        "expires_at":   expires.isoformat(),
        "seconds_left": boost["duration_min"] * 60,
    }


_COSMETIC_PRICES = {
    "cosmic_void":    {"price":  5000, "currency": "gold"},
    "deep_ocean":     {"price": 15000, "currency": "gold"},
    "ember_forge":    {"price": 30000, "currency": "gold"},
    "arcane_grove":   {"price":   250, "currency": "gems"},
    "royal_obsidian": {"price":   600, "currency": "gems"},
    # Shard-priced event items -- purchasable via /shop/buy_cosmetic or /event/shop/buy
    "shard_frame_gold":  {"price":  50, "currency": "shards"},
    "shard_boost_merge": {"price":  80, "currency": "shards"},
    "shard_gem_pack":    {"price": 120, "currency": "shards"},
}

_EVENT_SHOP_ITEMS = [
    {
        "id": "shard_frame_gold",
        "name": "Gilded Vault Frame",
        "description": "A shimmering gold border cosmetic for your board.",
        "cost": 50, "currency": "shards",
    },
    {
        "id": "shard_boost_merge",
        "name": "Merge Storm",
        "description": "Double merge XP for the next 24 hours of play.",
        "cost": 80, "currency": "shards",
    },
    {
        "id": "shard_gem_pack",
        "name": "Crystal Cache",
        "description": "Instantly receive 25 Gems deposited to your wallet.",
        "cost": 120, "currency": "shards",
    },
]


@app.post("/offer/game_over")
async def game_over_offer(request: Request):
    """
    Called immediately after a game ends. Evaluates whether the player was close
    to a progression milestone and returns a targeted loss-aversion offer.
    Returns {"offer": null} if no offer is appropriate.
    """
    player_id = extract_player_id(request)
    try:
        body         = await request.json()
        cash_at_end  = int(body.get("cash_at_end",  0))
        board_stage  = int(body.get("board_stage",  0))
        cashouts_run = int(body.get("cashouts_run", 0))
    except Exception:
        return {"offer": None}

    try:
        conn = get_connection()
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        cursor.execute(
            "SELECT total_money, board_stage, gems_balance FROM players "
            "WHERE player_id = ?", (player_id,)
        )
        row = cursor.fetchone()
        conn.close()
    except Exception:
        return {"offer": None}

    total_gold = int(row[0]) if row and row[0] else 0
    p_stage    = int(row[1]) if row and row[1] else 0
    gems       = int(row[2]) if row and row[2] else 0

    offer = None

    # Milestone offer: player is within 30% of next board expansion cost
    next_stage_idx = p_stage + 1
    if next_stage_idx < len(BOARD_STAGES):
        next_cost = BOARD_STAGES[next_stage_idx]["cost"]
        deficit   = next_cost - total_gold
        if 0 < deficit <= next_cost * 0.30:
            gems_needed = max(20, min(150, int(deficit / 100)))
            offer = {
                "type":        "gem_boost",
                "title":       "So Close!",
                "body":        f"You need {deficit:,} more gold for your next board. "
                               f"Get {gems_needed} Gems to push through!",
                "cost_gems":   gems_needed,
                "ttl_seconds": 60,
                "affordable":  gems >= gems_needed,
            }

    return {"offer": offer}


@app.post("/offer/accept")
async def accept_offer(request: Request):
    """
    Deducts gems when the player accepts a game-over loss-aversion offer.
    Requires X-Signature (HMAC-SHA256) matching /player/rescue pattern.
    Body: {"offer_type": str, "cost_gems": int}
    Returns: {"status": "success", "offer_type": str, "gems_spent": int, "new_gems": int}
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    raw_body = await request.body()
    try:
        body       = json.loads(raw_body) if raw_body else {}
        offer_type = str(body.get("offer_type", "")).strip()
        cost_gems  = int(body.get("cost_gems", 0))
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not offer_type:
        raise HTTPException(status_code=400, detail="offer_type is required")
    if cost_gems < 0:
        raise HTTPException(status_code=400, detail="cost_gems must be >= 0")

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    row = cursor.fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Player not found")

    gems = int(row[0]) if row[0] is not None else 0
    if cost_gems > 0 and gems < cost_gems:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost_gems": cost_gems, "gems": gems}

    if cost_gems > 0:
        cursor.execute(
            "UPDATE players SET gems_balance = gems_balance - ? WHERE player_id = ?",
            (cost_gems, player_id)
        )
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    new_gems = int(cursor.fetchone()[0])

    try:
        cursor.execute(
            "INSERT INTO telemetry_logs (player_id, event_name, event_data, session_id) VALUES (?, ?, ?, ?)",
            (player_id, "offer_accepted", json.dumps({"offer_type": offer_type, "gems_spent": cost_gems}), "server")
        )
    except Exception:
        pass

    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "offer_type": offer_type, "gems_spent": cost_gems, "new_gems": new_gems}


@app.get("/shop/cosmetics")
async def get_cosmetics(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT unlocked_cosmetics, active_cosmetic_id FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    conn.close()
    if row is None:
        return {"owned": [], "equipped": ""}
    try:
        owned = json.loads(row[0]) if row[0] else []
    except Exception:
        owned = []
    equipped = str(row[1]) if row[1] else ""
    return {"owned": owned, "equipped": equipped}


@app.post("/shop/buy_cosmetic")
async def buy_cosmetic(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        return {"status": "error", "message": "Invalid body"}

    cosmetic_id = str(body.get("id", ""))
    if cosmetic_id not in _COSMETIC_PRICES:
        return {"status": "error", "message": "Unknown cosmetic"}

    price_cfg = _COSMETIC_PRICES[cosmetic_id]
    cost      = price_cfg["price"]
    currency  = price_cfg["currency"]

    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT total_money, gems_balance, unlocked_cosmetics, shards_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    gold   = int(row[0]) if row[0] is not None else 0
    gems   = int(row[1]) if row[1] is not None else 0
    shards = int(row[3]) if row[3] is not None else 0
    try:
        owned = json.loads(row[2]) if row[2] else []
    except Exception:
        owned = []

    if cosmetic_id in owned:
        conn.close()
        return {"status": "error", "message": "Already owned"}

    new_gold = gold; new_gems = gems; new_shards = shards
    if currency == "gold":
        if gold < cost:
            conn.close()
            return {"status": "error", "message": "Not enough gold", "cost": cost, "gold": gold}
        owned.append(cosmetic_id)
        cursor.execute(
            "UPDATE players SET total_money = total_money - ?, unlocked_cosmetics = ?, active_cosmetic_id = ? WHERE player_id = ?",
            (cost, json.dumps(owned), cosmetic_id, player_id)
        )
        cursor.execute("SELECT total_money, gems_balance, shards_balance FROM players WHERE player_id = ?", (player_id,))
        upd = cursor.fetchone()
        new_gold = int(upd[0]); new_gems = int(upd[1]); new_shards = int(upd[2])
    elif currency == "shards":
        if shards < cost:
            conn.close()
            return {"status": "error", "message": "Not enough shards", "cost": cost, "shards": shards}
        owned.append(cosmetic_id)
        cursor.execute(
            "UPDATE players SET shards_balance = shards_balance - ?, unlocked_cosmetics = ?, active_cosmetic_id = ? WHERE player_id = ?",
            (cost, json.dumps(owned), cosmetic_id, player_id)
        )
        cursor.execute("SELECT total_money, gems_balance, shards_balance FROM players WHERE player_id = ?", (player_id,))
        upd = cursor.fetchone()
        new_gold = int(upd[0]); new_gems = int(upd[1]); new_shards = int(upd[2])
    else:  # gems
        if gems < cost:
            conn.close()
            return {"status": "error", "message": "Not enough gems", "cost": cost, "gems": gems}
        owned.append(cosmetic_id)
        cursor.execute(
            "UPDATE players SET gems_balance = gems_balance - ?, unlocked_cosmetics = ?, active_cosmetic_id = ? WHERE player_id = ?",
            (cost, json.dumps(owned), cosmetic_id, player_id)
        )
        cursor.execute("SELECT total_money, gems_balance, shards_balance FROM players WHERE player_id = ?", (player_id,))
        upd = cursor.fetchone()
        new_gold = int(upd[0]); new_gems = int(upd[1]); new_shards = int(upd[2])

    conn.commit()
    conn.close()
    return {
        "status":      "success",
        "id":          cosmetic_id,
        "currency":    currency,
        "new_gold":    new_gold,
        "new_gems":    new_gems,
        "new_shards":  new_shards,
        "owned":       owned,
        "equipped":    cosmetic_id,
    }


@app.post("/shop/equip_cosmetic")
async def equip_cosmetic(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        return {"status": "error", "message": "Invalid body"}

    cosmetic_id = str(body.get("id", ""))
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT unlocked_cosmetics FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    try:
        owned = json.loads(row[0]) if row[0] else []
    except Exception:
        owned = []

    # Allow equipping "" (default/unequip) or any owned cosmetic
    if cosmetic_id != "" and cosmetic_id not in owned:
        conn.close()
        return {"status": "error", "message": "Cosmetic not owned"}

    cursor.execute(
        "UPDATE players SET active_cosmetic_id = ? WHERE player_id = ?",
        (cosmetic_id, player_id)
    )
    conn.commit()
    conn.close()
    return {"status": "success", "equipped": cosmetic_id}


@app.post("/player/use_insurance")
async def use_insurance(request: Request):
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE players SET has_insurance = 0 WHERE player_id = ?",
        (player_id,)
    )
    conn.commit()
    conn.close()
    return {"status": "success"}


# ---------------------------------------------------------------------------
# Rescue deduplication helpers
# ---------------------------------------------------------------------------

async def _expire_rescue_flag(player_id: str, delay: float = 10.0) -> None:
    """
    Clears is_rescue_active after `delay` seconds.  This runs as a fire-and-forget
    asyncio task so that if the client never responds (player killed app, network
    error, etc.) the flag does not stay stuck at 1 forever.

    The conditional WHERE clause means a successful /player/rescue that already
    cleared the flag to 0 will not be incorrectly re-set to 1 by this expiry.
    """
    await asyncio.sleep(delay)
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute(
            "UPDATE players SET is_rescue_active = 0 "
            "WHERE player_id = ? AND is_rescue_active = 1",
            (player_id,)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # best-effort; do not crash the background loop


@app.post("/player/rescue_check")
async def rescue_check(request: Request):
    """
    Single-entry gate for the rescue popup.

    The client calls this endpoint every time it detects a game-over condition.
    The server enforces that the rescue window can only be opened ONCE per run:

      - "show_rescue"   : First detection.  Flag set; client must show the popup.
      - "already_active": Flag already set by an earlier duplicate call this run.
                          Client must NOT reset the existing countdown -- silently ignore.
      - "unavailable"   : Rescue has already been consumed this run (or player
                          row missing).  Client should proceed to real game over.
    """
    player_id = extract_player_id(request)
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    cursor.execute(
        "SELECT is_rescue_active FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "unavailable"}

    is_rescue_active = int(row[0]) if row[0] is not None else 0

    # Duplicate request arriving while the popup is already open.
    # Return early WITHOUT touching the flag or spawning another expiry task.
    if is_rescue_active == 1:
        conn.close()
        return {"status": "already_active"}

    # First detection: arm the rescue window and record the open timestamp so
    # /bank/balance can auto-expire the flag after a server restart.
    _rescue_now = datetime.datetime.utcnow().isoformat()
    cursor.execute(
        "UPDATE players SET is_rescue_active = 1, rescue_active_since = ? WHERE player_id = ?",
        (_rescue_now, player_id)
    )
    conn.commit()
    conn.close()

    # Spawn the auto-expiry background task.
    # Delay = client countdown (8 s) + server round-trip buffer (2 s) = 10 s.
    asyncio.create_task(_expire_rescue_flag(player_id, delay=10.0))

    return {"status": "show_rescue"}


@app.post("/player/rescue")
async def player_rescue(request: Request):
    """
    Deducts gems and confirms a successful rescue.
    Clears is_rescue_active atomically in the same UPDATE so there is no window
    where both the gem deduction and the flag clear could be seen inconsistently.
    cost is now read from the signed JSON body instead of a query parameter.
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    raw_body = await request.body()
    # Valid rescue costs mirror the client scoring formula: 5 / 10 / 15 / 20 gems.
    # 100 = Ember Blitz (Rush Mode) Daily Challenge revive: a fixed gems-only
    # premium price, distinct from the standard board-value-scaled costs.
    _RESCUE_VALID_COSTS = {5, 10, 15, 20, 100}
    _RESCUE_COST_MAX    = max(_RESCUE_VALID_COSTS)
    try:
        body = json.loads(raw_body) if raw_body else {}
        cost = int(body.get("cost", 10))
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if cost not in _RESCUE_VALID_COSTS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid rescue cost {cost}. Valid values: {sorted(_RESCUE_VALID_COSTS)}"
        )
    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute(
        "SELECT gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    gems = int(row[0]) if row[0] is not None else 0
    if gems < cost:
        conn.close()
        return {"status": "error", "message": "Not enough Gems", "cost": cost, "gems": gems}

    # Deduct gems AND clear the rescue flag in a single atomic write.
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance - ?, is_rescue_active = 0 "
        "WHERE player_id = ?",
        (cost, player_id)
    )
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    new_gems = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    return {"status": "success", "new_gems": new_gems}


@app.post("/rescues/claim_free")
async def claim_free_rescue(request: Request):
    """
    One-time free rescue for first-time game-over.
    No gem deduction; just clears is_rescue_active and records the telemetry event.
    The canonical "used" flag lives in the client's ConfigFile -- this endpoint
    exists so the claim is logged server-side and the 404 never blocks the UX.
    """
    player_id = extract_player_id(request)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        cursor.execute(
            "UPDATE players SET is_rescue_active = 0 WHERE player_id = ?",
            (player_id,)
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "success"}


def _seconds_until_utc_midnight(now_utc: datetime.datetime) -> int:
    """Seconds from now_utc until the next UTC calendar-day boundary (00:00:01)."""
    next_midnight = (now_utc + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=1, microsecond=0
    )
    return max(1, int((next_midnight - now_utc).total_seconds()))


def _compute_piggy_smash_state(smashes_today: int, last_reset_str: str, now_utc: datetime.datetime):
    """
    Calendar-day reset.  Compares today's UTC date to the date stored in
    last_piggy_reset.  Returns (effective_smashes_today, reset_needed,
    seconds_until_utc_midnight).

    reset_needed=True whenever last_reset_str is absent or belongs to a
    previous calendar day.

    seconds_until_utc_midnight is always the seconds to 00:00:01 UTC so
    clients can schedule a UI refresh exactly when the new day unlocks.
    """
    secs_left  = _seconds_until_utc_midnight(now_utc)
    today_date = now_utc.date()

    if not last_reset_str:
        return 0, True, secs_left

    try:
        last_date = datetime.datetime.fromisoformat(str(last_reset_str)).date()
        if last_date < today_date:
            return 0, True, secs_left   # new calendar day: full reset
        return smashes_today, False, secs_left  # same day: keep current count
    except (ValueError, AttributeError):
        return 0, True, secs_left


@app.get("/piggy/state")
async def piggy_state(request: Request):
    player_id = extract_player_id(request)
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT piggy_balance, gems_balance, piggy_smashes_today, last_piggy_reset, mastery_state "
        "FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    conn.close()
    if row is None:
        return {"piggy_balance": 0, "gems_balance": 0,
                "smashes_remaining": 3, "cooldown_active": False, "seconds_remaining": 0,
                "piggy_cap": 20000}

    piggy          = int(row[0]) if row[0] is not None else 0
    gems           = int(row[1]) if row[1] is not None else 0
    smashes_raw    = int(row[2]) if row[2] is not None else 0
    last_reset_str = row[3]
    piggy_cap      = get_piggy_cap(row[4] if row[4] else "{}")

    now_utc = datetime.datetime.utcnow()
    effective_smashes, _, secs_left = _compute_piggy_smash_state(smashes_raw, last_reset_str, now_utc)
    smashes_remaining = max(0, 3 - effective_smashes)

    gem_cost = get_piggy_gem_cost(piggy)
    return {
        "piggy_balance":     piggy,
        "gems_balance":      gems,
        "smashes_remaining": smashes_remaining,
        "cooldown_active":   smashes_remaining <= 0,
        "seconds_remaining": secs_left if smashes_remaining <= 0 else 0,
        "gem_cost":          gem_cost,
        "piggy_cap":         piggy_cap,
    }


@app.post("/piggy/smash")
async def piggy_smash(request: Request):
    player_id = extract_player_id(request)

    # Rate limit: 3 s minimum prevents a laggy client double-posting and spending two smashes.
    if not _check_rate_limit(player_id, "piggy_smash", min_interval_secs=3.0):
        return {"status": "error", "message": "Smash request too fast. Please wait."}
    if not _check_burst_limit(player_id, "piggy_smash", 8, 60.0):
        return {"status": "error", "message": "Too many smash requests. Please slow down."}

    try:
        body       = await request.json()
        smash_type = str(body.get("smash_type", "free")).lower()
    except Exception:
        return {"status": "error", "message": "Invalid request body"}

    if smash_type not in ("free", "ad", "gems"):
        return {"status": "error", "message": "Invalid smash_type"}

    # Ad smashes require a server-issued one-time token (prevents replay attacks).
    if smash_type == "ad":
        ad_token = str(body.get("ad_reward_token", "")).strip()
        if not ad_token or not _consume_ad_token(player_id, "piggy_smash", ad_token):
            return {"status": "error", "message": "Invalid or expired ad token. Watch the ad again."}

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute(
        "SELECT piggy_balance, gems_balance, total_money, piggy_smashes_today, last_piggy_reset, mastery_state "
        "FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    piggy          = int(row[0]) if row[0] is not None else 0
    gems           = int(row[1]) if row[1] is not None else 0
    smashes_raw    = int(row[3]) if row[3] is not None else 0
    last_reset_str = row[4]

    # econ_cashout_bonus mastery: +5% per level, max 3 levels (+15%)
    _ms = _safe_parse_mastery(row[5])
    _cashout_bonus_lvl  = int(_ms.get("econ_cashout_bonus", 0))
    _cashout_multiplier = 1.0 + _cashout_bonus_lvl * 0.05

    # Resolve daily window: may reset counter if 24 h have passed
    now_utc = datetime.datetime.utcnow()
    effective_smashes, reset_needed, secs_left = _compute_piggy_smash_state(
        smashes_raw, last_reset_str, now_utc
    )

    if effective_smashes >= 3:
        conn.close()
        h = max(0, secs_left // 3600)
        m = max(0, (secs_left % 3600) // 60)
        return {
            "status":            "error",
            "message":           f"Daily limit of 3 smashes reached. Resets in {h}h {m}m.",
            "smashes_remaining": 0,
            "seconds_remaining": secs_left,
        }

    if piggy <= 0:
        conn.close()
        return {"status": "error", "message": "Piggy Bank is empty! Fill it by merging."}

    # Calculate reward. _cashout_multiplier is 1.0 + (econ_cashout_bonus_lvl * 0.05).
    # Applied linearly (not compounded) to the base payout fraction.
    gem_cost = get_piggy_gem_cost(piggy)
    if smash_type == "free":
        reward = max(1, int(piggy * 0.30 * _cashout_multiplier))
    elif smash_type == "ad":
        piggy_cap_local = get_piggy_cap(row[5] if row[5] else "{}")
        fill_pct = piggy / max(1, piggy_cap_local)
        if fill_pct < 0.25:
            ad_fraction = 0.45
        elif fill_pct < 0.50:
            ad_fraction = 0.55
        elif fill_pct < 0.75:
            ad_fraction = 0.65
        else:
            ad_fraction = 0.70
        reward = max(1, int(piggy * ad_fraction * _cashout_multiplier))
    else:  # gems -- dynamic cost scales with piggy balance
        if gems < gem_cost:
            conn.close()
            return {"status": "error", "message": f"Need {gem_cost} Gems for a full Piggy Smash", "gems": gems, "gem_cost": gem_cost}
        reward = int(piggy * _cashout_multiplier)

    new_smashes_today = effective_smashes + 1
    new_reset_str     = now_utc.isoformat() if reset_needed else last_reset_str

    if smash_type == "gems":
        cursor.execute(
            """UPDATE players SET
                   total_money          = total_money + ?,
                   piggy_balance        = 0,
                   gems_balance         = gems_balance - ?,
                   piggy_smashes_today  = ?,
                   last_piggy_reset     = ?,
                   total_piggy_smashes  = COALESCE(total_piggy_smashes, 0) + 1,
                   total_piggy_earnings = COALESCE(total_piggy_earnings, 0) + ?
               WHERE player_id = ?""",
            (reward, gem_cost, new_smashes_today, new_reset_str, reward, player_id)
        )
    else:
        cursor.execute(
            """UPDATE players SET
                   total_money          = total_money + ?,
                   piggy_balance        = 0,
                   piggy_smashes_today  = ?,
                   last_piggy_reset     = ?,
                   total_piggy_smashes  = COALESCE(total_piggy_smashes, 0) + 1,
                   total_piggy_earnings = COALESCE(total_piggy_earnings, 0) + ?
               WHERE player_id = ?""",
            (reward, new_smashes_today, new_reset_str, reward, player_id)
        )

    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    smashes_remaining = max(0, 3 - new_smashes_today)
    return {
        "status":            "success",
        "smash_type":        smash_type,
        "reward_amount":     reward,
        "wallet_balance":    int(updated[0]),
        "gems_balance":      int(updated[1]),
        "piggy_balance":     0,
        "smashes_remaining": smashes_remaining,
        "gem_cost":          gem_cost,
    }


@app.post("/ad/reward_token")
async def ad_reward_token(request: Request):
    """
    Client calls this endpoint AFTER the AdMob SSV (or dev-sim) confirms the
    ad was watched.  Returns a 40-hex one-time token valid for 90 s that must
    be included in the subsequent economic API call (/piggy/smash, /player/double_daily).
    """
    player_id = extract_player_id(request)

    if not _check_rate_limit(player_id, "ad_reward_token", min_interval_secs=2.0):
        return {"status": "error", "message": "Token request too fast -- please wait."}

    try:
        body    = await request.json()
        context = str(body.get("context", "")).strip()[:32]
    except Exception:
        return {"status": "error", "message": "Invalid request body"}

    if context not in ("piggy_smash", "daily_double", "wheel_double", "piggy_double"):
        return {"status": "error", "message": f"Unknown ad context: {context}"}

    token = _issue_ad_token(player_id, context)
    return {"status": "success", "ad_reward_token": token, "context": context}


@app.post("/player/double_daily")
async def double_daily_reward(request: Request):
    """
    Credits a second copy of today's daily reward after a verified ad watch.
    Requires the one-time token issued by /ad/reward_token (context=daily_double).
    Safe to call only once per calendar day; enforced by the daily_doubled column.
    """
    player_id = extract_player_id(request)

    if not _check_rate_limit(player_id, "double_daily", min_interval_secs=5.0):
        return {"status": "error", "message": "Request too fast -- please wait."}

    try:
        body      = await request.json()
        ad_token  = str(body.get("ad_reward_token", "")).strip()
    except Exception:
        return {"status": "error", "message": "Invalid request body"}

    if not ad_token or not _consume_ad_token(player_id, "daily_double", ad_token):
        return {"status": "error", "message": "Invalid or expired ad token. Watch the ad again."}

    today_utc = datetime.datetime.utcnow().date().isoformat()

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT login_streak, last_login_date, daily_doubled FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}

    streak        = int(row[0]) if row[0] is not None else 1
    last_login    = str(row[1]) if row[1] is not None else ""
    daily_doubled = int(row[2]) if row[2] is not None else 0

    if last_login != today_utc:
        conn.close()
        return {"status": "error", "message": "Claim your daily reward first!"}

    if daily_doubled >= 1:
        conn.close()
        return {"status": "error", "message": "Daily reward already doubled today!"}

    # Re-credit the same day's reward amounts (2x total = original claim + this bonus).
    # Clamp streak to valid range [1..7] before indexing DAILY_REWARDS.
    safe_streak = max(1, min(streak, 7))
    reward      = DAILY_REWARDS[safe_streak]
    gold_bonus  = reward["gold"]
    gems_bonus  = reward["gems"]

    cursor.execute(
        """UPDATE players SET
               total_money  = total_money + ?,
               gems_balance = gems_balance + ?,
               daily_doubled = 1
           WHERE player_id = ?""",
        (gold_bonus, gems_bonus, player_id)
    )
    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":      "success",
        "gold_bonus":  gold_bonus,
        "gems_bonus":  gems_bonus,
        "new_balance": int(updated[0]),
        "new_gems":    int(updated[1]),
    }


@app.post("/wheel/double_reward")
async def wheel_double_reward(request: Request):
    """
    Credits a second copy of a Lucky Spin prize's currency after a verified ad watch.
    Only gold and gems are doubled -- modifiers, pending_mod, and boosts are NOT re-applied.
    The one-time token issued by /ad/reward_token (context=wheel_double) is consumed here.
    """
    player_id = extract_player_id(request)

    if not _check_rate_limit(player_id, "wheel_double_reward", min_interval_secs=5.0):
        return {"status": "error", "message": "Request too fast -- please wait."}

    try:
        body         = await request.json()
        ad_token     = str(body.get("ad_reward_token", "")).strip()
        winner_index = int(body.get("winner_index", -1))
    except Exception:
        return {"status": "error", "message": "Invalid request body"}

    if not ad_token or not _consume_ad_token(player_id, "wheel_double", ad_token):
        return {"status": "error", "message": "Invalid or expired ad token. Watch the ad again."}

    if winner_index < 0 or winner_index >= len(WHEEL_PRIZES):
        return {"status": "error", "message": "Invalid winner_index"}

    prize      = WHEEL_PRIZES[winner_index]
    gold_bonus = int(prize["gold"])
    gems_bonus = int(prize["gems"])

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        """UPDATE players SET
               total_money  = total_money  + ?,
               gems_balance = gems_balance + ?
           WHERE player_id = ?""",
        (gold_bonus, gems_bonus, player_id)
    )
    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":      "success",
        "gold_bonus":  gold_bonus,
        "gems_bonus":  gems_bonus,
        "new_balance": int(updated[0]),
        "new_gems":    int(updated[1]),
    }


@app.post("/piggy/ad_bonus")
async def piggy_ad_bonus(request: Request):
    """
    Credits the post-smash ad-doubler reward: 50% of the smash payout.
    Requires a one-time token issued by /ad/reward_token (context=piggy_double).
    The client sends the bonus amount; the server caps it at 5,000 gold to limit abuse.
    """
    player_id = extract_player_id(request)

    if not _check_rate_limit(player_id, "piggy_ad_bonus", min_interval_secs=5.0):
        return {"status": "error", "message": "Request too fast -- please wait."}

    try:
        body     = await request.json()
        ad_token = str(body.get("ad_reward_token", "")).strip()
        amount   = int(body.get("amount", 0))
    except Exception:
        return {"status": "error", "message": "Invalid request body"}

    if not ad_token or not _consume_ad_token(player_id, "piggy_double", ad_token):
        return {"status": "error", "message": "Invalid or expired ad token. Watch the ad again."}

    amount = max(0, min(amount, 5000))
    if amount == 0:
        conn   = get_connection()
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        cursor.execute("SELECT total_money, gems_balance FROM players WHERE player_id = ?", (player_id,))
        row = cursor.fetchone()
        conn.close()
        return {"status": "success", "gold_bonus": 0,
                "new_balance": int(row[0]) if row else 0, "new_gems": int(row[1]) if row else 0}

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "UPDATE players SET total_money = total_money + ? WHERE player_id = ?",
        (amount, player_id)
    )
    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    updated = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":      "success",
        "gold_bonus":  amount,
        "new_balance": int(updated[0]),
        "new_gems":    int(updated[1]),
    }


_WALL_OFFER_GEM_COST   = 700
_WALL_OFFER_GOLD_GRANT = 16_500
_WALL_OFFER_STAGE_IDX  = 3   # player must currently be on board_stage == 3


@app.post("/shop/wall_offer_activate")
async def wall_offer_activate(request: Request):
    """Atomically redeems the Stage 3->4 wall offer.
    Requires player to be on board stage 3.
    Deducts 700 gems, grants 16,500 gold, then purchases the Stage 4 board
    (costs 15,000 gold) in a single transaction."""
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT total_money, gems_balance, board_stage FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return {"status": "error", "message": "Player not found"}
    gold, gems, board_stage = int(row[0]), int(row[1]), int(row[2])

    if board_stage != _WALL_OFFER_STAGE_IDX:
        conn.close()
        return {"status": "error", "message": "Wall offer only applies when on board stage 3"}
    if gems < _WALL_OFFER_GEM_COST:
        conn.close()
        return {"status": "error", "message": "Not enough gems for wall offer",
                "required": _WALL_OFFER_GEM_COST, "gems": gems}

    next_stage_idx = _WALL_OFFER_STAGE_IDX + 1
    next_stage     = BOARD_STAGES[next_stage_idx]
    board_cost     = int(next_stage["cost"])

    gold_after_grant    = gold + _WALL_OFFER_GOLD_GRANT
    gold_after_purchase = gold_after_grant - board_cost
    gems_after          = gems - _WALL_OFFER_GEM_COST

    cursor.execute(
        """UPDATE players
           SET gems_balance = ?, total_money = ?,
               board_stage = ?, board_size = ?
           WHERE player_id = ?""",
        (gems_after, gold_after_purchase,
         next_stage_idx, max(next_stage["rows"], next_stage["cols"]),
         player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":      "success",
        "board_stage": next_stage_idx,
        "board_rows":  next_stage["rows"],
        "board_cols":  next_stage["cols"],
        "new_balance": gold_after_purchase,
        "new_gems":    gems_after,
    }


@app.get("/player/offer_seen")
async def offer_seen(request: Request):
    """Returns whether a one-time offer has already been shown to this player.
    Query param: offer_id (e.g. 'board_stage4_wall')."""
    player_id = extract_player_id(request)
    offer_id  = str(request.query_params.get("offer_id", "")).strip()
    if not offer_id:
        return {"status": "error", "message": "offer_id is required"}
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute("SELECT seen_offers FROM players WHERE player_id = ?", (player_id,))
    row = cursor.fetchone()
    conn.close()
    seen_map = json.loads(row[0]) if row and row[0] else {}
    return {"status": "success", "seen": bool(seen_map.get(offer_id, False))}


@app.post("/player/mark_offer_seen")
async def mark_offer_seen(request: Request):
    """Marks a one-time offer as seen so it is never shown again.
    Body: {offer_id: string}."""
    player_id = extract_player_id(request)
    if not _check_rate_limit(player_id, "mark_offer_seen", min_interval_secs=2.0):
        return {"status": "error", "message": "Request too fast -- please wait."}
    try:
        body     = await request.json()
        offer_id = str(body.get("offer_id", "")).strip()
    except Exception:
        return {"status": "error", "message": "Invalid request body"}
    if not offer_id:
        return {"status": "error", "message": "offer_id is required"}
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute("SELECT seen_offers FROM players WHERE player_id = ?", (player_id,))
    row      = cursor.fetchone()
    seen_map = json.loads(row[0]) if row and row[0] else {}
    seen_map[offer_id] = 1
    cursor.execute(
        "UPDATE players SET seen_offers = ? WHERE player_id = ?",
        (json.dumps(seen_map), player_id)
    )
    conn.commit()
    conn.close()
    return {"status": "success", "offer_id": offer_id}


@app.post("/iap/purchase")
async def iap_purchase(request: Request):
    """
    Production IAP endpoint.  Receives {item_id} in the JSON body, validates
    against IAP_CATALOG, credits the player, and returns new balances.
    Replace the credit logic with real receipt validation before shipping.
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body    = await request.json()
        item_id = str(body.get("item_id", "")).strip()
    except Exception:
        return {"status": "error", "message": "Invalid JSON body"}

    if item_id not in IAP_CATALOG:
        return {"status": "error", "message": f"Unknown item_id: {item_id}"}

    platform   = str(body.get("platform", "ios")).lower()
    receipt    = str(body.get("receipt",  ""))
    product_id = item_id   # store product IDs match item_id; adjust if App Store Connect differs

    receipt_ok = await _validate_receipt(platform, item_id, receipt, product_id)
    if not receipt_ok:
        raise HTTPException(
            status_code=403,
            detail="Receipt validation failed -- purchase not credited"
        )

    item       = IAP_CATALOG[item_id]
    gems_grant = item["gems"]
    gold_grant = item["gold"]

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)

    cursor.execute(
        """UPDATE players
               SET gems_balance  = gems_balance  + ?,
                   total_money   = total_money   + ?
           WHERE player_id = ?""",
        (gems_grant, gold_grant, player_id)
    )

    # Vault Pass activation via IAP
    now_utc_iap = datetime.datetime.now(datetime.timezone.utc)
    if item.get("vault_pass"):
        vp_expiry = now_utc_iap + datetime.timedelta(days=item["days"])
        cursor.execute(
            "UPDATE players SET vault_pass_active = 1, vault_pass_expiry = ? "
            "WHERE player_id = ?",
            (vp_expiry.isoformat(), player_id)
        )

    # Exclusive cosmetic unlock via IAP (append to unlocked_cosmetics JSON array)
    excl_cosm = item.get("exclusive_cosmetic")
    if excl_cosm:
        cursor.execute(
            "SELECT unlocked_cosmetics FROM players WHERE player_id = ?", (player_id,)
        )
        uc_row = cursor.fetchone()
        try:
            uc_list = json.loads(uc_row[0]) if uc_row and uc_row[0] else []
        except Exception:
            uc_list = []
        if excl_cosm not in uc_list:
            uc_list.append(excl_cosm)
        cursor.execute(
            "UPDATE players SET unlocked_cosmetics = ? WHERE player_id = ?",
            (json.dumps(uc_list), player_id)
        )

    # Track lifetime revenue and log BI event
    price_usd = float(IAP_CATALOG.get(item_id, {}).get("price_usd", "0") or "0")
    if price_usd > 0:
        cursor.execute(
            "UPDATE players SET lifetime_iap_usd = lifetime_iap_usd + ? "
            "WHERE player_id = ?",
            (price_usd, player_id)
        )
    try:
        cursor.execute(
            "INSERT INTO telemetry_logs "
            "(player_id, event_name, event_data, session_id) "
            "VALUES (?, ?, ?, ?)",
            (player_id, "iap_purchase",
             json.dumps({"item_id": item_id,
                         "gems_granted": gems_grant,
                         "revenue_usd": price_usd,
                         "platform": platform}),
             "server")
        )
    except Exception:
        pass   # telemetry failure must never block a purchase

    cursor.execute(
        "SELECT gems_balance, total_money FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":       "success",
        "item_id":      item_id,
        "gems_granted": gems_grant,
        "gold_granted": gold_grant,
        "new_gems":     int(row[0]),
        "new_balance":  int(row[1]),
    }


@app.post("/iap/verify")
async def iap_verify(request: Request):
    """
    Full receipt-validation IAP endpoint with transaction-level idempotency.
    Body: {"platform": "apple"|"google", "receipt": str, "product_id": str}
    Returns: {"status": "ok", "gems_granted": N, "new_gem_balance": M}

    The receipt is verified against the platform store API (Apple verifyReceipt
    or Google Play Developer API v3).  The transaction_id returned by the store
    is written to iap_receipts; a second call with the same transaction_id is
    rejected with HTTP 409 to prevent replay attacks.

    IAP_SANDBOX=1 bypasses store API calls and accepts any receipt string.
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)

    try:
        body       = await request.json()
        platform   = str(body.get("platform",   "")).strip().lower()
        receipt    = str(body.get("receipt",     "")).strip()
        product_id = str(body.get("product_id", "")).strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if platform not in ("apple", "ios", "google", "android"):
        raise HTTPException(status_code=400,
                            detail="platform must be 'apple' or 'google'")
    if not receipt:
        raise HTTPException(status_code=400, detail="receipt field is required")
    if product_id not in IAP_CATALOG:
        raise HTTPException(status_code=400,
                            detail=f"Unknown product_id '{product_id}'")

    # -- Call the appropriate platform verifier --
    if platform in ("apple", "ios"):
        result = await _verify_apple_receipt(receipt, is_sandbox=_IAP_SANDBOX_MODE)
    else:
        creds: dict = {}
        if _GOOGLE_CREDS_STR:
            try:
                creds = json.loads(_GOOGLE_CREDS_STR)
            except Exception:
                raise HTTPException(status_code=500,
                                    detail="Server misconfiguration: "
                                           "GOOGLE_PLAY_CREDENTIALS_JSON is not valid JSON")
        result = await _verify_google_receipt(
            package_name=_GOOGLE_PLAY_PACKAGE,
            product_id=product_id,
            purchase_token=receipt,
            credentials_json=creds,
        )

    if not result.get("valid"):
        raise HTTPException(
            status_code=403,
            detail=f"Receipt validation failed: {result.get('error', 'unknown error')}"
        )

    transaction_id = str(result.get("transaction_id", "")).strip()
    if not transaction_id:
        raise HTTPException(status_code=403,
                            detail="Store returned no transaction_id -- cannot credit")

    # -- Idempotency: reject replayed transaction IDs before touching balances --
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM iap_receipts WHERE transaction_id = ?", (transaction_id,)
    )
    if cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=409,
                            detail="Transaction already processed (duplicate receipt)")

    get_or_create_player(player_id, cursor)

    item       = IAP_CATALOG[product_id]
    gems_grant = int(item.get("gems", 0))
    gold_grant = int(item.get("gold", 0))
    price_usd  = float(item.get("price_usd", "0") or "0")

    # IAP gems are not subject to the daily free-gem cap -- paid gems are always uncapped.
    cursor.execute(
        """UPDATE players
               SET gems_balance      = gems_balance      + ?,
                   total_money       = total_money       + ?,
                   lifetime_iap_usd  = lifetime_iap_usd  + ?
           WHERE player_id = ?""",
        (gems_grant, gold_grant, price_usd, player_id)
    )

    # Vault Pass activation if this product includes a subscription.
    if item.get("vault_pass"):
        vp_expiry = (datetime.datetime.now(datetime.timezone.utc)
                     + datetime.timedelta(days=int(item.get("days", 30))))
        cursor.execute(
            "UPDATE players SET vault_pass_active = 1, vault_pass_expiry = ? "
            "WHERE player_id = ?",
            (vp_expiry.isoformat(), player_id)
        )

    # Exclusive cosmetic unlock.
    excl_cosm = item.get("exclusive_cosmetic")
    if excl_cosm:
        cursor.execute(
            "SELECT unlocked_cosmetics FROM players WHERE player_id = ?", (player_id,)
        )
        uc_row = cursor.fetchone()
        try:
            uc_list = json.loads(uc_row[0]) if uc_row and uc_row[0] else []
        except Exception:
            uc_list = []
        if excl_cosm not in uc_list:
            uc_list.append(excl_cosm)
        cursor.execute(
            "UPDATE players SET unlocked_cosmetics = ? WHERE player_id = ?",
            (json.dumps(uc_list), player_id)
        )

    # Record receipt -- UNIQUE constraint on transaction_id prevents any race condition.
    cursor.execute(
        """INSERT INTO iap_receipts
               (player_id, transaction_id, product_id, platform, usd_amount, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (player_id, transaction_id, product_id, platform,
         price_usd, datetime.datetime.utcnow().isoformat())
    )

    try:
        cursor.execute(
            "INSERT INTO telemetry_logs (player_id, event_name, event_data, session_id) "
            "VALUES (?, ?, ?, ?)",
            (player_id, "iap_purchase",
             json.dumps({"product_id":     product_id,
                         "transaction_id": transaction_id,
                         "gems_granted":   gems_grant,
                         "revenue_usd":    price_usd,
                         "platform":       platform,
                         "source":         "iap_verify"}),
             "server")
        )
    except Exception:
        pass   # telemetry failure must never block a purchase

    cursor.execute(
        "SELECT gems_balance FROM players WHERE player_id = ?", (player_id,)
    )
    row = cursor.fetchone()
    new_gems = int(row[0]) if row and row[0] is not None else 0

    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":          "ok",
        "gems_granted":    gems_grant,
        "new_gem_balance": new_gems,
    }


@app.post("/iap/starter_pack")
async def purchase_starter_pack(request: Request):
    """
    One-time REAL-MONEY Starter Pack IAP ($0.99).  Validates the store receipt
    (same path as /iap/validate/google), enforces single-use via iap_receipts,
    then credits 250 gems + 1,000 gold and sets has_seen_starter_pack = 1.

    Body: {"product_id", "purchase_token", "package_name", "platform"}
    Dev bypass: any non-empty token is accepted when APP_ENV != "production".
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)

    try:
        body           = await request.json()
        purchase_token = str(body.get("purchase_token", "")).strip()
        platform       = str(body.get("platform", "android")).strip().lower()
        package_name   = str(body.get("package_name", _GOOGLE_PLAY_PACKAGE)).strip()
        product_id     = str(body.get("product_id", "starter_pack")).strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not purchase_token:
        raise HTTPException(status_code=400, detail="purchase_token is required")

    # --- Receipt validation (production only; dev accepts any non-empty token) ---
    app_env = os.environ.get("APP_ENV", "development").lower()
    if app_env == "production":
        if platform != "android":
            # iOS Starter Pack receipt validation is not wired yet -- refuse rather
            # than grant on an unvalidated token.  (Launch target is Google Play.)
            raise HTTPException(status_code=400, detail="starter_pack IAP is Android-only for now")
        try:
            creds: dict = json.loads(_GOOGLE_CREDS_STR) if _GOOGLE_CREDS_STR else {}
            result = await _verify_google_receipt(
                package_name=package_name,
                product_id=product_id,
                purchase_token=purchase_token,
                credentials_json=creds,
            )
            if not result.get("valid"):
                raise HTTPException(status_code=402, detail="receipt_invalid")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=402, detail="receipt_invalid")

    transaction_id = purchase_token   # Play uses the purchase token as the unique id
    conn   = get_connection()
    cursor = conn.cursor()

    # Idempotency: reject replayed receipts before touching balances.
    cursor.execute(
        "SELECT id FROM iap_receipts WHERE transaction_id = ?", (transaction_id,)
    )
    if cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Transaction already processed")

    get_or_create_player(player_id, cursor)

    # One-time gate: a player may only ever own the Starter Pack once.
    cursor.execute(
        "SELECT has_seen_starter_pack FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row and int(row[0]) == 1:
        conn.close()
        return {"status": "error", "message": "already_purchased"}

    STARTER_GEMS = 250
    STARTER_GOLD = 1_000
    STARTER_USD  = 0.99

    cursor.execute(
        """UPDATE players
               SET gems_balance          = gems_balance     + ?,
                   total_money           = total_money      + ?,
                   lifetime_iap_usd      = lifetime_iap_usd + ?,
                   has_seen_starter_pack = 1
           WHERE player_id = ?""",
        (STARTER_GEMS, STARTER_GOLD, STARTER_USD, player_id)
    )
    cursor.execute(
        """INSERT INTO iap_receipts
               (player_id, transaction_id, product_id, platform, usd_amount, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (player_id, transaction_id, product_id, platform, STARTER_USD,
         datetime.datetime.utcnow().isoformat())
    )
    cursor.execute(
        "SELECT gems_balance, total_money FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":       "success",
        "gems_granted": STARTER_GEMS,
        "gold_granted": STARTER_GOLD,
        "new_gems":     int(row[0]),
        "new_balance":  int(row[1]),
    }


@app.post("/shop/starter_pack")
async def shop_starter_pack(request: Request):
    """
    Main in-game Starter Pack endpoint — one-time offer shown after Run 2.
    Grants 150 gems + 5,000 gold.  Uses the same has_seen_starter_pack column as
    /iap/starter_pack so purchasing via either route permanently closes the popup.
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    conn.commit()

    cursor.execute(
        "SELECT has_seen_starter_pack FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    if row and int(row[0]) == 1:
        conn.close()
        return {"status": "error", "message": "already_purchased"}

    STARTER_GEMS = 150
    STARTER_GOLD = 5_000

    cursor.execute(
        """UPDATE players
               SET gems_balance          = gems_balance + ?,
                   total_money           = total_money  + ?,
                   has_seen_starter_pack = 1
           WHERE player_id = ?""",
        (STARTER_GEMS, STARTER_GOLD, player_id)
    )
    cursor.execute(
        "SELECT gems_balance, total_money FROM players WHERE player_id = ?",
        (player_id,)
    )
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":       "success",
        "gems_granted": STARTER_GEMS,
        "gold_granted": STARTER_GOLD,
        "new_gems":     int(row[0]),
        "new_balance":  int(row[1]),
    }


@app.post("/iap/vault_pass/activate")
async def activate_vault_pass(request: Request):
    """
    Activates the Vault Pass for the calling player for 30 days.
    Replace with real receipt validation before shipping.
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    now_utc   = datetime.datetime.utcnow()
    expiry    = now_utc + datetime.timedelta(days=30)

    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "UPDATE players SET vault_pass_active = 1, vault_pass_expiry = ? WHERE player_id = ?",
        (expiry.isoformat(), player_id)
    )
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)

    return {
        "status":             "success",
        "vault_pass_active":  True,
        "vault_pass_expiry":  expiry.isoformat(),
        "vault_pass_days_left": 30,
    }


@app.get("/event/shop")
async def event_shop(request: Request):
    """
    Returns the current rotating shard event shop (3 hardcoded items) plus the
    player's shard balance.  No real rotation logic yet; the item list is static.
    """
    player_id = extract_player_id(request)
    conn   = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "SELECT shards_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    row    = cursor.fetchone()
    conn.close()
    shards = int(row[0]) if row and row[0] is not None else 0
    return {
        "status":         "ok",
        "shards_balance": shards,
        "items":          _EVENT_SHOP_ITEMS,
    }


# ============================================================================
#  Sprint 7.2 — Gem Bundle IAP (catalog + receipt validation + credit helper)
# ============================================================================

def _credit_gem_bundle(player_id: str, product_id: str, cursor) -> Dict[str, Any]:
    """
    Credit a gem bundle purchase to the player's account.
    Handles first-purchase 10x bonus, sales_log insert, and cache invalidation.
    Caller is responsible for conn.commit().
    """
    bundle = _GEM_BUNDLES_BY_ID.get(product_id)
    if bundle is None:
        raise ValueError(f"Unknown gem bundle product_id: {product_id}")

    gems       = int(bundle["gems"])
    gold       = int(bundle.get("gold", 0))
    price_usd  = float(bundle["usd"])

    # First-purchase bonus removed: frontend bundle cards show exact gem amounts and
    # the server must credit exactly those amounts (frontend is source of truth).
    bonus_gems     = 0
    first_purchase = False
    total_gems     = gems

    cursor.execute(
        """UPDATE players
               SET gems_balance      = COALESCE(gems_balance,     0) + ?,
                   total_money       = COALESCE(total_money,      0) + ?,
                   lifetime_iap_usd  = COALESCE(lifetime_iap_usd, 0.0) + ?,
                   first_iap_done    = 1
           WHERE player_id = ?""",
        (total_gems, gold, price_usd, player_id)
    )

    # Sales log entry for the base purchase.
    # item_level=0 for IAP bundles (column is INTEGER; product_id is stored in the sales log
    # only for gem merges where a numeric tier makes sense).
    cursor.execute(
        "INSERT INTO sales_log (seller, item_level, profit_made) VALUES (?, ?, ?)",
        (player_id, 0, price_usd)
    )

    # Bonus log entry retained as dead branch so the audit trail code compiles.
    if first_purchase and bonus_gems > 0:
        cursor.execute(
            "INSERT INTO sales_log (seller, item_level, profit_made) VALUES (?, ?, ?)",
            (player_id, 0, 0.0)
        )

    cursor.execute(
        "SELECT gems_balance FROM players WHERE player_id = ?", (player_id,)
    )
    row_bal   = cursor.fetchone()
    new_gems  = int(row_bal[0]) if row_bal and row_bal[0] is not None else 0

    _invalidate_balance_cache(player_id)

    result: Dict[str, Any] = {
        "gems_credited":       gems,
        "gold_credited":       gold,
        "new_gems_balance":    new_gems,
        "first_purchase_bonus": first_purchase,
        "bonus_gems":          bonus_gems,
    }
    return result


@app.get("/shop/iap_catalog")
async def iap_catalog(request: Request):
    """
    Returns the gem bundle catalog.  No auth required.
    Authenticated callers also receive first_purchase_available.
    """
    first_purchase_available = False
    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header.startswith("Bearer "):
        try:
            player_id, _ = extract_auth(request)
            conn_cat   = get_connection()
            cur_cat    = conn_cat.cursor()
            cur_cat.execute(
                "SELECT first_iap_done FROM players WHERE player_id = ?", (player_id,)
            )
            row_cat = cur_cat.fetchone()
            conn_cat.close()
            fip = int(row_cat[0]) if row_cat and row_cat[0] is not None else 0
            first_purchase_available = (fip == 0)
        except Exception:
            pass
    return {
        "bundles":                  GEM_BUNDLES,
        "first_purchase_available": first_purchase_available,
    }


# ----------------------------------------------------------------------------
#  Piggy->IAP bridge (Sprint 7): the piggy_instant_199 CONSUMABLE performs a
#  FULL piggy smash (cash-paid) instead of crediting gems. Reuses the same DB
#  economy as /piggy/smash (gems tier) -- minus the gem cost and minus the
#  daily-smash gate (it's a paid bypass, surfaced only when the piggy is full
#  AND the free daily smashes are exhausted). Standard gem purchases are
#  completely unaffected.
# ----------------------------------------------------------------------------
_PIGGY_INSTANT_PRODUCT = "piggy_instant_199"
_PIGGY_INSTANT_USD     = 1.99


def _execute_piggy_instant_smash(player_id: str, cursor) -> dict:
    cursor.execute(
        "SELECT piggy_balance, mastery_state FROM players WHERE player_id = ?",
        (player_id,)
    )
    row   = cursor.fetchone()
    piggy = int(row[0]) if row and row[0] is not None else 0
    _ms   = _safe_parse_mastery(row[1] if row else "{}")
    _cashout_multiplier = 1.0 + int(_ms.get("econ_cashout_bonus", 0)) * 0.05
    reward = int(piggy * _cashout_multiplier)   # full smash (same tier as the gems-paid smash)
    cursor.execute(
        """UPDATE players SET
               total_money          = total_money + ?,
               piggy_balance        = 0,
               total_piggy_smashes  = COALESCE(total_piggy_smashes, 0) + 1,
               total_piggy_earnings = COALESCE(total_piggy_earnings, 0) + ?
           WHERE player_id = ?""",
        (reward, reward, player_id)
    )
    cursor.execute(
        "SELECT total_money, gems_balance FROM players WHERE player_id = ?",
        (player_id,)
    )
    upd = cursor.fetchone()
    _invalidate_balance_cache(player_id)
    # gems_credited:0 keeps the client's gem-purchase dispatch happy (status 'ok').
    return {
        "gems_credited":  0,
        "bonus_gems":     0,
        "piggy_smash":    True,
        "reward_amount":  reward,
        "wallet_balance": int(upd[0]) if upd else 0,
        "gems_balance":   int(upd[1]) if upd else 0,
        "piggy_balance":  0,
    }


@app.post("/iap/validate/google")
async def iap_validate_google(request: Request):
    """
    Validates a Google Play purchase receipt and credits the gem bundle.
    Body: {"purchase_token": str, "product_id": str, "package_name": str}
    Idempotency enforced via UNIQUE(transaction_id) in iap_receipts.
    Dev bypass: any non-empty token is treated as valid when APP_ENV != "production".
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)

    try:
        body           = await request.json()
        purchase_token = str(body.get("purchase_token", "")).strip()
        product_id     = str(body.get("product_id",     "")).strip()
        package_name   = str(body.get("package_name",   _GOOGLE_PLAY_PACKAGE)).strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not purchase_token:
        raise HTTPException(status_code=400, detail="purchase_token is required")
    if product_id not in _GEM_BUNDLES_BY_ID and product_id != _PIGGY_INSTANT_PRODUCT:
        raise HTTPException(status_code=400, detail=f"Unknown product_id: {product_id}")

    app_env = os.environ.get("APP_ENV", "development").lower()

    # --- Receipt validation ---
    if app_env == "production":
        try:
            creds: dict = {}
            if _GOOGLE_CREDS_STR:
                creds = json.loads(_GOOGLE_CREDS_STR)
            result = await _verify_google_receipt(
                package_name=package_name,
                product_id=product_id,
                purchase_token=purchase_token,
                credentials_json=creds,
            )
            if not result.get("valid"):
                raise HTTPException(status_code=402, detail="receipt_invalid")
        except HTTPException:
            raise
        except Exception as _ge:
            raise HTTPException(status_code=402, detail="receipt_invalid")
    # Dev bypass: non-empty token accepted

    transaction_id = purchase_token   # Play uses purchase_token as unique identifier
    conn   = get_connection()
    cursor = conn.cursor()

    # Idempotency check
    cursor.execute(
        "SELECT id FROM iap_receipts WHERE transaction_id = ?", (transaction_id,)
    )
    if cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Transaction already processed")

    get_or_create_player(player_id, cursor)

    if product_id == _PIGGY_INSTANT_PRODUCT:
        # Cash-paid full piggy smash instead of gem crediting.
        credit_result = _execute_piggy_instant_smash(player_id, cursor)
        price_usd     = _PIGGY_INSTANT_USD
    else:
        try:
            credit_result = _credit_gem_bundle(player_id, product_id, cursor)
        except ValueError as _ve:
            conn.close()
            raise HTTPException(status_code=400, detail=str(_ve))
        bundle    = _GEM_BUNDLES_BY_ID[product_id]
        price_usd = float(bundle["usd"])

    cursor.execute(
        """INSERT INTO iap_receipts
               (player_id, transaction_id, product_id, platform, usd_amount, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (player_id, transaction_id, product_id, "google",
         price_usd, datetime.datetime.utcnow().isoformat())
    )

    conn.commit()
    conn.close()

    return {"status": "ok", **credit_result}


@app.post("/iap/validate/apple")
async def iap_validate_apple(request: Request):
    """
    Validates an Apple App Store receipt and credits the gem bundle.
    Body: {"transaction_id": str, "product_id": str}
    Idempotency enforced via UNIQUE(transaction_id) in iap_receipts.
    Dev bypass: any non-empty transaction_id is treated as valid when APP_ENV != "production".
    """
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)

    try:
        body           = await request.json()
        transaction_id = str(body.get("transaction_id", "")).strip()
        product_id     = str(body.get("product_id",     "")).strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not transaction_id:
        raise HTTPException(status_code=400, detail="transaction_id is required")
    if product_id not in _GEM_BUNDLES_BY_ID and product_id != _PIGGY_INSTANT_PRODUCT:
        raise HTTPException(status_code=400, detail=f"Unknown product_id: {product_id}")

    app_env = os.environ.get("APP_ENV", "development").lower()

    # --- Receipt validation ---
    if app_env == "production":
        try:
            result = await _verify_apple_receipt(transaction_id, is_sandbox=_IAP_SANDBOX_MODE)
            if not result.get("valid"):
                raise HTTPException(status_code=402, detail="receipt_invalid")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=402, detail="receipt_invalid")
    # Dev bypass: non-empty transaction_id accepted

    conn   = get_connection()
    cursor = conn.cursor()

    # Idempotency check
    cursor.execute(
        "SELECT id FROM iap_receipts WHERE transaction_id = ?", (transaction_id,)
    )
    if cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Transaction already processed")

    get_or_create_player(player_id, cursor)

    if product_id == _PIGGY_INSTANT_PRODUCT:
        # Cash-paid full piggy smash instead of gem crediting.
        credit_result = _execute_piggy_instant_smash(player_id, cursor)
        price_usd     = _PIGGY_INSTANT_USD
    else:
        try:
            credit_result = _credit_gem_bundle(player_id, product_id, cursor)
        except ValueError as _ve:
            conn.close()
            raise HTTPException(status_code=400, detail=str(_ve))
        bundle    = _GEM_BUNDLES_BY_ID[product_id]
        price_usd = float(bundle["usd"])

    cursor.execute(
        """INSERT INTO iap_receipts
               (player_id, transaction_id, product_id, platform, usd_amount, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (player_id, transaction_id, product_id, "apple",
         price_usd, datetime.datetime.utcnow().isoformat())
    )

    conn.commit()
    conn.close()

    return {"status": "ok", **credit_result}


@app.post("/dev/mock_iap_purchase")
async def mock_iap_purchase(request: Request):
    """
    DEV-ONLY endpoint: simulates an in-app purchase by crediting gems directly.
    Remove or gate behind an env flag before shipping to production.
    Expected body: {"amount": 100}
    """
    if os.environ.get("APP_ENV", "development").lower() == "production":
        raise HTTPException(status_code=404)
    player_id = extract_player_id(request)
    try:
        body = await request.json()
        amount = int(body.get("amount", 0))
    except Exception:
        return {"status": "error", "message": "Invalid JSON body"}

    if amount <= 0:
        return {"status": "error", "message": "amount must be positive"}

    # Optional item_id for revenue tracking (does not affect gems credited)
    mock_item_id = str(body.get("item_id", "")).strip()

    conn = get_connection()
    cursor = conn.cursor()
    get_or_create_player(player_id, cursor)
    cursor.execute(
        "UPDATE players SET gems_balance = gems_balance + ? WHERE player_id = ?",
        (amount, player_id)
    )
    # Track revenue if a known item_id was provided
    if mock_item_id and mock_item_id in IAP_CATALOG:
        price_usd = float(IAP_CATALOG[mock_item_id].get("price_usd", "0") or "0")
        if price_usd > 0:
            cursor.execute(
                "UPDATE players SET lifetime_iap_usd = lifetime_iap_usd + ? "
                "WHERE player_id = ?",
                (price_usd, player_id)
            )
    cursor.execute("SELECT gems_balance FROM players WHERE player_id = ?", (player_id,))
    new_gems = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    _invalidate_balance_cache(player_id)
    return {"status": "success", "new_gems": new_gems}


# ============================================================================
#  Telemetry
# ============================================================================

@app.post("/api/telemetry")
async def batch_telemetry(request: Request):
    """
    Batch telemetry sink. Body: list of {event_name, event_data, ts?}.
    Always returns 200 so the client never surfaces errors to the player.
    """
    player_id = extract_player_id(request)
    try:
        events = await request.json()
        if not isinstance(events, list):
            return {"status": "ok", "inserted": 0}
    except Exception:
        return {"status": "ok", "inserted": 0}

    rows = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        name = str(ev.get("event_name", "unknown"))[:128]
        data = ev.get("event_data", {})
        rows.append((player_id, name, json.dumps(data, ensure_ascii=False)))

    if rows:
        try:
            conn = get_connection()
            conn.executemany(
                "INSERT INTO telemetry_logs (player_id, event_name, event_data) VALUES (?, ?, ?)",
                rows,
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    return {"status": "ok", "inserted": len(rows)}


@app.post("/telemetry/log")
async def telemetry_log(request: Request):
    """
    Fire-and-forget telemetry sink.
    Expected body: {"event_name": "...", "event_data": {...}}
    Player ID is resolved from the Authorization: Bearer JWT (same as all other endpoints).
    Always returns 200 so the client never retries on failure.
    """
    player_id = extract_player_id(request)
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    event_name = str(body.get("event_name", "unknown"))
    event_data = body.get("event_data", {})
    event_data_json = json.dumps(event_data, ensure_ascii=False)

    try:
        conn = get_connection()
        conn.execute(
            "INSERT INTO telemetry_logs (player_id, event_name, event_data) VALUES (?, ?, ?)",
            (player_id, event_name, event_data_json),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # never surface telemetry errors to the client

    return {"status": "ok"}


@app.post("/telemetry/batch")
async def telemetry_batch(request: Request):
    """
    Batch telemetry sink -- persists every event to telemetry_logs.
    Body: {"session_id": "...", "events": [{"event": "...", "ts": 0, "params": {...}}]}
    Always returns 200 so the client never retries on failure.
    """
    player_id = extract_player_id(request)
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    events     = body.get("events", [])
    session_id = body.get("session_id", None)
    platform    = str(body.get("platform",    "unknown"))[:32]
    app_version = str(body.get("app_version", "1.0.0"))[:16]
    country     = str(body.get("country",     "XX"))[:8]
    if not events:
        return {"status": "ok"}

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        for ev in events:
            event_name = str(ev.get("event", ev.get("event_name", "")))
            event_data = json.dumps(ev.get("params", ev.get("event_data", {})))
            cursor.execute(
                "INSERT INTO telemetry_logs "
                "(player_id, event_name, event_data, session_id, "
                " client_platform, client_version, country_code) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (player_id, event_name, event_data, session_id,
                 platform, app_version, country)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[telemetry/batch] DB error: {e}")

    return {"status": "ok"}


@app.post("/telemetry/crash")
async def telemetry_crash(request: Request):
    """
    Client-side crash / HTTP-error report.
    Body: {"context": str, "error": str, "breadcrumbs": [...], "platform": str, "build": str}
    Always returns 200 -- must never block the game.
    """
    player_id = extract_player_id(request)
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    event_data = {
        "context":     str(body.get("context",   ""))[:128],
        "error":       str(body.get("error",     ""))[:512],
        "breadcrumbs": body.get("breadcrumbs", []),
        "platform":    str(body.get("platform",  ""))[:32],
        "build":       str(body.get("build",     ""))[:32],
    }
    try:
        conn = get_connection()
        conn.execute(
            "INSERT INTO telemetry_logs (player_id, event_name, event_data) VALUES (?, ?, ?)",
            (player_id, "client_crash", json.dumps(event_data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
    except Exception as _e:
        print(f"[telemetry/crash] DB error: {_e}")

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Structured telemetry event sink.
# Client sends one event at a time; server validates against the allowlist and
# writes to telemetry_events.  Always returns 200 -- never blocks the game.
# ---------------------------------------------------------------------------

_TELEMETRY_ALLOWLIST: set = {
    # CLAUDE.md planned events
    "merge_event", "shop_transaction", "booster_used",
    "session_start", "session_length",
    # New structured events added for BI pipeline
    "iap_purchase", "ad_watched",
    "offer_shown", "offer_accepted", "offer_declined",
    "cashout_triggered", "near_miss_detected", "spin_completed",
    # Legacy events from the existing log_event batch system (retained for
    # cross-table join consistency -- the batch system still writes telemetry_logs)
    "cashout", "first_cashout", "run_end", "run_started",
    "game_over", "app_session_start", "lucky_spin",
}


@app.post("/telemetry/event")
async def telemetry_event(request: Request):
    """Single-event structured telemetry sink with allowlist validation.
    Body: {event_type: str, params: dict, client_timestamp: float}
    Always returns 200 -- telemetry failure must never surface to the player."""
    player_id = extract_player_id(request)
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    event_type       = str(body.get("event_type", "")).strip()[:128]
    params           = body.get("params", {})
    client_timestamp = body.get("client_timestamp", None)

    if event_type not in _TELEMETRY_ALLOWLIST:
        # Unknown event type: silently drop -- never 4xx the client for telemetry.
        return {"status": "ok"}

    if not isinstance(params, dict):
        params = {}

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO telemetry_events "
            "(player_id, event_type, params_json, client_timestamp) "
            "VALUES (?, ?, ?, ?)",
            (player_id, event_type,
             json.dumps(params, ensure_ascii=False),
             float(client_timestamp) if client_timestamp is not None else None)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[telemetry/event] DB error: {e}")

    return {"status": "ok"}


# ============================================================================
#  QA Hard Reset  --  debug / testing only; wipes all server state for a player
# ============================================================================

@app.post("/api/qa/reset")
async def qa_hard_reset(request: Request):
    """
    Wipe every server-side record for the calling player so the next launch
    starts a 100% fresh FTUE.  Intended for QA / FTUE smoke-testing only.
    The client deletes its local identity file and quits immediately after
    calling this endpoint, so the next run generates a brand-new player_id.
    Disabled in production: returns 404 so the route is not discoverable.
    """
    if os.environ.get("APP_ENV", "development").lower() == "production":
        raise HTTPException(status_code=404)
    player_id = extract_player_id(request)
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        # Delete child rows first (FK order), then the player root row.
        cursor.execute("DELETE FROM achievements  WHERE player_id = ?", (player_id,))
        cursor.execute("DELETE FROM player_quests WHERE player_id = ?", (player_id,))
        cursor.execute("DELETE FROM telemetry_logs WHERE player_id = ?", (player_id,))
        cursor.execute("DELETE FROM sales_log     WHERE seller     = ?", (player_id,))
        cursor.execute("DELETE FROM players       WHERE player_id  = ?", (player_id,))
        conn.commit()
        conn.close()
    except Exception as exc:
        # Never crash the server over a QA wipe; log and return error body.
        print(f"[qa/reset] ERROR for {player_id}: {exc}")
        return {"status": "error", "message": str(exc)}
    print(f"[qa/reset] Wiped all data for player: {player_id}")
    return {"status": "ok", "player_id": player_id}


# ---------------------------------------------------------------------------
#  Run Autosave  (P0-S1 to P0-S4 client feature)
#
#  POST /runs/save_state   -- upsert the current run snapshot
#  GET  /runs/active_state -- return the snapshot or {"status":"none"}
#  POST /runs/clear_state  -- delete the snapshot (called on clean game-over)
#
#  Security model: player identity is extracted from the Authorization: Bearer
#  JWT, which is verified on every endpoint.  All three operations are
#  hard-scoped to that player_id, so one player can never read or overwrite
#  another player's save.
# ---------------------------------------------------------------------------

# Sanity caps for the incoming payload (mirrors anti-cheat constants above).
_SAVE_MAX_BOARD_CELLS  = 36    # 6x6 hard ceiling
_SAVE_VALID_CELL_RANGE = (-1, 99)   # -1 = cursed, 1-7 gems, 98/99 specials
_SAVE_MAX_TOOL_USES    = 2_000      # absurd ceiling; blocks crafted exploits


@app.post("/runs/save_state")
async def save_run_state(request: Request, payload: Dict[str, Any] = Body(...)):
    player_id = extract_player_id(request)

    # ---- Validate board_data ----
    board = payload.get("board_data")
    if not isinstance(board, list):
        raise HTTPException(status_code=422, detail="board_data must be a list")
    if len(board) == 0 or len(board) > _SAVE_MAX_BOARD_CELLS:
        raise HTTPException(status_code=422, detail=f"board_data length out of range (1-{_SAVE_MAX_BOARD_CELLS})")
    for cell in board:
        if not isinstance(cell, int) or not (_SAVE_VALID_CELL_RANGE[0] <= cell <= _SAVE_VALID_CELL_RANGE[1]):
            raise HTTPException(status_code=422, detail=f"Invalid cell value: {cell}")

    # ---- Validate scalar fields ----
    survival = payload.get("survival_time", 0)
    if not isinstance(survival, (int, float)) or survival < 0 or survival > _MAX_SURVIVAL_SECS:
        raise HTTPException(status_code=422, detail="survival_time out of valid range")

    for key in ("run_hammer_uses", "run_crystal_uses"):
        val = payload.get(key, 0)
        if not isinstance(val, int) or val < 0 or val > _SAVE_MAX_TOOL_USES:
            raise HTTPException(status_code=422, detail=f"{key} out of valid range")

    # ---- Build the stored snapshot.
    # wallet_balance and max_unlocked_tier are NOT trusted from the client;
    # the canonical values live in players.total_money and players.max_unlocked_tier.
    # We store them as-is for reference, but _on_run_state_loaded() on the client
    # deliberately does not restore these two fields from the snapshot.
    state_json = json.dumps({
        "board_data":        board,
        "survival_time":     float(survival),
        "max_unlocked_tier": int(payload.get("max_unlocked_tier", 0)),
        "run_hammer_uses":   int(payload.get("run_hammer_uses", 0)),
        "run_crystal_uses":  int(payload.get("run_crystal_uses", 0)),
    }, separators=(",", ":"))

    conn = get_connection()
    try:
        cursor = conn.cursor()
        get_or_create_player(player_id, cursor)
        cursor.execute("""
            INSERT INTO run_states (player_id, state_json, saved_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(player_id) DO UPDATE SET
                state_json = excluded.state_json,
                saved_at   = excluded.saved_at
        """, (player_id, state_json))
        conn.commit()
    finally:
        conn.close()

    return {"status": "success"}


@app.get("/runs/active_state")
def get_active_run_state(request: Request):
    player_id = extract_player_id(request)

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT state_json, saved_at FROM run_states WHERE player_id = ?",
            (player_id,)
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if not row:
        return {"status": "none"}

    try:
        state = json.loads(row[0])
    except Exception:
        # Corrupted snapshot -- treat as no save so the client starts fresh.
        return {"status": "none"}

    state["status"] = "ok"
    # Expose the DB-stamped save time so the client can compute idle duration
    # for the session_resume telemetry event.
    if row[1]:
        state["saved_at"] = str(row[1])
    return state


@app.post("/runs/clear_state")
async def clear_run_state(request: Request):
    player_id = extract_player_id(request)

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM run_states WHERE player_id = ?", (player_id,))
        conn.commit()
    finally:
        conn.close()

    return {"status": "success"}


# ============================================================================
#  ELITE ADAPTATION ENDPOINTS (Sprints 1-7)
#  Appended block. Reuses the EXISTING security path (extract_auth /
#  extract_player_id / _verify_financial_signature) -- no parallel auth system.
#  Content + per-player state live in elite_content.py (in-memory MOCK; swap the
#  helpers for DB queries later). JSON shapes match docs/SERVER_CONTRACT_*.md.
#  GET routes require a valid JWT; mutating routes are HMAC-signed exactly like
#  /bank/deposit and /shop/buy_boost.
# ============================================================================
import elite_content as _elite


@app.get("/meta/catalog")
def elite_meta_catalog(request: Request):
    extract_player_id(request)            # require a valid Bearer JWT
    return _elite.meta_catalog()


@app.get("/meta/chapter_state")
def elite_chapter_state(request: Request):
    return _elite.chapter_state(extract_player_id(request))


@app.post("/meta/complete_chapter")
async def elite_complete_chapter(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    return _elite.complete_chapter(player_id, str(body.get("chapter_id", "")))


@app.get("/estate/state")
def elite_estate_state(request: Request):
    return _elite.estate_state(extract_player_id(request))


@app.post("/estate/fund_task")
async def elite_fund_task(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    return _elite.fund_task(player_id, str(body.get("task_id", "")), int(body.get("cost", 0)))


@app.post("/estate/finish_room")
async def elite_finish_room(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    return _elite.finish_room(player_id, str(body.get("room_id", "")))


@app.get("/album/state")
def elite_album_state(request: Request):
    return _elite.album_state(extract_player_id(request))


@app.post("/album/claim_discovery")
async def elite_claim_discovery(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    return _elite.claim_discovery(player_id, int(body.get("value", 0)))


@app.get("/events/active")
def elite_events_active(request: Request):
    return _elite.active_events(extract_player_id(request))


@app.post("/events/claim_milestone")
async def elite_claim_milestone(request: Request):
    player_id, raw_token = extract_auth(request)
    await _verify_financial_signature(request, raw_token)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    return _elite.claim_milestone(player_id, str(body.get("event_id", "")), int(body.get("threshold", 0)))


# ============================================================================
#  COMPAT STUBS (full-stack 404 sweep): client endpoints that had NO server route.
#  Safe JWT-only stubs returning the EXACT shapes the client dispatch expects, so
#  they stop 404ing. These are STUBS (empty/zero) -- replace with real impls
#  (leaderboard ranking, new-player bonus, legacy event shards). The client sends
#  NO X-Signature on these, so they use extract_player_id only (no HMAC verify).
# ============================================================================
@app.get("/leaderboard/weekly")
def compat_leaderboard_weekly(request: Request):
    extract_player_id(request)
    return {"status": "ok", "leaderboard": []}


@app.post("/leaderboard/claim")
async def compat_leaderboard_claim(request: Request):
    extract_player_id(request)
    return {"status": "ok", "gems_awarded": 0}


@app.get("/leaderboard/rival")
def compat_leaderboard_rival(request: Request):
    extract_player_id(request)
    return {"status": "ok"}


@app.post("/events/claim_shards")
async def compat_claim_event_shards(request: Request):
    extract_player_id(request)
    return {"status": "ok", "gold_awarded": 0, "shards_remaining": 0}


@app.post("/player/claim_new_player_gem_bonus")
async def compat_claim_new_player_gem_bonus(request: Request):
    extract_player_id(request)
    return {"status": "ok", "gems_awarded": 0}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
# if __name__ == "__main__":
#     # Local development only. For production use the Procfile command below.
#     # Procfile: web: uvicorn server:app --host 0.0.0.0 --port $PORT --workers 2
#     uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
