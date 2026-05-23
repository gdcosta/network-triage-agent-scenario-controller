"""
Kinetic Leisure — Scenario Controller (Multi-Store)
Sends synthetic Splunk HEC events for the KL retail fleet with
per-store toggleable fault injection.

Usage:
    pip install -r requirements.txt
    python app.py

    Then open http://localhost:8080
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import random
import ssl
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scenario-controller")

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# HEC Log Handler
# ---------------------------------------------------------------------------
LOG_SOURCETYPE = "kl:scenario_controller:log"
LOG_SOURCE = "scenario_controller"
LOG_HOST = "kl-scenario-controller"

_log_buffer: deque = deque(maxlen=500)


class HecLogHandler(logging.Handler):
    """Buffers log records for periodic flush to Splunk HEC."""

    def emit(self, record: logging.LogRecord):
        if record.name in ("httpx", "httpcore", "uvicorn.access"):
            return
        try:
            _log_buffer.append({
                "time": str(record.created),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            })
        except Exception:
            pass


hec_handler = HecLogHandler()
hec_handler.setLevel(logging.INFO)
hec_handler.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(hec_handler)


async def flush_logs_to_hec(client: httpx.AsyncClient):
    """Send buffered log records to Splunk HEC."""
    if not state.hec_logging_enabled:
        _log_buffer.clear()
        return
    if not state.hec_url or not state.hec_token or not _log_buffer:
        return

    url = _hec_event_url()
    headers = {"Authorization": f"Splunk {state.hec_token}"}

    batch = []
    while _log_buffer:
        try:
            batch.append(_log_buffer.popleft())
        except IndexError:
            break

    for entry in batch:
        payload = {
            "time": entry["time"],
            "index": state.hec_index,
            "sourcetype": LOG_SOURCETYPE,
            "source": LOG_SOURCE,
            "host": LOG_HOST,
            "event": entry,
        }
        try:
            await client.post(url, json=payload, headers=headers, timeout=5.0)
        except Exception:
            pass


def _hec_event_url() -> str:
    """Normalise HEC URL to end with /services/collector/event."""
    url = state.hec_url.rstrip("/")
    if not url.endswith("/services/collector/event"):
        if url.endswith("/services/collector"):
            url += "/event"
        else:
            url += "/services/collector/event"
    return url


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
TEMPLATES_PATH = BASE_DIR / "event-templates.json"
STORES_PATH = BASE_DIR / "stores.json"

# ---------------------------------------------------------------------------
# Template Hydration
# ---------------------------------------------------------------------------

def hydrate_string(s: str, store: dict) -> str:
    """Replace tokens in a template string with store-specific values."""
    s = s.replace("__NWID__", f"N_KL{store['number']:07d}")
    s = s.replace("__S6__", f"{store['number']:06d}")
    s = s.replace("__S3__", f"{store['number']:03d}")
    s = s.replace("__SN__", str(store['number']))
    s = s.replace("__CC__", store['city_code'])
    s = s.replace("__cc__", store['city_code'].lower())
    s = s.replace("__CITY__", store['city'])
    s = s.replace("__IPO__", str(store['ip_octet']))
    s = s.replace("__MAC__", store['mac_octet'])
    return s


def hydrate_templates(templates: dict, store: dict) -> dict:
    """Deep-copy templates and hydrate all template/host_override strings."""
    result = copy.deepcopy(templates)

    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("template", "host_override") and isinstance(v, str):
                    obj[k] = hydrate_string(v, store)
                else:
                    walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(result)

    # Filter _comment entries from healthy events
    if "healthy" in result and "events" in result["healthy"]:
        result["healthy"]["events"] = [
            e for e in result["healthy"]["events"]
            if isinstance(e, dict) and "id" in e
        ]

    return result


# ---------------------------------------------------------------------------
# Runtime Variance Tokens
# ---------------------------------------------------------------------------
# Replaced at send time with random values to create natural metric
# fluctuation for ITSI adaptive thresholds and realistic baselines.

VARIANCE_TOKENS: dict[str, tuple[float, float, int]] = {
    # Token name      → (min, max, decimal_places)
    "%%JITTER%%":      (2, 8, 0),
    "%%LATENCY%%":     (10, 25, 0),
    "%%VQOE%%":        (7, 10, 0),
    "%%TX_OCT%%":      (2_000_000, 6_000_000, 0),
    "%%RX_OCT%%":      (1_500_000, 4_500_000, 0),
    "%%TX_PKT%%":      (15_000, 35_000, 0),
    "%%RX_PKT%%":      (10_000, 25_000, 0),
    "%%RESP_MS%%":     (30, 65, 0),
    "%%HOP_LAT%%":     (4, 12, 0),
    "%%ISP_LAT%%":     (3, 9, 0),
    "%%RSSI_GOOD%%":   (-55, -45, 0),
    "%%RSSI_OK%%":     (-68, -60, 0),
    "%%POE_USED%%":    (120, 280, 0),
    "%%UPTIME%%":      (86_400, 604_800, 0),
    "%%REACH_PCT%%":   (99.5, 100.0, 1),
    "%%PATH_CHG%%":    (0, 2, 0),
    "%%LOSS_ZERO%%":   (0.0, 0.3, 1),
}


def replace_variance_tokens(raw: str) -> str:
    """Replace %%TOKEN%% placeholders with random values."""
    for token, (lo, hi, decimals) in VARIANCE_TOKENS.items():
        while token in raw:
            if decimals == 0:
                val = str(random.randint(int(lo), int(hi)))
            else:
                val = f"{random.uniform(lo, hi):.{decimals}f}"
            raw = raw.replace(token, val, 1)  # one at a time for unique values
    return raw


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class AppState:
    """Mutable application state shared across the event loop and API."""

    def __init__(self):
        # HEC connection
        self.hec_url: str = ""
        self.hec_token: str = ""
        self.hec_index: str = "main"
        self.hec_verify_ssl: bool = False

        # Engine
        self.sending_enabled: bool = False
        self.healthy_interval: float = 3.0
        self.hec_logging_enabled: bool = False

        # Stores
        self.stores: list[dict] = []
        self.store_map: dict[int, dict] = {}  # number → store config

        # Templates (raw tokenized)
        self.raw_templates: dict = {}
        # Per-store hydrated templates: store_number → {"healthy": {...}, "faults": {...}}
        self.store_templates: dict[int, dict] = {}
        # Mutex groups from templates
        self.mutex_groups: dict[str, list[str]] = {}

        # Fault toggles — keyed by "store_number:fault_name"
        self.active_faults: dict[str, bool] = {}
        self.fired_phases: dict[str, set[int]] = {}
        # Fault start timestamps
        self.fault_starts: dict[str, float] = {}

        # Per-store enabled flag (all stores enabled by default)
        self.stores_enabled: dict[int, bool] = {}

        # Counters
        self.events_sent: int = 0
        self.errors: int = 0
        self.last_event_time: Optional[str] = None
        self.last_error: Optional[str] = None

    def _fault_key(self, store_number: int, fault_name: str) -> str:
        return f"{store_number}:{fault_name}"

    def load_config(self):
        """Load persisted config from config.json or env vars."""
        if CONFIG_PATH.exists():
            try:
                cfg = json.loads(CONFIG_PATH.read_text())
                self.hec_url = cfg.get("hec_url", self.hec_url)
                self.hec_token = cfg.get("hec_token", self.hec_token)
                self.hec_index = cfg.get("hec_index", self.hec_index)
                self.hec_verify_ssl = cfg.get("hec_verify_ssl", self.hec_verify_ssl)
                self.hec_logging_enabled = cfg.get("hec_logging_enabled",
                                                    self.hec_logging_enabled)
                # Restore per-store enabled state
                stored_enabled = cfg.get("stores_enabled", {})
                for k, v in stored_enabled.items():
                    self.stores_enabled[int(k)] = v
                log.info("Loaded config from config.json")
            except Exception as e:
                log.warning(f"Failed to load config.json: {e}")

        # Env vars override file config
        self.hec_url = os.getenv("HEC_URL", self.hec_url)
        self.hec_token = os.getenv("HEC_TOKEN", self.hec_token)
        self.hec_index = os.getenv("HEC_INDEX", self.hec_index)

    def save_config(self):
        """Persist current config to config.json."""
        cfg = {
            "hec_url": self.hec_url,
            "hec_token": self.hec_token,
            "hec_index": self.hec_index,
            "hec_verify_ssl": self.hec_verify_ssl,
            "hec_logging_enabled": self.hec_logging_enabled,
            "stores_enabled": {str(k): v for k, v in self.stores_enabled.items()},
        }
        CONFIG_PATH.write_text(json.dumps(cfg, indent=2))
        log.info("Saved config to config.json")

    def load_stores_and_templates(self):
        """Load stores.json and event-templates.json, hydrate per-store."""
        # Load stores
        if not STORES_PATH.exists():
            log.error(f"Store config not found: {STORES_PATH}")
            return
        stores_data = json.loads(STORES_PATH.read_text())
        self.stores = stores_data.get("stores", [])
        self.store_map = {s["number"]: s for s in self.stores}

        # Load raw templates
        if not TEMPLATES_PATH.exists():
            log.error(f"Event templates not found: {TEMPLATES_PATH}")
            return
        self.raw_templates = json.loads(TEMPLATES_PATH.read_text())

        # Load mutex groups
        self.mutex_groups = self.raw_templates.get("_mutex_groups", {})

        # Hydrate templates per store
        fault_names = list(self.raw_templates.get("faults", {}).keys())
        for store in self.stores:
            num = store["number"]
            self.store_templates[num] = hydrate_templates(self.raw_templates, store)

            # Enable store by default if not already configured
            if num not in self.stores_enabled:
                self.stores_enabled[num] = True

            # Initialise fault toggles for this store
            for fname in fault_names:
                key = self._fault_key(num, fname)
                self.active_faults[key] = False
                self.fired_phases[key] = set()

        healthy_count = len(self.raw_templates.get("healthy", {}).get("events", []))
        log.info(f"Loaded {len(self.stores)} stores, {healthy_count} healthy templates, "
                 f"{len(fault_names)} fault scenarios: {fault_names}")


state = AppState()

# ---------------------------------------------------------------------------
# HEC sender
# ---------------------------------------------------------------------------

async def send_to_hec(events: list[dict], client: httpx.AsyncClient,
                      store_number: Optional[int] = None,
                      abort_check: Optional[callable] = None):
    """POST a batch of events to Splunk HEC.

    If store_number is provided, injects store_lat, store_lon, store_id,
    and store_city as HEC indexed fields on every event.

    If abort_check is provided, it's called before each event — if it
    returns True, the batch is aborted (prevents healthy event leakage
    into a fault window).
    """
    if not state.hec_url or not state.hec_token:
        return

    url = _hec_event_url()
    headers = {"Authorization": f"Splunk {state.hec_token}"}

    # Build HEC fields for this store (if provided)
    hec_fields = None
    if store_number is not None:
        store = state.store_map.get(store_number)
        if store:
            hec_fields = {
                "store_id": f"{store['number']:03d}",
                "store_city": store["city"],
                "store_lat": store["lat"],
                "store_lon": store["lon"],
            }

    for i, event_def in enumerate(events):
        if abort_check and abort_check():
            log.info("Abort check triggered — stopping batch send")
            return

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        now_syslog = datetime.now(timezone.utc).strftime("%b %d %H:%M:%S")
        epoch = str(time.time())

        raw = event_def["template"]
        raw = raw.replace("TIMESTAMP_ISO", now_iso)
        raw = raw.replace("TIMESTAMP_SYSLOG", now_syslog)
        raw = replace_variance_tokens(raw)

        # Build event — inject store metadata into JSON events directly
        is_json = raw.strip().startswith("{")
        if is_json:
            event_obj = json.loads(raw)
            if hec_fields:
                event_obj["store_id"] = hec_fields["store_id"]
                event_obj["store_city"] = hec_fields["store_city"]
                event_obj["store_lat"] = hec_fields["store_lat"]
                event_obj["store_lon"] = hec_fields["store_lon"]
            payload = {
                "time": epoch,
                "index": state.hec_index,
                "sourcetype": event_def["sourcetype"],
                "source": event_def["source"],
                "host": event_def.get("host_override", "meraki-cloud"),
                "event": event_obj,
            }
        else:
            # Syslog — use HEC fields (can't inject into raw text)
            payload = {
                "time": epoch,
                "index": state.hec_index,
                "sourcetype": event_def["sourcetype"],
                "source": event_def["source"],
                "host": event_def.get("host_override", "meraki-cloud"),
                "event": raw,
            }
            if hec_fields:
                payload["fields"] = hec_fields

        try:
            resp = await client.post(url, json=payload, headers=headers, timeout=10.0)
            if resp.status_code == 200:
                state.events_sent += 1
                state.last_event_time = now_iso
            else:
                state.errors += 1
                state.last_error = f"HEC {resp.status_code}: {resp.text[:200]}"
                log.warning(f"HEC error: {resp.status_code} — {resp.text[:200]}")
        except Exception as e:
            state.errors += 1
            state.last_error = str(e)[:200]
            log.warning(f"HEC connection error: {e}")

        # Stagger events for natural distribution
        if i < len(events) - 1:
            await asyncio.sleep(random.uniform(0.1, 0.3))


# ---------------------------------------------------------------------------
# Background event loop
# ---------------------------------------------------------------------------

async def event_loop():
    """Main loop: iterates over all stores, sends healthy or fault events."""
    ssl_ctx = ssl.create_default_context()
    if not state.hec_verify_ssl:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    async with httpx.AsyncClient(verify=ssl_ctx) as client:
        while True:
            try:
                if state.sending_enabled and state.hec_url and state.hec_token:
                    for store in state.stores:
                        num = store["number"]

                        # Skip disabled stores
                        if not state.stores_enabled.get(num, True):
                            continue

                        templates = state.store_templates.get(num)
                        if not templates:
                            continue

                        # Check if any fault is active for THIS store
                        fault_names = list(templates.get("faults", {}).keys())
                        store_faults = {
                            fname: state.active_faults.get(
                                state._fault_key(num, fname), False)
                            for fname in fault_names
                        }
                        any_fault = any(store_faults.values())

                        if not any_fault:
                            # Send healthy events for this store
                            healthy = templates.get("healthy", {}).get("events", [])
                            if healthy:
                                def abort_check(sn=num):
                                    """Abort if a fault just activated for this store."""
                                    return any(
                                        state.active_faults.get(
                                            state._fault_key(sn, fn), False)
                                        for fn in fault_names
                                    )
                                await send_to_hec(healthy, client,
                                                  store_number=num,
                                                  abort_check=abort_check)
                        else:
                            # Process faults for this store
                            for fname, is_active in store_faults.items():
                                if not is_active:
                                    continue

                                key = state._fault_key(num, fname)

                                # Re-check live state
                                if not state.active_faults.get(key, False):
                                    continue

                                fault_def = templates.get("faults", {}).get(fname, {})
                                phases = fault_def.get("phases", [])

                                all_fired = True
                                for phase in phases:
                                    phase_num = phase["phase"]
                                    if phase_num in state.fired_phases.get(key, set()):
                                        continue

                                    all_fired = False
                                    delay = phase.get("delay_seconds", 0)
                                    fault_start = state.fault_starts.get(key, 0)
                                    if not fault_start:
                                        continue
                                    elapsed = time.time() - fault_start

                                    if elapsed >= delay:
                                        if not state.active_faults.get(key, False):
                                            break
                                        await send_to_hec(phase["events"], client,
                                                          store_number=num)
                                        state.fired_phases.setdefault(key, set()).add(
                                            phase_num)
                                        log.info(
                                            f"FAULT [Store {num} / {fname}] "
                                            f"Phase {phase_num}: {phase['name']} "
                                            f"— sent {len(phase['events'])} events")

                                # Ongoing re-send
                                if all_fired and state.active_faults.get(key, False):
                                    all_fault_events = []
                                    for phase in phases:
                                        all_fault_events.extend(
                                            phase.get("events", []))
                                    await send_to_hec(all_fault_events, client,
                                                      store_number=num)
                                    log.info(
                                        f"FAULT [Store {num} / {fname}] Ongoing "
                                        f"— re-sent {len(all_fault_events)} events")

            except Exception as e:
                log.error(f"Event loop error (will retry next cycle): {e}",
                          exc_info=True)

            # Flush buffered app logs to Splunk
            try:
                await flush_logs_to_hec(client)
            except Exception:
                pass

            await asyncio.sleep(state.healthy_interval)


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    state.load_config()
    state.load_stores_and_templates()
    task = asyncio.create_task(event_loop())
    log.info("Scenario controller started (multi-store)")
    yield
    task.cancel()

app = FastAPI(title="Kinetic Leisure — Scenario Controller", lifespan=lifespan)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/")
async def index():
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


# --- Config endpoints ---

class ConfigUpdate(BaseModel):
    hec_url: str
    hec_token: str
    hec_index: str = "main"
    hec_verify_ssl: bool = False
    hec_logging_enabled: bool = False

@app.get("/api/config")
async def get_config():
    return {
        "hec_url": state.hec_url,
        "hec_token": ("••••" + state.hec_token[-4:]
                      if len(state.hec_token) > 4 else ""),
        "hec_index": state.hec_index,
        "hec_verify_ssl": state.hec_verify_ssl,
        "hec_logging_enabled": state.hec_logging_enabled,
    }

@app.post("/api/config")
async def update_config(cfg: ConfigUpdate):
    state.hec_url = cfg.hec_url
    state.hec_token = cfg.hec_token
    state.hec_index = cfg.hec_index
    state.hec_verify_ssl = cfg.hec_verify_ssl
    state.hec_logging_enabled = cfg.hec_logging_enabled
    state.save_config()
    return {"status": "ok", "message": "Configuration saved"}

@app.post("/api/hec-logging")
async def toggle_hec_logging():
    state.hec_logging_enabled = not state.hec_logging_enabled
    state.save_config()
    status = "enabled" if state.hec_logging_enabled else "disabled"
    log.info(f"HEC logging {status}")
    return {"status": "ok", "hec_logging_enabled": state.hec_logging_enabled}


# --- Sending control ---

@app.post("/api/start")
async def start_sending():
    if not state.hec_url or not state.hec_token:
        raise HTTPException(400, "Configure HEC endpoint and token first")
    state.sending_enabled = True
    enabled_stores = [s["number"] for s in state.stores
                      if state.stores_enabled.get(s["number"], True)]
    log.info(f"Event sending STARTED — {len(enabled_stores)} stores active: "
             f"{enabled_stores}")
    return {"status": "ok", "sending": True}

@app.post("/api/stop")
async def stop_sending():
    state.sending_enabled = False
    # Reset all faults
    for key in list(state.active_faults.keys()):
        state.active_faults[key] = False
        state.fired_phases[key] = set()
    state.fault_starts.clear()
    log.info("Event sending STOPPED — all faults reset")
    return {"status": "ok", "sending": False}


# --- Store endpoints ---

@app.get("/api/stores")
async def get_stores():
    """Return all stores with their enabled state and active fault count."""
    result = []
    for store in state.stores:
        num = store["number"]
        fault_names = list(
            state.store_templates.get(num, {}).get("faults", {}).keys())
        active_count = sum(
            1 for fn in fault_names
            if state.active_faults.get(state._fault_key(num, fn), False))
        result.append({
            "number": num,
            "city": store["city"],
            "city_code": store["city_code"],
            "state_province": store["state_province"],
            "country": store["country"],
            "enabled": state.stores_enabled.get(num, True),
            "active_faults": active_count,
        })
    return result

@app.post("/api/stores/{store_number}/enable")
async def enable_store(store_number: int):
    if store_number not in state.store_map:
        raise HTTPException(404, f"Unknown store: {store_number}")
    state.stores_enabled[store_number] = True
    state.save_config()
    log.info(f"Store {store_number} ENABLED")
    return {"status": "ok", "store": store_number, "enabled": True}

@app.post("/api/stores/{store_number}/disable")
async def disable_store(store_number: int):
    if store_number not in state.store_map:
        raise HTTPException(404, f"Unknown store: {store_number}")
    # Deactivate any active faults for this store first
    fault_names = list(
        state.store_templates.get(store_number, {}).get("faults", {}).keys())
    for fn in fault_names:
        key = state._fault_key(store_number, fn)
        state.active_faults[key] = False
        state.fired_phases[key] = set()
        state.fault_starts.pop(key, None)
    state.stores_enabled[store_number] = False
    state.save_config()
    log.info(f"Store {store_number} DISABLED — faults reset")
    return {"status": "ok", "store": store_number, "enabled": False}


# --- Fault toggle endpoints ---

@app.get("/api/faults/{store_number}")
async def get_store_faults(store_number: int):
    if store_number not in state.store_map:
        raise HTTPException(404, f"Unknown store: {store_number}")

    templates = state.store_templates.get(store_number, {})
    faults = templates.get("faults", {})
    result = []
    for name, defn in faults.items():
        key = state._fault_key(store_number, name)
        phases = defn.get("phases", [])
        result.append({
            "name": name,
            "description": defn.get("_description", ""),
            "mutex_group": defn.get("mutex_group"),
            "active": state.active_faults.get(key, False),
            "phases": [
                {
                    "phase": p["phase"],
                    "name": p["name"],
                    "delay_seconds": p.get("delay_seconds", 0),
                    "event_count": len(p.get("events", [])),
                    "fired": p["phase"] in state.fired_phases.get(key, set()),
                    "description": p.get("description", ""),
                }
                for p in phases
            ],
        })
    return result


@app.post("/api/faults/{store_number}/{fault_name}/activate")
async def activate_fault(store_number: int, fault_name: str):
    if store_number not in state.store_map:
        raise HTTPException(404, f"Unknown store: {store_number}")

    key = state._fault_key(store_number, fault_name)
    if key not in state.active_faults:
        raise HTTPException(404, f"Unknown fault: {fault_name}")
    if not state.sending_enabled:
        raise HTTPException(400, "Start sending first")

    # Mutex group check — only within the same store
    fault_def = (state.store_templates.get(store_number, {})
                 .get("faults", {}).get(fault_name, {}))
    fault_mutex = fault_def.get("mutex_group")
    if fault_mutex:
        # Find all faults in the same mutex group
        for group_name, members in state.mutex_groups.items():
            if fault_name in members:
                for other in members:
                    if other == fault_name:
                        continue
                    other_key = state._fault_key(store_number, other)
                    if state.active_faults.get(other_key, False):
                        raise HTTPException(
                            409,
                            f"Cannot activate '{fault_name}' — conflicts with "
                            f"'{other}' (mutex group: {group_name}). "
                            f"Deactivate '{other}' first.")

    state.active_faults[key] = True
    state.fault_starts[key] = time.time()
    state.fired_phases[key] = set()
    log.info(f"FAULT ACTIVATED: Store {store_number} / {fault_name}")
    return {"status": "ok", "store": store_number, "fault": fault_name,
            "active": True}


@app.post("/api/faults/{store_number}/{fault_name}/deactivate")
async def deactivate_fault(store_number: int, fault_name: str):
    if store_number not in state.store_map:
        raise HTTPException(404, f"Unknown store: {store_number}")

    key = state._fault_key(store_number, fault_name)
    if key not in state.active_faults:
        raise HTTPException(404, f"Unknown fault: {fault_name}")

    state.active_faults[key] = False
    state.fault_starts.pop(key, None)
    state.fired_phases[key] = set()
    log.info(f"FAULT DEACTIVATED: Store {store_number} / {fault_name} — reset")
    return {"status": "ok", "store": store_number, "fault": fault_name,
            "active": False}


# --- Legacy /api/faults endpoint (returns all stores' faults) ---

@app.get("/api/faults")
async def get_all_faults():
    """Return faults across all stores — used by the fleet overview."""
    result = {}
    for store in state.stores:
        num = store["number"]
        faults = await get_store_faults(num)
        result[num] = faults
    return result


# --- Status endpoint ---

@app.get("/api/status")
async def get_status():
    any_fault = any(
        v for k, v in state.active_faults.items() if v
    )
    enabled_count = sum(1 for s in state.stores
                        if state.stores_enabled.get(s["number"], True))
    return {
        "sending": state.sending_enabled,
        "hec_configured": bool(state.hec_url and state.hec_token),
        "hec_logging_enabled": state.hec_logging_enabled,
        "mode": ("fault" if any_fault
                 else ("healthy" if state.sending_enabled else "idle")),
        "events_sent": state.events_sent,
        "errors": state.errors,
        "last_event_time": state.last_event_time,
        "last_error": state.last_error,
        "healthy_interval": state.healthy_interval,
        "stores_total": len(state.stores),
        "stores_enabled": enabled_count,
    }


# --- Test HEC connection ---

@app.post("/api/test-hec")
async def test_hec():
    if not state.hec_url or not state.hec_token:
        raise HTTPException(400, "Configure HEC endpoint and token first")

    url = _hec_event_url()
    headers = {"Authorization": f"Splunk {state.hec_token}"}
    payload = {
        "index": state.hec_index,
        "sourcetype": "scenario_controller:test",
        "source": "scenario_controller",
        "host": "kl-scenario-controller",
        "event": {
            "message": "Kinetic Leisure Scenario Controller — HEC connection test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }

    ssl_ctx = ssl.create_default_context()
    if not state.hec_verify_ssl:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    try:
        async with httpx.AsyncClient(verify=ssl_ctx) as client:
            resp = await client.post(url, json=payload, headers=headers,
                                     timeout=10.0)
            if resp.status_code == 200:
                return {"status": "ok", "message": "HEC connection successful"}
            else:
                return {"status": "error",
                        "message": f"HEC returned {resp.status_code}: "
                                   f"{resp.text[:300]}"}
    except Exception as e:
        return {"status": "error",
                "message": f"Connection failed: {str(e)[:300]}"}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    log.info(f"Starting Kinetic Leisure Scenario Controller on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info",
                access_log=False)
