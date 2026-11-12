# Kinetic Leisure — Scenario Controller (Multi-Store)

Sends synthetic Splunk HEC events for the KL retail fleet with per-store
toggleable fault injection. Replaces both Turbine and the single-store
scenario controller.

## Quick Start

```bash
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
caffeinate -i python app.py
```

Open http://localhost:8080

## Files

- `app.py` — FastAPI backend, HEC sender, per-store event loop
- `event-templates.json` — Tokenized event templates (healthy + 6 faults)
- `stores.json` — Fleet identity (6 stores with lat/lon)
- `static/index.html` — Fleet control panel UI
- `config.json` — Auto-generated (HEC settings, store enabled state)

## Architecture

At startup, `event-templates.json` is hydrated per-store using values from
`stores.json`. Tokens like `__S3__` (3-digit store number), `__CC__` (city
code), `__IPO__` (IP octet) are replaced with each store's identity.

The event loop iterates over all enabled stores each cycle. Stores without
active faults get healthy events; stores with active faults get cascading
fault events. Fault toggles are per-store and independent — Store 47 can
have a WAN failure while Store 112 runs healthy.

## Fault Scenarios

1. **WAN Transport Failure** — MPLS down, LTE failover, full cascade
2. **AP Hardware Failure** — Single AP dies, clients roam
3. **ISE Policy Rejection** — Certificate expiry, ISE rejects devices
4. **Camera WiFi Flap** — Coverage issue, single camera flapping
5. **Switch Failure** — Switch S001 down, cascades to AP-1 and AP-2
6. **WAN Degraded** — Elevated latency, no failover, no cascade

## Mutex Groups

- `wan`: wan_transport_failure, wan_degraded (per-store)
- `switch_ap_overlap`: switch_failure, ap_hardware_failure (per-store)

## Stores

| Store | City | Code | Country |
|-------|------|------|---------|
| 47 | Portland | PDX | US |
| 112 | Vancouver | VAN | CA |
| 237 | Portland | PDX | US |
| 305 | Seattle | SEA | US |
| 418 | Los Angeles | LAX | US |
| 521 | San Diego | SAN | US |

## K8s Deployment

`k8s-manifest.yaml` deploys the controller into the `net-triage-agent`
namespace (shared with the triage agent) as a Deployment + ClusterIP Service
(80 -> 8080), fronted by a Cilium Gateway API `Gateway` + `HTTPRoute`. A
commented nginx Ingress alternative is included at the bottom of the file.

```bash
docker build -t kl-scenario-controller:latest .
docker tag kl-scenario-controller:latest localhost:5000/kl-scenario-controller:latest
docker push localhost:5000/kl-scenario-controller:latest

# The namespace is normally created by the triage agent deployment; if you are
# deploying standalone, create it first:
kubectl create namespace net-triage-agent

# First deploy (Deployment, Service, Gateway, HTTPRoute):
kubectl apply -f k8s-manifest.yaml

# Subsequent image updates (imagePullPolicy is Always):
kubectl rollout restart deployment kl-scenario-controller -n net-triage-agent
```

Find the Gateway address with
`kubectl get gateway scenario-controller-gateway -n net-triage-agent`
(or use `kubectl port-forward svc/kl-scenario-controller 8080:80 -n net-triage-agent`).

### HEC configuration

The manifest contains no Secret and no env vars. Set the HEC URL, token and
index in the control-panel UI; the app persists them to `/app/config.json`
inside the container and reads that file at startup. `HEC_URL`, `HEC_TOKEN`
and `HEC_INDEX` environment variables, if set, override the file.

`config.json` lives on the container's writable layer (no volume is mounted),
so it survives only as long as the container does: after a
`rollout restart` or pod recreation, re-enter the HEC settings in the UI. If
you need them to persist, inject the `HEC_*` env vars from your own Secret (or
mount a persistent volume for `/app/config.json`).
