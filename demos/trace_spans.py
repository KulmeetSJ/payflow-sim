#!/usr/bin/env python3
"""Print the spans of one Tempo trace as a timeline: demos/trace_spans.py <trace-id> [tempo-url]"""
import json
import sys
import urllib.request

tid = sys.argv[1]
tempo = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:3200"
d = json.load(urllib.request.urlopen(f"{tempo}/api/traces/{tid}"))
spans = []
for b in d.get("batches", d.get("resourceSpans", [])):
    svc = next(a["value"]["stringValue"] for a in b["resource"]["attributes"] if a["key"] == "service.name")
    for ss in b.get("scopeSpans", []):
        for s in ss["spans"]:
            spans.append((int(s["startTimeUnixNano"]), int(s["endTimeUnixNano"]), svc, s.get("kind", ""), s["name"]))
spans.sort()
t0 = spans[0][0]
print(f"trace {tid}: {len(spans)} spans, services: {sorted({s[2] for s in spans})}")
for start, end, svc, kind, name in spans:
    kind = kind.replace("SPAN_KIND_", "").lower()
    print(f"  +{(start - t0) / 1e6:8.1f} ms {(end - start) / 1e6:8.1f} ms  {svc:18} {kind:9} {name}")
