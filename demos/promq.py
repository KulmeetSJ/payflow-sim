#!/usr/bin/env python3
"""Instant PromQL query, prints the first value (4 dp) or n/a: demos/promq.py '<expr>' [prom-url]"""
import json
import sys
import urllib.parse
import urllib.request

url = (sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9090") + "/api/v1/query?" + urllib.parse.urlencode({"query": sys.argv[1]})
result = json.load(urllib.request.urlopen(url))["data"]["result"]
print(f"{float(result[0]['value'][1]):.4f}" if result else "n/a")
