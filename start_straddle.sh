#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

echo "======================================================================"
echo "⚡ STRADDLE PRO - KOTAK NEO ALGORITHMIC PLATFORM"
echo "======================================================================"

python3 -m pip install -r requirements.txt --quiet
python3 web_server.py
