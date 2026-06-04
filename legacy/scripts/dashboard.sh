#!/bin/bash
set -e
cd "$(dirname "$0")/.."
uv run streamlit run src/interfaces/dashboard/app.py
