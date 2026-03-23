#!/bin/sh
set -e

ruff check
python -m  pytest