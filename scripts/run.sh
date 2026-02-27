#!/bin/bash

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

sudo nohup java -cp "bin:lib/*" cis5550.search.Frontend 80 localhost:8000 &
