#!/bin/bash

cd "$(dirname "$0")/.."

FILES=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --files)
            shift
            while [[ $# -gt 0 && ! $1 == --* ]]; do
                FILES+=("$1")
                shift
            done
            ;;
        *)
            FILES+=("$1")
            shift
            ;;
    esac
done

if [ ${#FILES[@]} -eq 0 ]; then
    tail -f logs/*.log
else
    tail -f "${FILES[@]}"
fi
