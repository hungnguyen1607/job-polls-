#!/usr/bin/env bash
# Turn LaTeX resumes into PDFs next to them (no Overleaf needed).
#   bash scripts/make_resume.sh                 # every *.tex resume -> "Hung Nguyen Resume (<Name>).pdf"
#   bash scripts/make_resume.sh canada.tex      # just one
set -euo pipefail
cd "$(dirname "$0")/.."
files=("$@")
[ ${#files[@]} -eq 0 ] && files=(america.tex canada.tex)
for src in "${files[@]}"; do
    [ -f "$src" ] || { echo "skip: $src not found"; continue; }
    name="$(basename "${src%.tex}")"
    out="Hung Nguyen Resume (${name^}).pdf"
    build="$(mktemp -d)"
    if ! pdflatex -interaction=nonstopmode -halt-on-error -output-directory "$build" "$src" > "$build/log.txt"; then
        echo "LaTeX error in $src:"; grep -A3 "^!" "$build/log.txt" | head -20; rm -rf "$build"; exit 1
    fi
    mv "$build/$name.pdf" "$out"
    rm -rf "$build"
    echo "Made: $out ($(pdfinfo "$out" | awk '/^Pages/ {print $2}') page)"
done
