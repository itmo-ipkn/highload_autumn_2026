#!/usr/bin/env bash
# Render a text file (terminal output) as a terminal-like PNG with headless Firefox.
#   scripts/term2png.sh out.txt docs/img/out.png ["$ command shown in prompt"]
set -euo pipefail
in=$1; out=$2; cmd=${3:-}
tmp=$(mktemp -d)
lines=$(( $(wc -l < "$in") + 3 ))
h=$(( lines * 19 + 40 ))
{
  echo '<html><body style="margin:0;background:#1e1e2e"><pre style="margin:0;padding:16px;color:#cdd6f4;font:14px/19px JetBrains Mono,Noto Sans Mono,monospace;white-space:pre">'
  [ -n "$cmd" ] && printf '<span style="color:#a6e3a1">$ %s</span>\n' "$cmd"
  sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' \
      -e 's#\(RESULT: OK.*\)#<span style="color:\#a6e3a1">\1</span>#' \
      -e 's#\(RESULT: PROBLEMS.*\|WARNING.*\|LOST events=[1-9][0-9]*\)#<span style="color:\#f38ba8">\1</span>#' \
      -e 's#^\(\[[123]\].*\)#<span style="color:\#f9e2af">\1</span>#' "$in"
  echo '</pre></body></html>'
} > "$tmp/t.html"
firefox --headless --no-remote --profile "$tmp" --window-size "1500,$h" --screenshot "$(realpath -m "$out")" "file://$tmp/t.html" >/dev/null 2>&1
rm -rf "$tmp"
echo "saved $out"
