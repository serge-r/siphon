#!/bin/sh
set -e

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
fi

echo "siphon-probe installed."
echo "Edit /etc/default/siphon-probe, then enable it with:"
echo "  systemctl enable --now siphon-probe"
