#!/usr/bin/env bash
# ── Crypto15min PolyTrader — In-place updater ────────────────────────────
# Run this ON the VPS where the bot is already running.
#
# Usage:
#   1. Upload the release zip to the VPS (any location)
#   2. Run:  bash update_customer.sh /path/to/crypto15min_polytrader_vX.Y.Z.zip
#
# What it does:
#   - Backs up the customer's .env, logs, and data (nothing is lost)
#   - Stops the running container
#   - Extracts the new code
#   - Restores their config
#   - Rebuilds the Docker image
#   - Starts the bot
set -euo pipefail

ZIP="${1:?Usage: bash update_customer.sh /path/to/crypto15min_polytrader_vX.Y.Z.zip}"

# Auto-detect the bot install directory
if [ -d "/home/linuxuser/crypto15min-polytrader" ]; then
    BOT_DIR="/home/linuxuser/crypto15min-polytrader"
elif [ -d "/opt/crypto15min-polytrader" ]; then
    BOT_DIR="/opt/crypto15min-polytrader"
elif [ -d "$HOME/crypto15min-polytrader" ]; then
    BOT_DIR="$HOME/crypto15min-polytrader"
else
    # Fallback: find it by docker-compose
    BOT_DIR=$(docker inspect crypto15min-polytrader --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' 2>/dev/null || true)
    if [ -z "$BOT_DIR" ] || [ ! -d "$BOT_DIR" ]; then
        # Try finding by container name in docker ps
        echo "ERROR: Cannot find crypto15min-polytrader install directory."
        echo "Please provide it:  BOT_DIR=/path/to/dir bash update_customer.sh $ZIP"
        exit 1
    fi
fi

VER=$(basename "$ZIP" | grep -oP 'v[\d._-]+' | head -1)
echo "=== Crypto15min PolyTrader — Update to ${VER:-new version} ==="
echo "Bot directory: $BOT_DIR"
echo "Update zip:    $ZIP"
echo ""

# ── 1. Back up customer config ──────────────────────────────────────────
echo "[1/5] Backing up config, logs, data..."
BACKUP_DIR="$BOT_DIR/_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Preserve .env (the critical file)
if [ -f "$BOT_DIR/config/.env" ]; then
    cp "$BOT_DIR/config/.env" "$BACKUP_DIR/config.env"
    echo "  ✓ config/.env backed up"
elif [ -f "$BOT_DIR/.env" ]; then
    cp "$BOT_DIR/.env" "$BACKUP_DIR/dot.env"
    echo "  ✓ .env backed up"
fi

# Preserve logs and data (symlink-safe copy)
[ -d "$BOT_DIR/logs" ] && cp -r "$BOT_DIR/logs" "$BACKUP_DIR/logs" && echo "  ✓ logs backed up"
[ -d "$BOT_DIR/data" ] && cp -r "$BOT_DIR/data" "$BACKUP_DIR/data" && echo "  ✓ data backed up"

echo "  Backup saved to: $BACKUP_DIR"

# ── 2. Stop running container ───────────────────────────────────────────
echo ""
echo "[2/5] Stopping running container..."
cd "$BOT_DIR"
docker compose down 2>/dev/null || docker-compose down 2>/dev/null || echo "  (no running container)"

# ── 3. Extract new code ─────────────────────────────────────────────────
echo ""
echo "[3/5] Extracting new code..."

# Remove old source files (but NOT config, logs, data, backup)
rm -rf "$BOT_DIR/src" "$BOT_DIR/templates" "$BOT_DIR/static"
rm -f "$BOT_DIR/Dockerfile" "$BOT_DIR/docker-compose.yml" "$BOT_DIR/requirements.txt"
rm -f "$BOT_DIR/.env.example" "$BOT_DIR/VERSION" "$BOT_DIR/setup.sh"

# Extract zip
unzip -o "$ZIP" -d "$BOT_DIR"
echo "  ✓ extracted"

# ── 4. Restore customer config ──────────────────────────────────────────
echo ""
echo "[4/5] Restoring customer config..."

# Ensure config dir exists
mkdir -p "$BOT_DIR/config"
mkdir -p "$BOT_DIR/logs"
mkdir -p "$BOT_DIR/data"
mkdir -p "$BOT_DIR/releases"

# Restore .env
if [ -f "$BACKUP_DIR/config.env" ]; then
    cp "$BACKUP_DIR/config.env" "$BOT_DIR/config/.env"
    echo "  ✓ config/.env restored"
elif [ -f "$BACKUP_DIR/dot.env" ]; then
    cp "$BACKUP_DIR/dot.env" "$BOT_DIR/config/.env"
    echo "  ✓ .env moved to config/.env"
fi

# Restore logs if they were clobbered
if [ -f "$BACKUP_DIR/logs/state.json" ] && [ ! -f "$BOT_DIR/logs/state.json" ]; then
    cp -r "$BACKUP_DIR/logs/"* "$BOT_DIR/logs/" 2>/dev/null || true
    echo "  ✓ logs restored"
fi

echo "  ✓ Customer config preserved"

# ── 5. Rebuild and start ────────────────────────────────────────────────
echo ""
echo "[5/5] Rebuilding Docker image and starting bot..."
cd "$BOT_DIR"
docker compose build --no-cache 2>/dev/null || docker-compose build --no-cache
docker compose up -d 2>/dev/null || docker-compose up -d

echo ""
echo "=== Update complete! ==="
echo ""
echo "Checking status..."
sleep 3
docker compose logs --tail=20 2>/dev/null || docker-compose logs --tail=20
echo ""
echo "Dashboard should be available at: http://$(hostname -I | awk '{print $1}'):8603"
echo "Version: $(cat "$BOT_DIR/VERSION")"
