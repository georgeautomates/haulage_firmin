#!/bin/bash
# Firmin VPS setup script
# Run once on the VPS after cloning the repo:
#   bash deploy/setup_vps.sh

set -e

INSTALL_DIR="/opt/firmin"
REPO_URL="https://github.com/georgeautomates/haulage_firmin"
SERVICE_NAME="firmin"

echo "=== Firmin VPS Setup ==="

# 1. Clone or update repo
if [ -d "$INSTALL_DIR/.git" ]; then
    echo "[1/6] Updating existing repo..."
    cd "$INSTALL_DIR"
    git pull
else
    echo "[1/6] Cloning repo to $INSTALL_DIR..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

# 2. Create virtual environment
echo "[2/6] Setting up Python virtual environment..."
python3 -m venv .venv
.venv/bin/pip install --upgrade pip --quiet
.venv/bin/pip install -r requirements.txt --quiet
echo "  Dependencies installed."

# 3. Check for .env
echo "[3/6] Checking .env..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "  .env created from .env.example — EDIT IT NOW before starting the service."
    echo "  nano /opt/firmin/.env"
else
    echo "  .env already exists."
fi

# 4. Check for config credentials
echo "[4/6] Checking config credentials..."
MISSING=0
for f in config/gmail_token.json config/gmail_credentials.json config/service_account.json; do
    if [ ! -f "$f" ]; then
        echo "  MISSING: $f"
        MISSING=1
    else
        echo "  OK: $f"
    fi
done
if [ "$MISSING" = "1" ]; then
    echo ""
    echo "  Upload missing credential files via SCP, e.g.:"
    echo "    scp config/gmail_token.json root@YOUR_VPS_IP:/opt/firmin/config/"
fi

# 5. Install systemd service
echo "[5/6] Installing systemd service..."
cp deploy/firmin.service /etc/systemd/system/firmin.service
systemctl daemon-reload
systemctl enable firmin
echo "  Service enabled (will start on boot)."

# 6. Done
echo ""
echo "[6/6] Setup complete."
echo ""
echo "Next steps:"
echo "  1. Edit /opt/firmin/.env with your real credentials"
echo "  2. Upload credential files to /opt/firmin/config/ if missing"
echo "  3. Start the agent: systemctl start firmin"
echo "  4. Check status:    systemctl status firmin"
echo "  5. Watch logs:      journalctl -u firmin -f"
