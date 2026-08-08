#!/bin/bash
set -e

# Navigate to project root
cd "$(dirname "$0")"

echo "=========================================================="
echo "          PSLog Interactive Build & Config Setup          "
echo "=========================================================="

# -----------------------------------------------------------------------------
# 1. Interactive Configuration Prompts (Blank = Default)
# -----------------------------------------------------------------------------
echo -e "\n--- [Step 1/5] System Configuration ---"
echo "Press [ENTER] to accept default values."

# Helper function to prompt with defaults
prompt_param() {
    local prompt_text="$1"
    local default_val="$2"
    local var_name="$3"

    read -rp "$prompt_text [$default_val]: " input_val
    if [ -z "$input_val" ]; then
        eval "$var_name=\"$default_val\""
    else
        eval "$var_name=\"$input_val\""
    fi
}

prompt_param "Control Plane Address" "127.0.0.1:8080" CONTROL_PLANE_ADDR
prompt_param "Unix Socket Path" "/tmp/pslog.sock" BROKER_SOCKET_PATH
prompt_param "Broker Management Port" "60759" BROKER_MGMT_PORT

echo -e "\nWriting configuration settings..."

# Write configuration files
CONF_FILE="$HOME/.pslog.conf"
cat <<EOF > "$CONF_FILE"
# PSLog System Configuration
# Generated on $(date)

CONTROL_PLANE_ADDR=$CONTROL_PLANE_ADDR
BROKER_ADDR=$BROKER_SOCKET_PATH
BROKER_MGMT_PORT=$BROKER_MGMT_PORT
EOF

# Save local copy as fallback
cp "$CONF_FILE" ./pslog.conf

echo "  -> Configuration saved to: $CONF_FILE"
echo "  -> Local copy saved to : ./pslog.conf"

# -----------------------------------------------------------------------------
# 2. Build Pipeline
# -----------------------------------------------------------------------------
echo -e "\n--- [Step 2/5] Preparing bin/ directory ---"
mkdir -p bin

echo -e "\n--- [Step 3/5] Compiling Go Broker ---"
cd broker
go build -o ../bin/broker main.go
cd ..
echo "  -> Success: bin/broker"

echo -e "\n--- [Step 4/5] Compiling C++ CLI Engine ---"
cd cli
mkdir -p build && cd build
cmake ..
make -j$(nproc 2>/dev/null || echo 2)
cp pslog ../../bin/
cd ../..
echo "  -> Success: bin/pslog"

echo -e "\n--- [Step 5/5] Packaging Spring Boot Control Plane ---"
cd control-plane
./mvnw clean package -DskipTests
cd ..
echo "  -> Success: control-plane/target/control-plane-0.0.1-SNAPSHOT.jar"

# -----------------------------------------------------------------------------
# 3. Completion Summary & Path Instructions
# -----------------------------------------------------------------------------
CURRENT_DIR="$(pwd)"

echo -e "\n=========================================================="
echo "          PSLog Build Completed Successfully!             "
echo "=========================================================="
echo "Active Configuration ($CONF_FILE):"
echo "  • Control Plane Endpoint : $CONTROL_PLANE_ADDR"
echo "  • Local Unix Socket Path : $BROKER_SOCKET_PATH"
echo "  • Broker Management Port : $BROKER_MGMT_PORT"
echo "=========================================================="
echo "TIP: Add 'bin/' to your PATH to run 'pslog' from anywhere:"
echo ""
echo "  Option A (Current Terminal Session Only):"
echo "    export PATH=\"$CURRENT_DIR/bin:\$PATH\""
echo ""
echo "  Option B (Permanent - Append to Shell Config):"
if [ -n "$ZSH_VERSION" ] || [[ "$SHELL" == *"zsh"* ]]; then
    echo "    echo 'export PATH=\"$CURRENT_DIR/bin:\$PATH\"' >> ~/.zshrc && source ~/.zshrc"
else
    echo "    echo 'export PATH=\"$CURRENT_DIR/bin:\$PATH\"' >> ~/.bashrc && source ~/.bashrc"
fi
echo "=========================================================="
echo "Quick Commands:"
echo "  1. Control Plane Service :"
echo "     cd control-plane && ./mvnw spring-boot:run"
echo ""
echo "  2. Publish Log Stream :"
echo "     pslog pub <topic> \"<command> [args...]\""
echo "     Example: pslog pub app-logs \"tail -f /var/log/syslog\""
echo ""
echo "  3. Subscribe to Stream :"
echo "     pslog sub <topic>"
echo "     Example: pslog sub app-logs"
echo ""
echo "  4. Discover Active Topics :"
echo "     pslog scan"
echo "=========================================================="