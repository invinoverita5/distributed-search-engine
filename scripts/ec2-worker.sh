#!/bin/bash

# Start worker services on EC2

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Check for Java JDK
if ! command -v javac &> /dev/null; then
    echo "❌ ERROR: Java JDK not found (javac missing)"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./scripts/ec2-setup.sh"
    echo ""
    exit 1
fi

if ! command -v java &> /dev/null; then
    echo "❌ ERROR: Java runtime not found"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./scripts/ec2-setup.sh"
    echo ""
    exit 1
fi

# Parse arguments
COORDINATOR_IP="$1"
WORKER_ID="$2"

if [ -z "$COORDINATOR_IP" ] || [ -z "$WORKER_ID" ]; then
    echo "Usage: $0 <coordinator-ip> <worker-id>"
    echo "Example: $0 10.0.1.100 ccccc"
    exit 1
fi

KVS_COORDINATOR="$COORDINATOR_IP:8000"
FLAME_COORDINATOR="$COORDINATOR_IP:9000"

echo "🚀 === Starting Worker Node ==="
echo -e "⚙️  Coordinator: $COORDINATOR_IP | Worker ID: $WORKER_ID\n"

# Clean up any existing processes
echo "🧹 Cleaning up existing processes..."
pkill -f cis5550 2>/dev/null
sleep 2
echo -e "✅ Existing processes cleaned up!\n"

# Clean up and create logs directory
echo "📋 Cleaning up logs..."
rm -rf logs/
mkdir -p logs
echo -e "✅ Logs directory cleaned!\n"

# Setup worker data directory
echo "📁 Setting up data directory..."
mkdir -p data
echo -n "$WORKER_ID" > "data/id"
echo -e "✅ Data directory ready (id: $WORKER_ID)\n"

# Compile the code
./scripts/compile.sh
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Compilation failed!"
    exit 1
fi
echo ""

# Start KVS Worker
echo "👷 Starting KVS Worker on port 8001..."
nohup java -cp bin cis5550.kvs.Worker 8001 data "$KVS_COORDINATOR" > logs/kvs-worker.log 2>&1 &
sleep 3
echo -e "✅ KVS Worker running\n"

# Start Flame Worker
echo "🔥 Starting Flame Worker on port 9001..."
nohup java -Djava.net.preferIPv4Stack=true -Dhttp.maxConnections=200 -cp "bin:lib/*" cis5550.flame.Worker 9001 "$FLAME_COORDINATOR" > logs/flame-worker.log 2>&1 &
sleep 3
echo -e "✅ Flame Worker running\n"

echo ""
echo "🎉 === Worker Node Started ==="
echo ""
echo "📊 Services:"
echo "  KVS Worker:   port 8001 -> $KVS_COORDINATOR"
echo "  Flame Worker: port 9001 -> $FLAME_COORDINATOR"
echo ""
echo "📊 Process summary:"
ps aux | grep cis5550 | grep -v grep | wc -l | xargs echo "  Running processes:"
