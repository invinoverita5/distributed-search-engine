#!/bin/bash

# Start coordinator services on EC2

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🚀 === Starting Coordinator Node ==="
echo ""

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

# Compile the code
./scripts/compile.sh
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Compilation failed!"
    exit 1
fi
echo ""

# Get coordinator IP first (needed for Flame Coordinator to pass to workers)
COORDINATOR_IP=$(hostname -I | awk '{print $1}')

# Start KVS Coordinator
echo "🗄️  Starting KVS Coordinator on port 8000..."
nohup java -cp bin cis5550.kvs.Coordinator 8000 > logs/kvs-coordinator.log 2>&1 &
sleep 3

# Verify KVS coordinator started
if ! curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo "❌ ERROR: KVS Coordinator failed to start!"
    exit 1
fi
echo -e "✅ KVS Coordinator running\n"

# Start Flame Coordinator (use actual IP so workers can reach the KVS)
echo "🔥 Starting Flame Coordinator on port 9000..."
nohup java -cp bin cis5550.flame.Coordinator 9000 $COORDINATOR_IP:8000 > logs/flame-coordinator.log 2>&1 &
sleep 3

# Verify Flame coordinator started
if ! curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "❌ ERROR: Flame Coordinator failed to start!"
    exit 1
fi
echo -e "✅ Flame Coordinator running\n"

echo ""
echo "🎉 === Coordinator Node Started ==="
echo ""
echo "📊 Services:"
echo "  KVS Coordinator:   port 8000"
echo "  Flame Coordinator: port 9000"
echo ""
echo "🔗 Run this on each worker node:"
echo "  ./scripts/ec2-worker.sh $COORDINATOR_IP <worker-id>"
echo ""
echo "  Worker IDs: ccccc, hhhhh, mmmmm, rrrrr, wwwww"
