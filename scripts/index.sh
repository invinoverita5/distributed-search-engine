#!/bin/bash

# Submit indexer job on EC2 (assumes coordinator/workers already running)

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

FLAME_COORDINATOR="${1:-localhost:9000}"

echo "🚀 === Submitting Indexer Job ==="
echo -e "⚙️  Flame Coordinator: $FLAME_COORDINATOR\n"

# Compile the code
./scripts/compile.sh
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Compilation failed!"
    exit 1
fi
echo ""

# Build the indexer JAR
echo "📦 Building indexer.jar..."
jar cvf indexer.jar -C bin cis5550/jobs/Indexer.class -C bin cis5550/jobs/Crawler.class > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ ERROR: JAR creation failed!"
    exit 1
fi
echo -e "✅ indexer.jar created!\n"

# Submit the indexer job
echo "📚 Submitting indexer job..."
java -cp bin cis5550.flame.FlameSubmit "$FLAME_COORDINATOR" indexer.jar cis5550.jobs.Indexer
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Job submission failed!"
    exit 1
fi
echo ""
echo "🎊 ✅ Indexer job completed successfully!"
