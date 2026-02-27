#!/bin/bash

# Submit PageRank job on EC2 (assumes coordinator/workers already running)

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

FLAME_COORDINATOR="${1:-localhost:9000}"

echo "🚀 === Submitting PageRank Job ==="
echo -e "⚙️  Flame Coordinator: $FLAME_COORDINATOR\n"

# Compile the code
./scripts/compile.sh
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Compilation failed!"
    exit 1
fi
echo ""

# Build the pagerank JAR
echo "📦 Building pagerank.jar..."
jar cvf pagerank.jar -C bin cis5550/jobs/PageRank.class -C bin cis5550/jobs/Crawler.class > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ ERROR: JAR creation failed!"
    exit 1
fi
echo -e "✅ pagerank.jar created!\n"

# Submit the PageRank job
echo "📊 Submitting PageRank job..."
java -cp bin cis5550.flame.FlameSubmit "$FLAME_COORDINATOR" pagerank.jar cis5550.jobs.PageRank
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Job submission failed!"
    exit 1
fi
echo ""
echo "🎊 ✅ PageRank job completed successfully!"
