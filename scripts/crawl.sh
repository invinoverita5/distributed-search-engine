#!/bin/bash

# Submit crawler job on EC2 (assumes coordinator/workers already running)
# Usage: ./crawl.sh [MAX_PAGES] [SEED_URL] [FLAME_COORDINATOR]

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

MAX_PAGES="${1:-5000}"
SEED_URL="${2:-https://zminsc.dev}"
FLAME_COORDINATOR="${3:-localhost:9000}"

echo "🚀 === Submitting Crawler Job ==="
echo -e "⚙️  Max Pages: $MAX_PAGES | Seed URL: $SEED_URL | Flame Coordinator: $FLAME_COORDINATOR\n"

# Compile the code
./scripts/compile.sh
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Compilation failed!"
    exit 1
fi
echo ""

# Build the crawler JAR
echo "📦 Building crawler.jar..."
jar cvf crawler.jar -C bin cis5550/jobs/Crawler.class > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ ERROR: JAR creation failed!"
    exit 1
fi
echo -e "✅ crawler.jar created!\n"

# Submit the crawler job
echo "🕷️  Submitting crawler job..."
java -cp bin cis5550.flame.FlameSubmit "$FLAME_COORDINATOR" crawler.jar cis5550.jobs.Crawler "$MAX_PAGES" "$SEED_URL"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Job submission failed!"
    exit 1
fi
echo ""
echo "🎊 ✅ Crawler job completed successfully!"
