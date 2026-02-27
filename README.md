# CIS5550 Final Project - Web Crawler

## Overview

This project implements a distributed web crawler using the Flame distributed computing framework and KVS (Key-Value Store). The crawler fetches web pages from seed URLs, extracts links, respects robots.txt, enforces politeness delays, and stores crawled content in a distributed key-value store.

## Project Structure

```
Perplexity/
├── src/
│   └── cis5550/
│       ├── kvs/           # Key-Value Store from HW5
│       ├── flame/         # Flame distributed computing from HW7
│       ├── webserver/     # Web server from HW3
│       ├── generic/       # Generic worker/coordinator base classes
│       ├── tools/         # Utility classes (Hasher, URLParser, Logger, etc.)
│       └── jobs/          # Flame jobs (Crawler)
├── bin/                   # Compiled Java classes
├── data/                  # KVS data storage (worker directories)
├── logs/                  # Log files from coordinators and workers
├── scripts/               # Build and monitoring scripts
├── seeds.txt              # Seed URLs for crawler
├── dashboard.html         # Live crawler dashboard (auto-generated)
└── README.md              # This file
```

## Crawler Features

- **Distributed crawling**: Parallel fetching across multiple Flame workers
- **Politeness**: Respects robots.txt and enforces per-host delays
- **Link extraction**: Parses HTML to discover new URLs
- **Content storage**: Stores page content, headers, and metadata in KVS
- **Crawl limits**: Configurable max pages (5,000) and max pages per domain (100)
- **Resume capability**: Can resume from saved queue after interruption
- **Monitoring**: Real-time dashboard showing crawl progress and statistics

## Requirements

- Java 11 or higher
- Unix-like environment (macOS, Linux)
- Multiple terminal windows (7+ recommended)

## Quick Start

### Option 1: Automated One-Command Start (Recommended)

The easiest way to start the crawler is with a single script that handles everything:

```bash
cd /Users/zac/eclipse-workspace/Perplexity
./scripts/crawl.sh
```

Or specify the number of workers:

```bash
./scripts/crawl.sh [num_kvs_workers] [num_flame_workers]

# Examples:
./scripts/crawl.sh           # Default: 6 KVS workers, 6 Flame workers
./scripts/crawl.sh 3         # 3 KVS workers, 6 Flame workers (default)
./scripts/crawl.sh 4 8       # 4 KVS workers, 8 Flame workers
./scripts/crawl.sh 10 12     # 10 KVS workers, 12 Flame workers
```

This script automatically:

1. Cleans up old logs
2. Compiles all Java source files
3. Builds `crawler.jar`
4. Verifies `seeds.txt` exists
5. Stops any existing processes
6. Starts KVS Coordinator (port 8000) + N KVS Workers (ports 8001-800N)
7. Starts Flame Coordinator (port 9000) + M Flame Workers (ports 9001-900M)
8. Submits the crawler job to Flame
9. All logs written to `logs/` directory

**Prerequisites**: Create a `seeds.txt` file with one seed URL per line:

```
https://en.wikipedia.org/
https://www.cis.upenn.edu/
https://news.ycombinator.com/
```

**Performance Tip**: More Flame workers = faster crawling. More KVS workers = better storage distribution. For a typical crawl, 6-10 workers of each type is recommended.

**Note**: The script runs all services in the background using `nohup`. After the job is submitted, the crawler runs autonomously.

**Monitoring**: After the crawler starts, monitor its progress:

```bash
# Generate and view the dashboard
./scripts/monitor.sh
open dashboard.html
```

See the "Monitoring and Debugging" section below for more details.

---

### Option 2: Manual Step-by-Step Setup

If you prefer more control over each step:

#### Step 1: Compile the Code

```bash
cd /Users/zac/eclipse-workspace/Perplexity
./scripts/compile.sh
```

This compiles all Java source files into the `bin/` directory.

#### Step 2: Create the Crawler JAR

To package the Crawler job for submission to Flame, create a JAR file containing only the Crawler class:

```bash
cd /Users/zac/eclipse-workspace/Perplexity
jar cvf crawler.jar -C bin cis5550/jobs/Crawler.class
```

This creates `crawler.jar` containing the compiled Crawler job.

#### Step 3: Start the Cluster

Create the logs directory if it doesn't exist:

```bash
mkdir -p logs
```

**Start KVS Cluster** - Open **3 separate terminal windows** and run:

**Terminal 1 - KVS Coordinator:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.kvs.Coordinator 8000 > logs/kvs-coordinator.log 2>&1
```

**Terminal 2 - KVS Worker 1:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.kvs.Worker 8001 data/worker1 localhost:8000 > logs/kvs-worker1.log 2>&1
```

**Terminal 3 - KVS Worker 2:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.kvs.Worker 8002 data/worker2 localhost:8000 > logs/kvs-worker2.log 2>&1
```

You can add more KVS workers on different ports (8003, 8004, etc.) as needed. Wait for all workers to register with the coordinator.

**Start Flame Cluster** - Open **4+ separate terminal windows** and run:

**Terminal 4 - Flame Coordinator:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.flame.Coordinator 9000 localhost:8000 > logs/flame-coordinator.log 2>&1
```

**Terminal 5 - Flame Worker 1:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.flame.Worker 9001 localhost:9000 > logs/flame-worker1.log 2>&1
```

**Terminal 6 - Flame Worker 2:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.flame.Worker 9002 localhost:9000 > logs/flame-worker2.log 2>&1
```

**Terminal 7 - Flame Worker 3:**

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.flame.Worker 9003 localhost:9000 > logs/flame-worker3.log 2>&1
```

You can start additional Flame workers (9004, 9005, 9006) for increased parallelism. Wait for all Flame workers to register with the coordinator.

### Step 4: Submit the Crawler Job

Ensure you have a `seeds.txt` file with one seed URL per line, then submit the job using FlameSubmit:

```bash
cd /Users/zac/eclipse-workspace/Perplexity
java -cp bin cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler seeds.txt
```

This command:

1. Connects to the Flame coordinator at `localhost:9000`
2. Uploads `crawler.jar` containing the Crawler job
3. Specifies the main class `cis5550.jobs.Crawler`
4. Passes `seeds.txt` as an argument to the crawler

### Step 5: Monitor the Crawler

Generate a live dashboard showing crawl statistics:

```bash
./scripts/monitor.sh
```

This creates `dashboard.html` with:

- Total pages crawled
- Unique domains visited
- Error counts
- Data storage size
- Top crawled domains
- Progress toward the 5,000 page limit

The dashboard auto-refreshes every 10 seconds. Open it in your browser:

```bash
open dashboard.html
```

You can also monitor in real-time by running the dashboard generator in watch mode:

```bash
watch -n 10 ./scripts/monitor.sh
```

## Performance Tuning

### Add More Workers

More Flame workers = more parallelism:

```bash
# Flame Worker 4
java -cp bin cis5550.flame.Worker 9004 localhost:9000 > logs/flame-worker4.log 2>&1

# Flame Worker 5
java -cp bin cis5550.flame.Worker 9005 localhost:9000 > logs/flame-worker5.log 2>&1
```

More KVS workers = better storage distribution:

```bash
# KVS Worker 3
java -cp bin cis5550.kvs.Worker 8003 data/worker3 localhost:8000 > logs/kvs-worker3.log 2>&1
```

### Adjust Limits

Edit `src/cis5550/jobs/Crawler.java`:

```java
private static final int MAX_PAGES = 5000;              // Global page limit
private static final int MAX_PAGES_PER_DOMAIN = 100;    // Per-domain limit
private static final int CONNECT_TIMEOUT = 10000;       // Connection timeout (ms)
private static final int READ_TIMEOUT = 15000;          // Read timeout (ms)
```

Recompile and rebuild JAR after changes.

## Cleanup

Stop all services:

```bash
# Kill all Flame/KVS processes
pkill -f "cis5550"

# Or kill individually
pkill -f "cis5550.kvs.Coordinator"
pkill -f "cis5550.kvs.Worker"
pkill -f "cis5550.flame.Coordinator"
pkill -f "cis5550.flame.Worker"
```

Clean up data and logs:

```bash
# Remove crawled data (WARNING: deletes all crawled pages)
rm -rf data/

# Remove logs
rm -rf logs/

# Remove generated files
rm -f crawler.jar dashboard.html
```

Clean compiled files:

```bash
rm -rf bin/*
```

## Next Steps

- Integrate indexer for full-text search
- Add PageRank computation
- Build web frontend for search queries
- Optimize for larger datasets (50,000+ pages)
- Deploy to distributed AWS EC2 cluster
