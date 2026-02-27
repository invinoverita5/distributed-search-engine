#!/bin/bash

# Backup KVS worker data to S3
# Usage: ./scripts/backup.sh [backup|restore|list] [options]
#
# Commands:
#   backup              Backup all worker data to S3
#   restore             Restore all worker data from S3
#   list                List available backups
#
# Options:
#   --bucket NAME       S3 bucket name (default: from S3_BUCKET env var)
#   --prefix PREFIX     Backup prefix/folder in S3 (default: kvs-backup)
#   --workers DIR       Local worker data directory (default: data)
#   --tables TABLES     Comma-separated list of tables to backup/restore (default: all pt-* tables)
#   --timestamp TAG     Use specific timestamp for restore (default: latest)
#   --worker-id ID      Restore specific worker directly to data/ (EC2 mode)
#   --dry-run           Show what would be done without doing it

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Load .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Default configuration
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="kvs-backup"
WORKERS_DIR="data"
TABLES=""
TIMESTAMP=""
WORKER_ID=""
DRY_RUN=false
COMMAND=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        backup|restore|list)
            COMMAND="$1"
            shift
            ;;
        --bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        --prefix)
            S3_PREFIX="$2"
            shift 2
            ;;
        --workers)
            WORKERS_DIR="$2"
            shift 2
            ;;
        --tables)
            TABLES="$2"
            shift 2
            ;;
        --timestamp)
            TIMESTAMP="$2"
            shift 2
            ;;
        --worker-id)
            WORKER_ID="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            head -20 "$0" | tail -n +2 | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate
if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3 bucket not specified. Use --bucket or set S3_BUCKET env var."
    echo "Example: S3_BUCKET=my-bucket ./scripts/backup.sh backup"
    exit 1
fi

if [ -z "$COMMAND" ]; then
    echo "Error: No command specified. Use: backup, restore, or list"
    exit 1
fi

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI not installed. Install with: brew install awscli"
    exit 1
fi

# Generate timestamp for backup (use provided timestamp if given)
if [ -n "$TIMESTAMP" ]; then
    BACKUP_TIMESTAMP="$TIMESTAMP"
else
    BACKUP_TIMESTAMP=$(date +%Y%m%d-%H%M%S)
fi

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Get list of worker directories
# Supports both local mode (data/worker1, data/worker2, ...) and EC2 mode (data/)
get_workers() {
    if [ -f "$WORKERS_DIR/id" ]; then
        # EC2 mode: single worker in data/
        echo "$WORKERS_DIR"
    else
        # Local mode: multiple workers in data/worker*
        find "$WORKERS_DIR" -maxdepth 1 -type d -name "worker*" 2>/dev/null | sort
    fi
}

# Get persistent tables from a worker directory
get_tables() {
    local worker_dir="$1"
    if [ -n "$TABLES" ]; then
        # Use specified tables
        echo "$TABLES" | tr ',' '\n' | while read table; do
            if [ -d "$worker_dir/$table" ]; then
                echo "$table"
            fi
        done
    else
        # Find all pt-* tables
        find "$worker_dir" -maxdepth 1 -type d -name "pt-*" -exec basename {} \; 2>/dev/null | sort
    fi
}

do_backup() {
    log "Starting backup to s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_TIMESTAMP"

    local total_files=0
    local total_size=0

    for worker_dir in $(get_workers); do
        # Use worker ID as name if available, otherwise use directory name
        if [ -f "$worker_dir/id" ]; then
            worker_name=$(cat "$worker_dir/id")
        else
            worker_name=$(basename "$worker_dir")
        fi
        log "Processing $worker_name..."

        # Backup worker ID file
        if [ -f "$worker_dir/id" ]; then
            local s3_path="s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_TIMESTAMP/$worker_name/id"
            if [ "$DRY_RUN" = true ]; then
                echo "  [DRY-RUN] Would upload: $worker_dir/id -> $s3_path"
            else
                aws s3 cp "$worker_dir/id" "$s3_path" --quiet
            fi
        fi

        for table in $(get_tables "$worker_dir"); do
            local table_dir="$worker_dir/$table"
            local file_count=$(find "$table_dir" -type f ! -name ".*" 2>/dev/null | wc -l | tr -d ' ')
            local dir_size=$(du -sh "$table_dir" 2>/dev/null | cut -f1)

            log "  $table: $file_count files ($dir_size)"

            local s3_path="s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_TIMESTAMP/$worker_name/$table/"

            if [ "$DRY_RUN" = true ]; then
                echo "  [DRY-RUN] Would sync: $table_dir -> $s3_path"
            else
                aws s3 sync "$table_dir" "$s3_path" \
                    --exclude ".*" \
                    --delete \
                    --quiet
            fi

            total_files=$((total_files + file_count))
        done
    done

    # Create a latest pointer
    if [ "$DRY_RUN" = false ]; then
        echo "$BACKUP_TIMESTAMP" | aws s3 cp - "s3://$S3_BUCKET/$S3_PREFIX/latest" --quiet
    fi

    log "Backup complete! Timestamp: $BACKUP_TIMESTAMP"
    log "Total files backed up: $total_files"

    if [ "$DRY_RUN" = false ]; then
        # Show S3 storage used
        local s3_size=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_TIMESTAMP" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3}')
        if [ -n "$s3_size" ]; then
            log "S3 storage used: $(numfmt --to=iec-i --suffix=B $s3_size 2>/dev/null || echo "$s3_size bytes")"
        fi
    fi
}

do_restore() {
    # Determine which timestamp to restore
    local restore_ts="$TIMESTAMP"

    if [ -z "$restore_ts" ]; then
        # Get latest timestamp
        restore_ts=$(aws s3 cp "s3://$S3_BUCKET/$S3_PREFIX/latest" - 2>/dev/null || echo "")
        if [ -z "$restore_ts" ]; then
            echo "Error: No backups found. Run 'list' to see available backups."
            exit 1
        fi
    fi

    log "Restoring from s3://$S3_BUCKET/$S3_PREFIX/$restore_ts"

    # Check if backup exists
    if ! aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/" &>/dev/null; then
        echo "Error: Backup $restore_ts not found."
        exit 1
    fi

    # EC2 mode: restore specific worker directly to data/
    if [ -n "$WORKER_ID" ]; then
        log "Restoring worker $WORKER_ID to $WORKERS_DIR/ (EC2 mode)..."

        # Get available tables from S3
        local s3_tables=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/$WORKER_ID/" | grep PRE | awk '{print $2}' | tr -d '/')

        # Filter tables if --tables specified
        local restore_tables="$s3_tables"
        if [ -n "$TABLES" ]; then
            restore_tables=""
            for t in $(echo "$TABLES" | tr ',' ' '); do
                if echo "$s3_tables" | grep -qx "$t"; then
                    restore_tables="$restore_tables $t"
                fi
            done
            restore_tables=$(echo "$restore_tables" | xargs)
        fi

        if [ "$DRY_RUN" = true ]; then
            echo "  [DRY-RUN] Would restore to: $WORKERS_DIR"
            echo "  Tables to restore:"
            for t in $restore_tables; do
                echo "    $t/"
            done
            if [ -n "$TABLES" ]; then
                echo "  (filtered by --tables)"
            fi
        else
            mkdir -p "$WORKERS_DIR"
            # Restore id file
            aws s3 cp "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/$WORKER_ID/id" "$WORKERS_DIR/id" --quiet 2>/dev/null || true
            # Restore each table
            for t in $restore_tables; do
                aws s3 sync "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/$WORKER_ID/$t/" "$WORKERS_DIR/$t/" \
                    --delete \
                    --quiet
            done
        fi

        log "Restore complete from timestamp: $restore_ts"
        return
    fi

    # Local mode: restore all workers to data/<worker_name>/
    local backup_workers=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/" | grep PRE | awk '{print $2}' | tr -d '/')

    for worker_name in $backup_workers; do
        local worker_dir="$WORKERS_DIR/$worker_name"
        log "Restoring $worker_name..."

        # Get available tables from S3
        local s3_tables=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/$worker_name/" | grep PRE | awk '{print $2}' | tr -d '/')

        # Filter tables if --tables specified
        local restore_tables="$s3_tables"
        if [ -n "$TABLES" ]; then
            restore_tables=""
            for t in $(echo "$TABLES" | tr ',' ' '); do
                if echo "$s3_tables" | grep -qx "$t"; then
                    restore_tables="$restore_tables $t"
                fi
            done
            restore_tables=$(echo "$restore_tables" | xargs)
        fi

        if [ "$DRY_RUN" = true ]; then
            echo "  [DRY-RUN] Would restore to: $worker_dir"
            echo "  Tables to restore:"
            for t in $restore_tables; do
                echo "    $t/"
            done
            if [ -n "$TABLES" ]; then
                echo "  (filtered by --tables)"
            fi
        else
            mkdir -p "$worker_dir"
            # Restore id file
            aws s3 cp "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/$worker_name/id" "$worker_dir/id" --quiet 2>/dev/null || true
            # Restore each table
            for t in $restore_tables; do
                aws s3 sync "s3://$S3_BUCKET/$S3_PREFIX/$restore_ts/$worker_name/$t/" "$worker_dir/$t/" \
                    --delete \
                    --quiet
            done
        fi
    done

    log "Restore complete from timestamp: $restore_ts"
}

do_list() {
    log "Available backups in s3://$S3_BUCKET/$S3_PREFIX/"
    echo ""

    # Get latest
    local latest=$(aws s3 cp "s3://$S3_BUCKET/$S3_PREFIX/latest" - 2>/dev/null || echo "none")

    # List all backup timestamps
    local backups=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" | grep PRE | awk '{print $2}' | tr -d '/' | grep -v '^$' | sort -r)

    if [ -z "$backups" ]; then
        echo "No backups found."
        return
    fi

    echo "TIMESTAMP            SIZE        TABLES"
    echo "-------------------------------------------"

    for ts in $backups; do
        local size=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/$ts" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3}')
        local size_human=$(numfmt --to=iec-i --suffix=B $size 2>/dev/null || echo "${size}B")
        local marker=""
        if [ "$ts" = "$latest" ]; then
            marker=" (latest)"
        fi
        printf "%-20s %-11s%s\n" "$ts" "$size_human" "$marker"
    done

    echo ""
    echo "To restore (local):  ./scripts/backup.sh restore --timestamp <TIMESTAMP>"
    echo "To restore (EC2):    ./scripts/backup.sh restore --worker-id <ID>"
}

# Execute command
case $COMMAND in
    backup)
        do_backup
        ;;
    restore)
        do_restore
        ;;
    list)
        do_list
        ;;
esac
