#!/bin/bash

# Deploy code to EC2 instances using rsync

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Load .env file
ENV_FILE="$PROJECT_ROOT/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ ERROR: .env not found!"
    echo "  Copy .env.example to .env and fill in your values."
    exit 1
fi

source "$ENV_FILE"

# Expand ~ in SSH key path
EC2_SSH_KEY="${EC2_SSH_KEY/#\~/$HOME}"

echo "🚀 === Deploying to EC2 Instances ==="
echo ""

for host in $EC2_HOSTS; do
    echo "📤 Deploying to $host..."
    rsync -avz --delete \
        --exclude 'data/' \
        --exclude 'logs/' \
        --exclude 'bin/' \
        --exclude '.git/' \
        --exclude '/*.jar' \
        -e "ssh -i $EC2_SSH_KEY -o StrictHostKeyChecking=no" \
        "$PROJECT_ROOT/" "$EC2_SSH_USER@$host:$EC2_REMOTE_DIR/"

    if [ $? -eq 0 ]; then
        echo "  ✅ Done"
    else
        echo "  ❌ Failed"
    fi

    # Sync AWS credentials and config if they exist locally
    if [ -f "$HOME/.aws/credentials" ] || [ -f "$HOME/.aws/config" ]; then
        echo "  🔑 Syncing AWS credentials..."
        ssh -i "$EC2_SSH_KEY" -o StrictHostKeyChecking=no "$EC2_SSH_USER@$host" "mkdir -p ~/.aws"
        if [ -f "$HOME/.aws/credentials" ]; then
            rsync -avz \
                -e "ssh -i $EC2_SSH_KEY -o StrictHostKeyChecking=no" \
                "$HOME/.aws/credentials" "$EC2_SSH_USER@$host:~/.aws/credentials"
        fi
        if [ -f "$HOME/.aws/config" ]; then
            rsync -avz \
                -e "ssh -i $EC2_SSH_KEY -o StrictHostKeyChecking=no" \
                "$HOME/.aws/config" "$EC2_SSH_USER@$host:~/.aws/config"
        fi
        echo "  ✅ AWS credentials synced"
    fi
    echo ""
done

echo "🎉 === Deployment Complete ==="
