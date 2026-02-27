#!/bin/bash

# Compile all Java source files

echo "🔨 Compiling Java sources..."

# Get the project root directory (parent of scripts directory)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_ROOT"

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all Java files with lib/*.jar on classpath
find src -name "*.java" | xargs javac -d bin -sourcepath src -cp "lib/*"

if [ $? -eq 0 ]; then
    echo "✅ Compilation successful!"
else
    echo "❌ Compilation failed!"
    exit 1
fi
