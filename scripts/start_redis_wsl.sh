#!/bin/sh
# 仅用于本机实验：从官方Alpine包提取到项目目录，不修改系统服务。
set -eu
PROJECT_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
mkdir -p "$PROJECT_ROOT/.local/redis"
for package in "$PROJECT_ROOT"/.local/packages/*.apk; do
    tar -xzf "$package" -C "$PROJECT_ROOT/.local/redis" 2>/dev/null || true
done
export LD_LIBRARY_PATH="$PROJECT_ROOT/.local/redis/usr/lib:$PROJECT_ROOT/.local/redis/lib"
exec "$PROJECT_ROOT/.local/redis/usr/bin/redis-server" \
    --bind 127.0.0.1 --port 16379 --save '' --appendonly no --daemonize yes \
    --logfile "$PROJECT_ROOT/.local/redis.log" --pidfile "$PROJECT_ROOT/.local/redis.pid" \
    --maxmemory-policy noeviction
