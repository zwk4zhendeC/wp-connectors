#!/bin/bash

# ClickHouse 测试环境设置脚本

echo "========================================="
echo "ClickHouse Sink 测试环境设置"
echo "========================================="
echo ""

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装 Docker"
    exit 1
fi

echo "✅ Docker 已安装"
echo ""

# 检查 ClickHouse 容器是否运行
if docker ps | grep -q clickhouse-server; then
    echo "✅ ClickHouse 容器已在运行"
    CONTAINER_ID=$(docker ps | grep clickhouse-server | awk '{print $1}')
    echo "   容器 ID: $CONTAINER_ID"
else
    echo "📦 启动 ClickHouse 容器..."
    docker run -d \
        --name clickhouse-server \
        -p 8123:8123 \
        -p 9000:9000 \
        --ulimit nofile=262144:262144 \
        clickhouse/clickhouse-server:latest
    
    if [ $? -eq 0 ]; then
        echo "✅ ClickHouse 容器启动成功"
        echo "   等待 ClickHouse 初始化..."
        sleep 5
    else
        echo "❌ ClickHouse 容器启动失败"
        exit 1
    fi
fi

echo ""
echo "📊 创建测试数据库和表..."

# 创建数据库
docker exec -it clickhouse-server clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test_db"

# 创建表
docker exec -it clickhouse-server clickhouse-client --query "
CREATE TABLE IF NOT EXISTS test_db.wp_nginx (
    wp_event_id        Int64,
    wp_src_key         LowCardinality(String),
    sip                String,
    \`timestamp\`      String,
    \`http/request\`   String,
    status             UInt16,
    size               UInt32,
    referer            String,
    \`http/agent\`     String
) ENGINE = MergeTree()
ORDER BY (timestamp, wp_event_id);
"

if [ $? -eq 0 ]; then
    echo "✅ 数据库和表创建成功"
else
    echo "❌ 数据库和表创建失败"
    exit 1
fi

echo ""
echo "========================================="
echo "环境设置完成！"
echo "========================================="
echo ""
echo "现在可以运行测试："
echo "  cargo run --release --example clickhouse_sink_concurrent_test --features clickhouse"
echo ""
echo "查看数据："
echo "  docker exec -it clickhouse-server clickhouse-client --query \"SELECT COUNT(*) FROM test_db.wp_nginx\""
echo ""
echo "停止容器："
echo "  docker stop clickhouse-server"
echo ""
echo "删除容器："
echo "  docker rm clickhouse-server"
echo ""
