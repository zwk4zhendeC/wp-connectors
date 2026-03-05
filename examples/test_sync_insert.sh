#!/bin/bash
# 测试同步插入是否能正确捕获表结构不匹配错误

set -e

echo "=========================================="
echo "测试 ClickHouse 同步插入错误检测"
echo "=========================================="
echo ""

# 检查 ClickHouse 是否运行
if ! curl -s "http://localhost:8123/ping" > /dev/null 2>&1; then
    echo "❌ ClickHouse 未运行，请先启动 ClickHouse"
    exit 1
fi

echo "✅ ClickHouse 正在运行"
echo ""

# 创建测试数据库和表
echo "📋 创建测试表..."
curl -u default:default -s "http://localhost:8123/" --data-binary "
CREATE DATABASE IF NOT EXISTS test_sync
"

# 创建一个简单的表
curl -u default:default -s "http://localhost:8123/" --data-binary "
CREATE TABLE IF NOT EXISTS test_sync.simple_table (
    id UInt64,
    name String,
    value Int64
) ENGINE = MergeTree()
ORDER BY id
"

echo "✅ 测试表创建成功"
echo ""

# 测试1: 正确的数据格式（应该成功）
echo "测试1: 插入正确格式的数据..."
RESULT=$(curl -u default:default -s "http://localhost:8123/?async_insert=0" \
    --data-binary "INSERT INTO test_sync.simple_table FORMAT JSONEachRow" \
    --data-binary '{"id":1,"name":"test","value":100}')

if [ -z "$RESULT" ]; then
    echo "✅ 正确格式数据插入成功"
else
    echo "❌ 插入失败: $RESULT"
fi
echo ""

# 测试2: 错误的字段名（应该失败）
echo "测试2: 插入错误字段名的数据（应该立即报错）..."
RESULT=$(curl -u default:default -s "http://localhost:8123/?async_insert=0" \
    --data-binary "INSERT INTO test_sync.simple_table FORMAT JSONEachRow" \
    --data-binary '{"id":2,"wrong_field":"test","value":200}' 2>&1)

if echo "$RESULT" | grep -q "Code:"; then
    echo "✅ 正确捕获错误: $(echo $RESULT | grep -o 'Code: [0-9]*')"
else
    echo "❌ 未捕获错误（这是问题！）"
fi
echo ""

# 测试3: 错误的数据类型（应该失败）
echo "测试3: 插入错误数据类型（应该立即报错）..."
RESULT=$(curl -u default:default -s "http://localhost:8123/?async_insert=0" \
    --data-binary "INSERT INTO test_sync.simple_table FORMAT JSONEachRow" \
    --data-binary '{"id":"not_a_number","name":"test","value":300}' 2>&1)

if echo "$RESULT" | grep -q "Code:"; then
    echo "✅ 正确捕获错误: $(echo $RESULT | grep -o 'Code: [0-9]*')"
else
    echo "❌ 未捕获错误（这是问题！）"
fi
echo ""

# 验证数据
echo "📊 验证插入的数据..."
COUNT=$(curl -u default:default -s "http://localhost:8123/?query=SELECT+COUNT(*)+FROM+test_sync.simple_table")
echo "表中记录数: $COUNT"

if [ "$COUNT" = "1" ]; then
    echo "✅ 只有正确的数据被插入（错误的数据被拒绝）"
else
    echo "⚠️  记录数不符合预期"
fi
echo ""

# 清理
echo "🧹 清理测试数据..."
curl -u default:default -s "http://localhost:8123/" --data-binary "DROP TABLE IF EXISTS test_sync.simple_table"
curl -u default:default -s "http://localhost:8123/" --data-binary "DROP DATABASE IF EXISTS test_sync"

echo "✅ 清理完成"
echo ""
echo "=========================================="
echo "测试完成"
echo "=========================================="
