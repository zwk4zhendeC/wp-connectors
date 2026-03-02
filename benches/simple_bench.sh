#!/bin/bash

# Doris Sink 简单压测脚本

echo "==================================="
echo "Doris Sink 性能基准测试"
echo "==================================="
echo ""

# 检查是否安装了 cargo
if ! command -v cargo &> /dev/null; then
    echo "错误: 未找到 cargo 命令"
    exit 1
fi

# 运行基准测试
echo "正在运行基准测试..."
echo ""

cargo bench --bench doris_benchmark

echo ""
echo "==================================="
echo "基准测试完成！"
echo "==================================="
echo ""
echo "查看详细报告："
echo "  open target/criterion/report/index.html"
echo ""
