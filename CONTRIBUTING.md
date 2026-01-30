# Contributing to wp-connectors

Thank you for contributing to wp-connectors! This document describes the development workflow and contribution guidelines.

## Branch Strategy

This project uses a three-tier branch model:

```
feature/* ──► develop ──► release/x.y ──► main
              (alpha)      (beta/rc)     (stable)
```

### Branch Overview

| Branch | Purpose | Stability |
|--------|---------|-----------|
| `main` | Production releases | Stable |
| `develop` | Development mainline, continuous integration | Alpha |
| `release/x.y` | Release branch, feature-frozen, bug fixes only | Beta/RC |
| `feature/*` | Feature development branches | Unstable |
| `issues/*` | Issue fix branches | Unstable |

### Development Workflow

1. **Feature Development**
   - Create `feature/<name>` or `issues/<number>` branch from `develop`
   - Submit a PR to merge into `develop` when complete

2. **Release Preparation**
   - Create `release/x.y` branch from `develop`
   - Perform beta testing and bug fixes on the release branch
   - Publish RC versions for final validation

3. **Production Release**
   - Merge `release/x.y` into `main`
   - Tag on `main` (e.g. `v0.8.0`)
   - Merge `main` back into `develop` (sync release fixes)
   - Delete `release/x.y` branch

4. **Hotfix**
   - Create `hotfix/<name>` branch from `main`
   - After fixing, merge into both `main` and `develop`

### Version Naming

- Alpha: `x.y.z-alpha.n` (e.g. `0.8.0-alpha.1`)
- Beta: `x.y.z-beta.n` (e.g. `0.8.0-beta.1`)
- RC: `x.y.z-rc.n` (e.g. `0.8.0-rc.1`)
- Stable: `x.y.z` (e.g. `0.8.0`)

## Development Environment

### Build & Test

```bash
# Build
cargo build

# Format and lint
cargo fmt --all
cargo clippy --all-targets -- -D warnings

# Test
cargo test

# Skip Kafka integration tests
SKIP_KAFKA_INTEGRATION_TESTS=1 cargo test
```

### Feature Flags

```bash
# Default
cargo build

# Enable Prometheus
cargo build --features prometheus

# Enable Doris
cargo build --features doris

# All features
cargo build --all-features
```

## Commit Conventions

### Commit Message

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation update
- `refactor`: Code refactoring
- `test`: Test related
- `ci`: CI/CD related
- `chore`: Miscellaneous

**Example:**
```
feat(kafka): add batch compression support

- Add snappy and lz4 compression options
- Update configuration schema

Closes #42
```

### Pull Request

A PR should include:

1. **Title**: Concise description of the change
2. **Description**:
   - Purpose and scope
   - Implementation overview
   - Verification steps
3. **Linked Issue**: Use `Closes #<number>` or `Fixes #<number>`

### Code Review

- All PRs require at least one approving review
- CI checks must pass (format, lint, tests)
- Rebase onto the latest target branch before merging

## Code Standards

### Style Guide

- Rust Edition 2024
- 4-space indentation
- `snake_case` for functions and modules
- `CamelCase` for types and traits
- Use `anyhow::Result<T>` for error handling

### Testing Guidelines

- Unit tests go in the module or `tests/` directory
- Async tests use `#[tokio::test]`
- Tests requiring external services are gated by environment variables
- Test file naming: `tests/<area>/*_tests.rs`

### Security

- Never hardcode credentials
- Pass sensitive configuration via specs
- New dependencies must undergo security review

## Release Process

1. Update `CHANGELOG.md`
2. Update version in `Cargo.toml`
3. Create a release PR
4. Tag on `main` after merging
5. CI publishes automatically

## Getting Help

- See [AGENTS.md](./AGENTS.md) for project structure
- File an [Issue](https://github.com/wp-labs/wp-connectors/issues) to report problems
- Join [Discussions](https://github.com/wp-labs/wp-connectors/discussions)

## License

This project is licensed under [Apache License 2.0](./LICENSE). By contributing, you agree to license your contributions under the same license.

---

# 贡献指南（中文）

感谢你对 wp-connectors 的贡献！本文档介绍项目的开发流程和贡献规范。

## 分支策略

本项目采用三层分支模型：

```
feature/* ──► develop ──► release/x.y ──► main
              (alpha)      (beta/rc)     (stable)
```

### 分支说明

| 分支 | 用途 | 稳定性 |
|------|------|--------|
| `main` | 正式发布版本 | 稳定 |
| `develop` | 开发主线，持续集成 | Alpha |
| `release/x.y` | 版本发布分支，功能冻结，只修 bug | Beta/RC |
| `feature/*` | 功能开发分支 | 不稳定 |
| `issues/*` | Issue 修复分支 | 不稳定 |

### 开发流程

1. **功能开发**
   - 从 `develop` 创建 `feature/<name>` 或 `issues/<number>` 分支
   - 完成开发后，提交 PR 合并到 `develop`

2. **版本发布准备**
   - 从 `develop` 创建 `release/x.y` 分支
   - 在 release 分支上进行 Beta 测试和 bug 修复
   - 发布 RC 版本进行最终验证

3. **正式发布**
   - `release/x.y` 合并到 `main`
   - 在 `main` 上打 tag（如 `v0.8.0`）
   - `main` 合并回 `develop`（同步 release 中的修复）
   - 删除 `release/x.y` 分支

4. **热修复**
   - 从 `main` 创建 `hotfix/<name>` 分支
   - 修复后合并到 `main` 和 `develop`

### 版本命名

- Alpha: `x.y.z-alpha.n`（如 `0.8.0-alpha.1`）
- Beta: `x.y.z-beta.n`（如 `0.8.0-beta.1`）
- RC: `x.y.z-rc.n`（如 `0.8.0-rc.1`）
- 正式: `x.y.z`（如 `0.8.0`）

## 开发环境

### 构建和测试

```bash
# 构建
cargo build

# 格式化和 lint
cargo fmt --all
cargo clippy --all-targets -- -D warnings

# 测试
cargo test

# 跳过 Kafka 集成测试
SKIP_KAFKA_INTEGRATION_TESTS=1 cargo test
```

### 功能特性

```bash
# 默认
cargo build

# 启用 Prometheus
cargo build --features prometheus

# 启用 Doris
cargo build --features doris

# 全部特性
cargo build --all-features
```

## 提交规范

### Commit Message

使用 [Conventional Commits](https://www.conventionalcommits.org/) 格式：

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**类型：**
- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `refactor`: 代码重构
- `test`: 测试相关
- `ci`: CI/CD 相关
- `chore`: 其他杂项

**示例：**
```
feat(kafka): add batch compression support

- Add snappy and lz4 compression options
- Update configuration schema

Closes #42
```

### Pull Request

PR 应包含：

1. **标题**：简明描述变更内容
2. **描述**：
   - 变更目的和范围
   - 实现方案概述
   - 验证步骤
3. **关联 Issue**：使用 `Closes #<number>` 或 `Fixes #<number>`

### 代码审查

- 所有 PR 需要至少一个 Approving Review
- CI 检查必须通过（格式、lint、测试）
- 合并前需要 rebase 到目标分支最新提交

## 代码规范

### 风格指南

- Rust Edition 2024
- 4 空格缩进
- `snake_case` 用于函数和模块
- `CamelCase` 用于类型和 trait
- 使用 `anyhow::Result<T>` 处理错误

### 测试规范

- 单元测试放在模块内或 `tests/` 目录
- 异步测试使用 `#[tokio::test]`
- 需要外部服务的测试用环境变量控制
- 测试文件命名：`tests/<area>/*_tests.rs`

### 安全注意事项

- 不要硬编码凭证
- 敏感配置通过 spec 传入
- 新增依赖需进行安全审查

## 发布流程

1. 更新 `CHANGELOG.md`
2. 更新 `Cargo.toml` 版本号
3. 创建 release PR
4. 合并后在 `main` 打 tag
5. CI 自动发布

## 获取帮助

- 查看 [AGENTS.md](./AGENTS.md) 了解项目结构
- 提交 [Issue](https://github.com/wp-labs/wp-connectors/issues) 报告问题
- 参与 [Discussions](https://github.com/wp-labs/wp-connectors/discussions) 讨论

## 许可证

本项目采用 [Apache License 2.0](./LICENSE) 许可证。贡献代码即表示你同意以相同许可证授权你的贡献。
