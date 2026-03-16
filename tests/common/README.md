# 测试工具使用说明

## Component Trait - 统一组件管理接口

所有工具都实现了 `Component` trait，提供统一的组件管理接口：

```rust
pub trait Component {
    fn pull_dependencies(&self) -> Result<()>;  // 拉取依赖
    fn up(&self) -> Result<()>;                  // 启动组件
    fn down(&self) -> Result<()>;                // 停止组件
    fn restart(&self) -> Result<()>;             // 重启组件
    fn setup_and_up(&self) -> Result<()>;        // 完整启动流程
}
```

### 统一接口使用示例

```rust
use common::{Component, DockerComposeTool, ShellScriptTool};
use anyhow::Result;

#[test]
fn test_unified_interface() -> Result<()> {
    // 使用 Docker Compose 工具
    let docker: Box<dyn Component> = Box::new(
        DockerComposeTool::new(".other/clickhouse/examples/docker-compose.yml")?
    );
    
    // 统一接口操作
    docker.setup_and_up()?;  // 拉取依赖并启动
    // docker.restart()?;     // 重启
    // docker.down()?;        // 停止
    
    // 使用 Shell 脚本工具
    let shell: Box<dyn Component> = Box::new(
        ShellScriptTool::new("install.sh", "start.sh", "stop.sh")?
    );
    
    shell.setup_and_up()?;
    
    Ok(())
}
```

## 1. Docker Compose 操作工具

用于自动化管理 Docker Compose 服务。

### 使用示例

```rust
use anyhow::Result;

#[test]
fn test_docker_compose() -> Result<()> {
    let tool = common::DockerComposeTool::new(".other/clickhouse/examples/docker-compose.yml")?;
    
    // 拉取镜像并启动
    tool.pull_and_up()?;
    
    // 或者分步操作
    // tool.pull()?;
    // tool.up()?;
    
    // 查看状态
    let status = tool.ps()?;
    println!("服务状态: {}", status);
    
    // 停止服务
    // tool.down()?;
    
    Ok(())
}
```

### API 方法

#### Component Trait 方法
- `pull_dependencies()` - 拉取镜像
- `up()` - 启动服务
- `down()` - 停止服务
- `restart()` - 重启服务
- `setup_and_up()` - 拉取镜像并启动

#### 专有方法
- `new(compose_file)` - 创建工具实例
- `pull()` - 拉取镜像（同 pull_dependencies）
- `ps()` - 查看服务状态
- `pull_and_up()` - 拉取镜像并启动（同 setup_and_up）

## 2. Shell 脚本操作工具

用于管理依赖安装、服务启动和停止脚本。

### 使用示例

```rust
use anyhow::Result;

#[test]
fn test_shell_scripts() -> Result<()> {
    let tool = common::ShellScriptTool::new(
        "./scripts/install.sh",
        "./scripts/start.sh",
        "./scripts/stop.sh",
    )?;
    
    // 安装依赖
    tool.install()?;
    
    // 启动服务
    tool.start()?;
    
    // 停止服务
    // tool.stop()?;
    
    // 重启服务
    // tool.restart()?;
    
    Ok(())
}
```

### API 方法

#### Component Trait 方法
- `pull_dependencies()` - 安装依赖
- `up()` - 启动服务
- `down()` - 停止服务
- `restart()` - 重启服务
- `setup_and_up()` - 安装依赖并启动

#### 专有方法
- `new(install_deps_sh, start_sh, stop_sh)` - 创建工具实例
- `install()` - 执行依赖安装脚本（同 pull_dependencies）
- `start()` - 执行启动脚本（同 up）
- `stop()` - 执行停止脚本（同 down）

## 完整测试示例

```rust
mod common;

use common::{Component, DockerComposeTool, ShellScriptTool};
use anyhow::Result;

#[test]
fn test_clickhouse_with_component_trait() -> Result<()> {
    // 使用 Component trait 统一接口
    let docker_tool = DockerComposeTool::new(
        ".other/clickhouse/examples/docker-compose.yml"
    )?;
    
    // 完整启动流程
    docker_tool.setup_and_up()?;
    
    // 等待服务就绪
    std::thread::sleep(std::time::Duration::from_secs(5));
    
    // 运行测试...
    
    // 重启服务
    // docker_tool.restart()?;
    
    // 清理
    docker_tool.down()?;
    
    Ok(())
}

#[test]
fn test_with_scripts_component_trait() -> Result<()> {
    let sh_tool = ShellScriptTool::new(
        "./scripts/install_deps.sh",
        "./scripts/start_service.sh",
        "./scripts/stop_service.sh",
    )?;
    
    // 使用统一接口
    sh_tool.setup_and_up()?;
    
    // 运行测试...
    
    sh_tool.down()?;
    
    Ok(())
}

#[test]
fn test_polymorphic_components() -> Result<()> {
    // 多态使用不同组件
    let components: Vec<Box<dyn Component>> = vec![
        Box::new(DockerComposeTool::new(".other/clickhouse/examples/docker-compose.yml")?),
        Box::new(ShellScriptTool::new("install.sh", "start.sh", "stop.sh")?),
    ];
    
    for component in components {
        component.setup_and_up()?;
        // 运行测试...
        component.down()?;
    }
    
    Ok(())
}
```

## 注意事项

1. 确保 Docker 已安装并运行
2. Shell 脚本需要有执行权限
3. 所有路径都相对于项目根目录
4. 工具会自动打印执行日志
5. 错误会通过 `anyhow::Result` 返回
