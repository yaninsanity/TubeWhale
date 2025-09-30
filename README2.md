# 项目快速启动说明

以下说明以中文列出如何使用 Docker 快速启动并查看前端页面。

## 前提
- 已安装 Docker Desktop（mac / Windows）。Windows 推荐启用 WSL2 并使用 Linux 容器。
- 本说明假设你已将代码克隆到本地，并在项目根目录能看到 `start-docker.sh`。

## 步骤

1. 启动 Docker
   - mac：启动 Docker Desktop 应用，等待 Docker 状态为 Running。
   - Windows：启动 Docker Desktop（建议使用 WSL2），等待 Docker 状态为 Running。

2. 在终端进入项目目录
   - 打开 Terminal (mac) / WSL / Git Bash / PowerShell (Windows)，执行：
     cd /path/to/your/project
   - 例如：
     cd ~/Downloads/temp_projects/TubeWhale2_backend

3. 运行启动脚本（mac / Linux / Windows）
   - mac / Linux：
     chmod +x start-docker.sh
     ./start-docker.sh
   - Windows：
     - 推荐（WSL 或 Git Bash）：
       bash start-docker.sh
     - PowerShell（已启用 WSL）：
       wsl bash start-docker.sh
   （注：start-docker.sh 为 shell 脚本，Windows 原生 cmd/PowerShell 可能无法直接执行，推荐使用 WSL/Git Bash。）

4. 等待启动
   - 脚本运行后请等待一段时间（视网络与机器速度），在 Docker Desktop 的 Containers 列表中应能看到 5 个容器被创建并处于 Running 状态。
   - 也可以在终端运行 `docker ps` 来确认正在运行的容器。

5. 访问前端
   - 打开浏览器访问：
     http://localhost:8000/dashboard/templates/
   - 若页面未出现，请检查容器是否都为 Running，或查看容器日志排查错误（例如 `docker logs <container>`）。

## 后续管理（不需要在本地直接运行代码）
- 停止服务：
  - 在 Docker Desktop 中选中容器并停止，或在终端运行：
    docker stop <container_id或name>
  - 若使用 docker-compose（若仓库提供），可用：
    docker-compose down
- 启动服务：
  - 在 Docker Desktop 中启动容器，或：
    docker start <container_id或name>
  - 或再次运行 start-docker.sh（如果脚本支持重复启动）
- 查看日志与状态：
  - docker ps
  - docker logs <container_id或name>

