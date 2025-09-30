# TubeWhale3 CLI — 简明使用说明

下面说明如何准备环境、配置并运行项目的命令行程序（cli）。

一、准备 .env（放在项目根目录）

若根目录没有`.env`文件, 则在项目根目录创建一个名为 `.env` 的文件，包含以下字段（示例）：

若存在则根据需要修改。

```env
# .env 示例（放在项目根目录
# 多个 key 用逗号隔开
YOUTUBE_API_KEYS=key1,key2,key3
# 向 weilu 获取统一 key
OPENAI_API_KEY=your_openai_api_key

# Pipeline flags
FULL_AUDIO_ANALYSIS=true
PERSIST_AGENT_SUMMARIES=true
DRY_RUN=false

# Search & analysis
# 搜索关键词
KEYWORD=""
# 联想词汇数量（keyword generation）
MAX_N=2
# top 几（搜索结果）
TOP_K=2
FILTER_TYPE="view_count"

# Storage & concurrency
# 数据库文件名，建议每次试验换个名字以避免覆盖
DB_PATH="xxx.db"
CONCURRENCY=2
```

二、Optional：修改 prompt 配置

项目的 OpenAI 模型与 prompt 模板位于：
`utils/openai_config.yaml`

这是可选的，针对特定任务你可以(根据用户提供的问题对prompt进行调整)：
- 调整 summarization 的prompt以满足不同摘要风格；
- 调整 structured_output 的字段结构以匹配你需要的 JSON schema。

三、先决条件

- 已安装 Python

四、创建并激活虚拟环境（按平台）

cd 到项目根目录后，根据你的操作系统和 shell 选择对应命令运行并安装依赖。

macOS / Linux (bash / zsh)：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows — PowerShell：

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Windows — cmd：

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
```

五、运行 CLI

确保在项目根目录并激活了虚拟环境后，直接运行：

```bash
python cli.py
```
