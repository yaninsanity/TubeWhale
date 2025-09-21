# TubeWhale Django Backend - Production Ready Dockerfile
# 多阶段构建，优化镜像大小和安全性

# Stage 1: Build stage (use uv for fast installs & caching)
FROM ghcr.io/astral-sh/uv:0.4.20-python3.11-bookworm AS builder
LABEL org.opencontainers.image.source="https://github.com/yaninsanity/TubeWhale" \
    org.opencontainers.image.title="TubeWhale Backend" \
    org.opencontainers.image.description="TubeWhale Django backend with prompt + execution supervision" \
    org.opencontainers.image.licenses="MIT"

# 设置构建参数
ARG DEBIAN_FRONTEND=noninteractive

# 安装系统依赖（仅构建时需要）
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    default-libmysqlclient-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python & uv 环境
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy

# 创建虚拟环境并使用 uv 安装依赖
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 复制并安装Python依赖（利用 Docker 缓存）
COPY requirements.txt django_requirements.txt ./
RUN uv pip install --upgrade pip setuptools wheel && \
    uv pip install -r requirements.txt -r django_requirements.txt

# Stage 2: Production stage
FROM python:3.11-slim AS production
ARG BUILD_REVISION=dev
LABEL org.opencontainers.image.revision="$BUILD_REVISION"

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DJANGO_SETTINGS_MODULE=tubewhale_project.settings \
    PATH="/opt/venv/bin:$PATH"

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libpq5 \
    default-mysql-client \
    gettext \
    curl \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 创建应用用户（安全最佳实践）
RUN groupadd -r tubewhale && useradd -r -g tubewhale tubewhale

# 复制虚拟环境
COPY --from=builder /opt/venv /opt/venv

# 设置工作目录
WORKDIR /app

# 复制应用代码 (避免无关文件进入镜像，可后续加 .dockerignore 优化)
COPY --chown=tubewhale:tubewhale . .

# CLI wrapper
COPY --chown=tubewhale:tubewhale run_cli.sh /app/run_cli.sh
RUN chmod +x /app/run_cli.sh

# 创建必要的目录
RUN mkdir -p /app/logs /app/media /app/staticfiles && \
    chown -R tubewhale:tubewhale /app

# 切换到非root用户
USER tubewhale

# 收集静态文件
RUN python manage.py collectstatic --noinput || true

# 编译翻译文件
RUN python manage.py compilemessages || true

# 健康检查 (使用公开健康检查端点)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD bash -c 'command -v ffmpeg >/dev/null 2>&1 && curl -f http://localhost:8000/api/v1/tubewhale/health/ || exit 1'

# 暴露端口
EXPOSE 8000

# 启动脚本
COPY docker-entrypoint.sh /docker-entrypoint.sh
USER root
RUN chmod +x /docker-entrypoint.sh \
    && chmod +x /app/start_server.sh || true
USER tubewhale

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/app/start_server.sh"]
