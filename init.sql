# TubeWhale PostgreSQL 初始化脚本
-- 创建数据库扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 创建自定义函数和触发器
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 设置数据库配置
ALTER DATABASE tubewhale SET timezone TO 'Asia/Shanghai';
ALTER DATABASE tubewhale SET default_text_search_config TO 'english';

-- 创建索引优化查询性能
-- 这些索引将在 Django migrate 后添加
