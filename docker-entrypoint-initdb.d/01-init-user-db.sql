-- TubeWhale PostgreSQL Development Database Initialization

-- Create user if not exists
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'tubewhale_dev') THEN

      CREATE ROLE tubewhale_dev LOGIN PASSWORD 'dev123';
   END IF;
END
$do$;

-- Create database if not exists
SELECT 'CREATE DATABASE tubewhale_dev'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tubewhale_dev')\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE tubewhale_dev TO tubewhale_dev;

-- Connect to the new database and create extensions
\c tubewhale_dev

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create custom functions
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Set database configuration
ALTER DATABASE tubewhale_dev SET timezone TO 'Asia/Shanghai';
ALTER DATABASE tubewhale_dev SET default_text_search_config TO 'english';

-- Grant privileges on the public schema
GRANT ALL ON SCHEMA public TO tubewhale_dev;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO tubewhale_dev;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO tubewhale_dev;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO tubewhale_dev;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO tubewhale_dev;