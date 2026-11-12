CREATE ROLE querystorm_app LOGIN PASSWORD 'querystorm_app';
CREATE ROLE querystorm_monitor LOGIN PASSWORD 'querystorm_monitor';
CREATE EXTENSION pg_stat_statements;
GRANT CONNECT ON DATABASE querystorm TO querystorm_app;
GRANT CONNECT ON DATABASE querystorm TO querystorm_monitor;
GRANT CREATE ON SCHEMA public TO querystorm_app;
GRANT pg_monitor TO querystorm_monitor;
