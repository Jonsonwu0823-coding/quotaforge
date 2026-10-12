-- 限流的限额：Key 和账户各有每分钟的请求数（rpm_limit）和 token 数（tpm_limit），0 表示这一层不限。
-- 已有的 Key 和账户迁移后都是不限。
-- MySQL 8 的单条 ALTER TABLE 是原子的：每张表一条，按 rpm_limit 在不在判断，迁移中途失败后可以重跑。
SET @qf_ddl = (SELECT IF(COUNT(*) = 0,
  'ALTER TABLE api_keys
     ADD COLUMN rpm_limit BIGINT NOT NULL DEFAULT 0,
     ADD COLUMN tpm_limit BIGINT NOT NULL DEFAULT 0',
  'DO 0') FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name = 'api_keys' AND column_name = 'rpm_limit');
PREPARE qf_stmt FROM @qf_ddl;
EXECUTE qf_stmt;
DEALLOCATE PREPARE qf_stmt;

SET @qf_ddl = (SELECT IF(COUNT(*) = 0,
  'ALTER TABLE accounts
     ADD COLUMN rpm_limit BIGINT NOT NULL DEFAULT 0,
     ADD COLUMN tpm_limit BIGINT NOT NULL DEFAULT 0',
  'DO 0') FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name = 'accounts' AND column_name = 'rpm_limit');
PREPARE qf_stmt FROM @qf_ddl;
EXECUTE qf_stmt;
DEALLOCATE PREPARE qf_stmt;
