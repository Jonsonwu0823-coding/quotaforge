-- 嵌入缓存：命中的调用记成 CACHED（不预留、不调上游、不动额度），结清的证据是 cache，cached_from 是原来那次的调用号。
--
-- 两个 ENUM 都只在末尾加值、加一列可为 NULL 的列：ALGORITHM=INSTANT 只改元数据，不拷表，旧版本的 QF 照常读写。
-- cached_from 不加外键：原来那次的调用不在了（例如数据库清空了、缓存的 Redis 还留着），命中照常写入，由 verify 报出来。
-- MySQL 8 的 ALTER TABLE 是原子的，所以只按 cached_from 在不在判断，迁移中途失败后可以重跑。
SET @qf_ddl = (SELECT IF(COUNT(*) = 0,
  'ALTER TABLE calls
     MODIFY COLUMN status ENUM(''RESERVED'', ''SETTLED'', ''CANCELLED'', ''USAGE_UNKNOWN'', ''IN_FLIGHT'', ''CACHED'') NOT NULL,
     MODIFY COLUMN resolution_evidence ENUM(''response'', ''not_sent'', ''late_response'', ''call_log'', ''bill'', ''cache'') NULL,
     ADD COLUMN cached_from VARCHAR(64) CHARACTER SET ascii NULL,
     ALGORITHM=INSTANT',
  'DO 0') FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name = 'calls' AND column_name = 'cached_from');
PREPARE qf_stmt FROM @qf_ddl;
EXECUTE qf_stmt;
DEALLOCATE PREPARE qf_stmt;
