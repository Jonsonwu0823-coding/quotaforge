-- 未知用量与恢复：发送意图（IN_FLIGHT）、结清的审计，以及回收扫描用的索引。
--
-- 新的状态值加在 ENUM 末尾：已有值的序号不变，不用整表复制。sent_at 是发送意图落库的时间；
-- 结清（变成 SETTLED 或 CANCELLED）的那一次转换写 resolved_by、resolution_evidence、resolved_at，谁、凭什么、什么时候。
-- MySQL 8 的 ALTER TABLE 是原子的：这一条要么整条生效，要么没有，所以只按 sent_at 在不在判断，迁移中途失败后可以重跑。
SET @qf_ddl = (SELECT IF(COUNT(*) = 0,
  'ALTER TABLE calls
     MODIFY COLUMN status ENUM(''RESERVED'', ''SETTLED'', ''CANCELLED'', ''USAGE_UNKNOWN'', ''IN_FLIGHT'') NOT NULL,
     ADD COLUMN sent_at DATETIME(6) NULL,
     ADD COLUMN resolved_by ENUM(''gateway'', ''reclaim'', ''reconcile'') NULL,
     ADD COLUMN resolution_evidence ENUM(''response'', ''not_sent'', ''late_response'', ''call_log'', ''bill'') NULL,
     ADD COLUMN resolved_at DATETIME(6) NULL,
     ADD KEY calls_status_created (status, created_at)',
  'DO 0') FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name = 'calls' AND column_name = 'sent_at');
PREPARE qf_stmt FROM @qf_ddl;
EXECUTE qf_stmt;
DEALLOCATE PREPARE qf_stmt;

-- 回填迁移前的调用，只写从已有列推得出的值，推不出的留 NULL。每条都带条件，重跑不会改第二次。
-- 迁移前的 RESERVED 可能已经发出（收尾失败的那种），不能被回收当成没发出而释放：改成 IN_FLIGHT，
-- 发送意图的时间取 created_at，回收会把它标成未知。
UPDATE calls SET status = 'IN_FLIGHT', sent_at = created_at WHERE status = 'RESERVED';
-- 迁移前收尾的调用都是网关收尾的；证据按状态和错误码推：上游 4xx（三个错误码）是拿到的回答，其余释放是确定没发出。
UPDATE calls SET resolved_by = 'gateway', resolved_at = finished_at,
  resolution_evidence = IF(status = 'SETTLED' OR error_code IN ('upstream_bad_request', 'upstream_auth_failed', 'upstream_rate_limited'),
    'response', 'not_sent')
  WHERE status IN ('SETTLED', 'CANCELLED') AND resolved_by IS NULL;
-- 其余迁移前的调用推不出发送的时间，sent_at 留 NULL；verify 不要求它们有（见 Verify 的 sent 规则）。
