-- 幂等记录：每个 (账户, Idempotency-Key) 一行，指向这个键最近一次真正执行的调用，并存那次写回的响应快照。
-- 键按字节比较（ascii_bin），Abc 和 abc 是两个键。ascii_bin 会忽略尾部空格，但键里不允许空格。
-- 快照：response_status、response_bytes 有值表示 QF 写回了上游的 2xx；正文不超过上限时 response_body 存正文，
-- 超过上限时只记状态码和长度、正文为 NULL。四列都为 NULL 表示没有写回 2xx。
-- response_content_type 是 Content-Type 的值列表，以换行连接（值里不会有换行）；NULL 表示上游没给。
-- 写入大正文受服务端 max_allowed_packet 限制（MySQL 8.4 默认 64 MiB），远大于快照的上限。
CREATE TABLE IF NOT EXISTS idempotency_keys (
  account_id            BIGINT UNSIGNED NOT NULL,
  idem_key              VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  fingerprint           BINARY(32) NOT NULL,
  call_id               VARCHAR(64) CHARACTER SET ascii NOT NULL,
  expires_at            DATETIME(6) NOT NULL,
  response_status       SMALLINT NULL,
  response_bytes        BIGINT NULL,
  response_content_type MEDIUMBLOB NULL,
  response_body         LONGBLOB NULL,
  PRIMARY KEY (account_id, idem_key),
  KEY idempotency_keys_expires (expires_at),
  CONSTRAINT idempotency_keys_call FOREIGN KEY (call_id) REFERENCES calls (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 每次带键的调用都记下它用的键（包括 CANCELLED 的），幂等记录只指向最新的那次。
-- ALTER TABLE 没有 IF NOT EXISTS：先查列在不在，迁移中途失败后可以重跑。
SET @qf_ddl = (SELECT IF(COUNT(*) = 0,
  'ALTER TABLE calls ADD COLUMN idempotency_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL',
  'DO 0') FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name = 'calls' AND column_name = 'idempotency_key');
PREPARE qf_stmt FROM @qf_ddl;
EXECUTE qf_stmt;
DEALLOCATE PREPARE qf_stmt;
