-- 用量事件：调用结清（SETTLED、CANCELLED、CACHED）时，在同一个事务里往 outbox 写一行；后台转发到消息队列，
-- 消费者按调用号去重（usage_seen）、按账户按天汇总（usage_daily）。只新建表，不动已有的表；都可以重跑。

-- 开关：一行，enabled_at 为 NULL 是关着（迁移之后默认关）。配了用量事件的 QF 启动时打开，只开不关。
-- 结清事务按它决定写不写 outbox（见 ledger.writeEvent），所以 QF 和 qfctl 用的是同一个开关。
CREATE TABLE IF NOT EXISTS usage_events (
  id         TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  enabled_at DATETIME(6) NULL,
  CONSTRAINT usage_events_one_row CHECK (id = 1)
) ENGINE=InnoDB;
INSERT IGNORE INTO usage_events (id, enabled_at) VALUES (1, NULL);

-- outbox：一个调用最多一行，内容从结清时的调用行抄。seq 是插入的顺序（不是提交的顺序），转发按它取；
-- sent_at 为 NULL 是还没发出。(sent_at, seq) 给转发取没发出的行、清理取发出很久的行用。
CREATE TABLE IF NOT EXISTS outbox (
  seq                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  call_id                 VARCHAR(64) CHARACTER SET ascii NOT NULL,
  account_id              BIGINT UNSIGNED NOT NULL,
  purpose                 ENUM('chat', 'embeddings') NOT NULL,
  model_alias             VARCHAR(255) NOT NULL,
  upstream                VARCHAR(255) NOT NULL,
  upstream_model          VARCHAR(255) NOT NULL,
  status                  ENUM('RESERVED', 'SETTLED', 'CANCELLED', 'USAGE_UNKNOWN', 'IN_FLIGHT', 'CACHED') NOT NULL,
  usage_prompt_tokens     BIGINT NULL,
  usage_completion_tokens BIGINT NULL,
  usage_total_tokens      BIGINT NULL,
  resolved_at             DATETIME(6) NOT NULL,
  cached_from             VARCHAR(64) CHARACTER SET ascii NULL,
  created_at              DATETIME(6) NOT NULL,
  sent_at                 DATETIME(6) NULL,
  UNIQUE KEY outbox_call (call_id),
  KEY outbox_sent (sent_at, seq),
  CONSTRAINT outbox_call FOREIGN KEY (call_id) REFERENCES calls (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 消费者的两张表。真实部署里它们在下游自己的库里，所以不加指向账本的外键。
-- usage_seen：处理过的调用号（去重），和汇总在同一个事务里写；不清理，清理了的话比保留期还晚到的重复会被算两次。
CREATE TABLE IF NOT EXISTS usage_seen (
  call_id     VARCHAR(64) CHARACTER SET ascii NOT NULL PRIMARY KEY,
  account_id  BIGINT UNSIGNED NOT NULL,
  day         DATE NOT NULL,
  purpose     ENUM('chat', 'embeddings') NOT NULL,
  consumed_at DATETIME(6) NOT NULL,
  KEY usage_seen_day (day, account_id)
) ENGINE=InnoDB;

-- usage_daily：按账户、UTC 日期、用途的汇总。结算的调用数和用量（就是这一天的已消费）、缓存命中的调用数和省下的
-- total、取消的调用数。
CREATE TABLE IF NOT EXISTS usage_daily (
  account_id        BIGINT UNSIGNED NOT NULL,
  day               DATE NOT NULL,
  purpose           ENUM('chat', 'embeddings') NOT NULL,
  settled_calls     BIGINT NOT NULL,
  prompt_tokens     BIGINT NOT NULL,
  completion_tokens BIGINT NOT NULL,
  total_tokens      BIGINT NOT NULL,
  cached_calls      BIGINT NOT NULL,
  cached_tokens     BIGINT NOT NULL,
  cancelled_calls   BIGINT NOT NULL,
  PRIMARY KEY (account_id, day, purpose)
) ENGINE=InnoDB;
