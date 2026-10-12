-- 账户、额度、Key、调用、账本。数值都是有符号 BIGINT：允许超用，上限 − 已预留 − 已消费 可以是负数，
-- UNSIGNED 做这个减法会直接报错。时间都由 UTC_TIMESTAMP(6) 写入，不依赖服务器时区。
-- MySQL 的 DDL 会隐式提交，迁移中途失败时只能重跑，所以每条都是 IF NOT EXISTS。

CREATE TABLE IF NOT EXISTS accounts (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(64) CHARACTER SET ascii NOT NULL,
  created_at DATETIME(6) NOT NULL,
  UNIQUE KEY accounts_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 每个账户一行，单位 token；不按周期重置。
CREATE TABLE IF NOT EXISTS budgets (
  account_id      BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  limit_tokens    BIGINT NOT NULL,
  reserved_tokens BIGINT NOT NULL DEFAULT 0,
  consumed_tokens BIGINT NOT NULL DEFAULT 0,
  updated_at      DATETIME(6) NOT NULL,
  CONSTRAINT budgets_account FOREIGN KEY (account_id) REFERENCES accounts (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 只存 Key 的 SHA-256 和用来辨认的前缀，不存明文。models 是允许的别名数组：鉴权每次都读整份，从不按别名反查。
CREATE TABLE IF NOT EXISTS api_keys (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT UNSIGNED NOT NULL,
  sha256     CHAR(64) CHARACTER SET ascii NOT NULL,
  prefix     VARCHAR(16) CHARACTER SET ascii NOT NULL,
  models     JSON NOT NULL,
  status     ENUM('active', 'revoked') NOT NULL,
  created_at DATETIME(6) NOT NULL,
  revoked_at DATETIME(6) NULL,
  UNIQUE KEY api_keys_sha256 (sha256),
  CONSTRAINT api_keys_account FOREIGN KEY (account_id) REFERENCES accounts (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 每次预留成功的调用一行，id 是 QF 的调用号（x-request-id）。usage 三列为 NULL 表示未知。
-- 客户端头和上游给的号按日志的同一条规则存（不超过 128 个字符），不合法就存 NULL。
CREATE TABLE IF NOT EXISTS calls (
  id                      VARCHAR(64) CHARACTER SET ascii NOT NULL PRIMARY KEY,
  account_id              BIGINT UNSIGNED NOT NULL,
  key_id                  BIGINT UNSIGNED NOT NULL,
  status                  ENUM('RESERVED', 'SETTLED', 'CANCELLED', 'USAGE_UNKNOWN') NOT NULL,
  purpose                 ENUM('chat', 'embeddings') NOT NULL,
  model_alias             VARCHAR(255) NOT NULL,
  upstream                VARCHAR(255) NOT NULL,
  upstream_model          VARCHAR(255) NOT NULL,
  est_input_tokens        BIGINT NOT NULL,
  est_output_tokens       BIGINT NOT NULL,
  reserved_tokens         BIGINT NOT NULL,
  usage_prompt_tokens     BIGINT NULL,
  usage_completion_tokens BIGINT NULL,
  usage_total_tokens      BIGINT NULL,
  client_request_id       VARCHAR(128) NULL,
  run_id                  VARCHAR(128) NULL,
  upstream_x_request_id   VARCHAR(128) NULL,
  upstream_response_id    VARCHAR(128) NULL,
  response_model          VARCHAR(128) NULL,
  http_status             SMALLINT NULL,
  error_code              VARCHAR(64) NULL,
  created_at              DATETIME(6) NOT NULL,
  finished_at             DATETIME(6) NULL,
  KEY calls_account_status (account_id, status),
  CONSTRAINT calls_account FOREIGN KEY (account_id) REFERENCES accounts (id),
  CONSTRAINT calls_key FOREIGN KEY (key_id) REFERENCES api_keys (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 追加式账本：每行是“已预留”“已消费”的变动量。(call_id, kind) 唯一，同一个调用不会结算或释放两次。
CREATE TABLE IF NOT EXISTS ledger (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  call_id        VARCHAR(64) CHARACTER SET ascii NOT NULL,
  kind           ENUM('reserve', 'settle', 'release') NOT NULL,
  reserved_delta BIGINT NOT NULL,
  consumed_delta BIGINT NOT NULL,
  created_at     DATETIME(6) NOT NULL,
  UNIQUE KEY ledger_call_kind (call_id, kind),
  CONSTRAINT ledger_call FOREIGN KEY (call_id) REFERENCES calls (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
