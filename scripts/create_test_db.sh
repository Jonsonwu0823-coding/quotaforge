#!/usr/bin/env bash
# 建测试库 quotaforge_test 和两个账号；CI 和本机共用。
#   qf_migrate：库内全部权限，用于建表、改表
#   qf_app：只有增删改查，没有建表、改表权限
# 用法：create_test_db.sh <以 root 连 MySQL 的命令...>，例如
#   docker exec -i -e MYSQL_PWD qf-test-mysql mysql -uroot -h127.0.0.1
# 密码只从环境变量 QF_MIGRATE_PASSWORD、QF_APP_PASSWORD 读（十六进制随机串），SQL 经 stdin 交给 mysql。
# 重复运行会把两个账号的密码重置为当前环境变量的值。
set -eu

: "${QF_MIGRATE_PASSWORD:?}" "${QF_APP_PASSWORD:?}"
[ $# -gt 0 ] || { echo "usage: $0 <mysql command...>" >&2; exit 2; }

"$@" <<SQL
CREATE DATABASE IF NOT EXISTS quotaforge_test;
CREATE USER IF NOT EXISTS 'qf_migrate'@'%' IDENTIFIED BY '${QF_MIGRATE_PASSWORD}';
ALTER USER 'qf_migrate'@'%' IDENTIFIED BY '${QF_MIGRATE_PASSWORD}';
GRANT ALL PRIVILEGES ON quotaforge_test.* TO 'qf_migrate'@'%';
CREATE USER IF NOT EXISTS 'qf_app'@'%' IDENTIFIED BY '${QF_APP_PASSWORD}';
ALTER USER 'qf_app'@'%' IDENTIFIED BY '${QF_APP_PASSWORD}';
GRANT SELECT, INSERT, UPDATE, DELETE ON quotaforge_test.* TO 'qf_app'@'%';
SQL
