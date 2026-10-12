#!/usr/bin/env bash
# mysql 镜像首次初始化时运行（挂在 /docker-entrypoint-initdb.d/ 下）：建两个账号，
# qf_migrate（库内全部权限，qfctl 迁移用）和 qf_app（只有增删改查，QF 服务用）。
#
# 入口脚本对这里的 .sh：有执行权限就单独执行，没有就 source。Windows 的 Docker Desktop 把挂载的文件
# 显示成可执行，Linux 保留仓库里的 100755，所以两边都是单独执行；被 source 也照样工作。
# 因此不用入口脚本的函数（单独执行时没有），直接用 mysql 客户端经 socket 连初始化阶段的临时服务器
# （它不监听 TCP；入口脚本的 SOCKET 变量没有导出，用字面路径）。MYSQL_ROOT_PASSWORD、MYSQL_DATABASE
# 由入口脚本从 *_FILE 读出后导出。主体在子 shell 里：set 选项不影响 source 它的入口脚本，出错时非 0 退出、初始化中止。
# 子 shell 的结果单独一行检查，不写成 ( … ) || exit：放在 || 左边时，bash 会让子 shell 里的 set -e 失效。
#
# 用户名和密码从 QF、qfctl 用的同两个连接串 secret 里取，密码只有一份。连接串形如 user:password@tcp(host:port)/db。
# 密码只允许字母、数字、- 和 _（脚本生成的是十六进制），拼进 SQL 不用转义。

(
	set -euo pipefail

	# dsn_account <文件>：打印“用户名 密码”；格式不对就报错（不回显文件内容）。
	dsn_account() {
		local user password
		user=$(sed -n 's/^\([A-Za-z0-9_]\{1,32\}\):[A-Za-z0-9_-]\{1,128\}@tcp(.*$/\1/p' "$1")
		password=$(sed -n 's/^[A-Za-z0-9_]\{1,32\}:\([A-Za-z0-9_-]\{1,128\}\)@tcp(.*$/\1/p' "$1")
		if [ -z "$user" ] || [ -z "$password" ]; then
			echo "qf-accounts: $1 is not user:password@tcp(host:port)/db with a password of letters, digits, - and _" >&2
			return 1
		fi
		printf '%s %s' "$user" "$password"
	}

	migrate=$(dsn_account /run/secrets/migrate_dsn)
	app=$(dsn_account /run/secrets/app_dsn)
	read -r migrate_user migrate_password <<<"$migrate"
	read -r app_user app_password <<<"$app"

	MYSQL_PWD=$MYSQL_ROOT_PASSWORD mysql -uroot --protocol=socket --socket=/var/run/mysqld/mysqld.sock <<-SQL
		CREATE USER '${migrate_user}'@'%' IDENTIFIED BY '${migrate_password}';
		GRANT ALL PRIVILEGES ON \`${MYSQL_DATABASE}\`.* TO '${migrate_user}'@'%';
		CREATE USER '${app_user}'@'%' IDENTIFIED BY '${app_password}';
		GRANT SELECT, INSERT, UPDATE, DELETE ON \`${MYSQL_DATABASE}\`.* TO '${app_user}'@'%';
	SQL
	echo "qf-accounts: created accounts ${migrate_user} (schema changes) and ${app_user} (data only)"
)
status=$?
if [ "$status" != 0 ]; then exit "$status"; fi
