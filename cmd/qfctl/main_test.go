package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

func TestMain(m *testing.M) { dbtest.Main(m) }

// qfctl 跑一条命令，返回退出码和两个输出。env 是全部环境变量；标准输入是空的。
func qfctl(env map[string]string, args ...string) (code int, stdout, stderr string) {
	return qfctlStdin(env, "", args...)
}

// qfctlStdin 和 qfctl 一样，标准输入是 stdin。
func qfctlStdin(env map[string]string, stdin string, args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	code = run(args, func(k string) string { return env[k] }, strings.NewReader(stdin), &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestUsageErrors(t *testing.T) {
	env := map[string]string{"QFCTL_MYSQL_DSN": "u:p@tcp(127.0.0.1:1)/db"}
	for _, args := range [][]string{
		nil,
		{"nope"},
		{"account"},
		{"account", "delete"},
		{"account", "create", "-name", "x"},
		{"account", "create", "-limit", "5"},
		{"account", "create", "-name", "x", "-limit", "-1"},
		{"account", "set-limit", "-name", "x"},
		{"key", "issue", "-account", "x"},
		{"key", "list"},
		{"key", "revoke", "-id", "abc"},
		{"key", "revoke"},
		{"verify", "extra"},
		{"usage", "-bogus"},
		{"idempotency"},
		{"idempotency", "list"},
		{"idempotency", "purge", "extra"},
		{"unknown", "extra"},
		{"reconcile"},
		{"reconcile", "-apply"},
		{"reconcile", "-bill", "a.csv", "-log", "b.log"},
		{"usage-reconcile", "-day", "2026-13-01"},
		{"usage-reconcile", "-day", "yesterday"},
		{"usage-reconcile", "extra"},
		// 限流的限额：0 到 10^12 的整数；set-rate 两个都必填
		{"account", "create", "-name", "x", "-limit", "5", "-rpm", "-1"},
		{"account", "create", "-name", "x", "-limit", "5", "-tpm", "1000000000001"},
		{"account", "create", "-name", "x", "-limit", "5", "-rpm", "1.5"},
		{"key", "issue", "-account", "x", "-models", "a", "-tpm", "-1"},
		{"key", "issue", "-account", "x", "-models", "a", "-rpm", "ten"},
		{"account", "set-rate", "-name", "x", "-rpm", "1"},
		{"account", "set-rate", "-name", "x", "-tpm", "1"},
		{"account", "set-rate", "-rpm", "1", "-tpm", "1"},
		{"account", "set-rate", "-name", "x", "-rpm", "1", "-tpm", "1000000000001"},
		{"key", "set-rate", "-id", "1", "-rpm", "1"},
		{"key", "set-rate", "-id", "1", "-tpm", "1"},
		{"key", "set-rate", "-rpm", "1", "-tpm", "1"},
		{"key", "set-rate", "-id", "0", "-rpm", "1", "-tpm", "1"},
		{"key", "set-rate", "-id", "1", "-rpm", "-1", "-tpm", "1"},
	} {
		code, _, stderr := qfctl(env, args...)
		if code != 2 || !strings.Contains(stderr, "usage: qfctl") {
			t.Errorf("%q: exit %d, stderr %q", args, code, stderr)
		}
	}
}

// 连接串正好来自一处；任何报错都不带连接串里的密码。
func TestDSNSource(t *testing.T) {
	const secret = "pw-SECRET-MARKER"
	dsn := "qf_app:" + secret + "@tcp(127.0.0.1:1)/db?timeout=1s"
	file := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(file, []byte(dsn+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"neither", map[string]string{}, "one of QFCTL_MYSQL_DSN and QFCTL_MYSQL_DSN_FILE is required"},
		{"both", map[string]string{"QFCTL_MYSQL_DSN": dsn, "QFCTL_MYSQL_DSN_FILE": file}, "set only one of"},
		{"env, closed port", map[string]string{"QFCTL_MYSQL_DSN": dsn}, "qfctl:"},
		{"file, closed port", map[string]string{"QFCTL_MYSQL_DSN_FILE": file}, "qfctl:"},
		{"invalid", map[string]string{"QFCTL_MYSQL_DSN": "qf_app:" + secret + "@nope"}, "database DSN is invalid"},
	} {
		code, stdout, stderr := qfctl(tc.env, "usage")
		if code != 1 || !strings.Contains(stderr, tc.want) {
			t.Errorf("%s: exit %d, stderr %q", tc.name, code, stderr)
		}
		if strings.Contains(stdout+stderr, secret) {
			t.Errorf("%s: output contains the password", tc.name)
		}
	}
}

// 每个子命令都用一遍：迁移、建账户、改上限、发 Key、列 Key、查用量、吊销、校验、清理幂等记录。
func TestCommands(t *testing.T) {
	env := map[string]string{"QFCTL_MYSQL_DSN": dbtest.MigrateDSN(t)}
	must := func(args ...string) string {
		t.Helper()
		code, stdout, stderr := qfctl(env, args...)
		if code != 0 {
			t.Fatalf("%q: exit %d, stderr %q", args, code, stderr)
		}
		return stdout
	}
	if out := must("migrate"); !strings.Contains(out, "up to date") {
		t.Fatalf("migrate: %q", out)
	}
	store := dbtest.Store(t)
	name := dbtest.Account(t, store, 1) // 拿一个没用过的名字
	name += "-q"
	must("account", "create", "-name", name, "-limit", "1000")
	if code, _, stderr := qfctl(env, "account", "create", "-name", name, "-limit", "1"); code != 1 || !strings.Contains(stderr, "already exists") {
		t.Fatalf("duplicate account: exit %d, %q", code, stderr)
	}
	must("account", "set-limit", "-name", name, "-limit", "2000")

	out := must("key", "issue", "-account", name, "-models", "a,b")
	m := regexp.MustCompile(`(?m)^id: (\d+)\nprefix: (qfk_\S{8})\nkey: (qfk_\S{43})\n$`).FindStringSubmatch(out)
	if m == nil || !strings.HasPrefix(m[3], m[2]) {
		t.Fatal("key issue output has the wrong shape")
	}
	k, err := store.KeyByHash(context.Background(), apikey.Hash(m[3]))
	if err != nil || k == nil || !k.Allows("a") || !k.Allows("b") {
		t.Fatalf("issued key does not authenticate: %v", err)
	}
	list := must("key", "list", "-account", name)
	// 限流限额是最后两列，前面各列的位置不变（脚本按列号读）。没写 -rpm、-tpm 就是 0（不限）。
	if !strings.HasPrefix(list, "id\tprefix\tstatus\tmodels\trpm_limit\ttpm_limit\n") ||
		!strings.Contains(list, m[1]+"\t"+m[2]+"\tactive\ta,b\t0\t0\n") || strings.Contains(list, m[3]) {
		t.Fatalf("key list = %q", list)
	}
	usage := must("usage", "-account", name)
	// 新列（IN_FLIGHT，之后是账户的限流限额）都在最后，前面各列的位置不变（脚本按列号读）。
	if !strings.HasPrefix(usage, "account\tlimit\treserved\tconsumed\tremaining\tRESERVED\tSETTLED\tCANCELLED\tUSAGE_UNKNOWN\tIN_FLIGHT\trpm_limit\ttpm_limit\n") ||
		!strings.Contains(usage, name+"\t2000\t0\t0\t2000\t0\t0\t0\t0\t0\t0\t0\n") {
		t.Fatalf("usage = %q", usage)
	}

	// 限流的限额：建的时候带上、之后改；key list、usage 的最后两列，KeyByHash 都读到同样的值。
	must("account", "set-rate", "-name", name, "-rpm", "60", "-tpm", "1000000000000")
	if u := must("usage", "-account", name); !strings.Contains(u, name+"\t2000\t0\t0\t2000\t0\t0\t0\t0\t0\t60\t1000000000000\n") {
		t.Fatalf("usage after set-rate = %q", u)
	}
	rated := must("key", "issue", "-account", name, "-models", "a", "-rpm", "5", "-tpm", "700")
	rm := regexp.MustCompile(`(?m)^id: (\d+)\nprefix: (qfk_\S{8})\nkey: (qfk_\S{43})\n$`).FindStringSubmatch(rated)
	if rm == nil {
		t.Fatal("key issue -rpm -tpm output has the wrong shape")
	}
	if l := must("key", "list", "-account", name); !strings.Contains(l, rm[1]+"\t"+rm[2]+"\tactive\ta\t5\t700\n") {
		t.Fatalf("key list = %q", l)
	}
	must("key", "set-rate", "-id", rm[1], "-rpm", "0", "-tpm", "9")
	k, err = store.KeyByHash(context.Background(), apikey.Hash(rm[3]))
	if err != nil || k == nil || k.Rate != (ledger.Rate{TPM: 9}) || k.AccountRate != (ledger.Rate{RPM: 60, TPM: 1_000_000_000_000}) {
		t.Fatalf("KeyByHash after set-rate = %+v, %v", k, err)
	}
	if code, _, stderr := qfctl(env, "account", "set-rate", "-name", name+"-none", "-rpm", "1", "-tpm", "1"); code != 1 || !strings.Contains(stderr, "account not found") {
		t.Fatalf("set-rate on a missing account: exit %d, %q", code, stderr)
	}
	created := name + "-r"
	must("account", "create", "-name", created, "-limit", "5", "-rpm", "7", "-tpm", "8")
	if u := must("usage", "-account", created); !strings.HasSuffix(u, "\t7\t8\n") {
		t.Fatalf("usage of an account created with limits = %q", u)
	}
	must("key", "revoke", "-id", rm[1])
	if code, _, stderr := qfctl(env, "key", "set-rate", "-id", rm[1], "-rpm", "1", "-tpm", "1"); code != 1 || !strings.Contains(stderr, "already revoked") {
		t.Fatalf("set-rate on a revoked key: exit %d, %q", code, stderr)
	}
	if all := must("usage"); !strings.Contains(all, name+"\t2000\t") {
		t.Fatalf("usage for all accounts misses %s", name)
	}
	must("key", "revoke", "-id", m[1])
	if code, _, stderr := qfctl(env, "key", "revoke", "-id", m[1]); code != 1 || !strings.Contains(stderr, "already revoked") {
		t.Fatalf("revoke twice: exit %d, %q", code, stderr)
	}
	if k, _ := store.KeyByHash(context.Background(), apikey.Hash(m[3])); k != nil {
		t.Fatal("revoked key still authenticates")
	}
	if out := must("verify"); out != "ledger ok\n" {
		t.Fatalf("verify = %q", out)
	}
	if out := must("idempotency", "purge"); !regexp.MustCompile(`^purged \d+ expired idempotency records\n$`).MatchString(out) {
		t.Fatalf("idempotency purge = %q", out)
	}

	// 改坏这个账户的额度行：verify 以 1 退出并列出规则和账户；最后改回来
	admin := dbtest.Admin(t)
	tamper := func(delta int) {
		t.Helper()
		if _, err := admin.Exec(`UPDATE budgets b JOIN accounts a ON a.id = b.account_id
			SET b.consumed_tokens = b.consumed_tokens + ? WHERE a.name = ?`, delta, name); err != nil {
			t.Fatal(err)
		}
	}
	tamper(7)
	t.Cleanup(func() { tamper(-7) })
	code, stdout, stderr := qfctl(env, "verify")
	if code != 1 || !strings.Contains(stdout, "FAIL budget_consumed account="+name+": budget 7, ledger 0") || !strings.Contains(stderr, "1 violations") {
		t.Fatalf("verify after tampering: exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

// idempotency purge 部分成功：一个账户的额度行被锁住，别的账户的过期键照常删掉。
// 先打印删掉的条数，再以 1 退出；放开之后再来一次，删掉剩下的。
func TestIdempotencyPurgePartial(t *testing.T) {
	store, admin := dbtest.Store(t), dbtest.Admin(t)
	env := map[string]string{"QFCTL_MYSQL_DSN": dbtest.MigrateDSN(t)}
	expiredKey := func(limit int64) int64 { // 返回账户号；账户里有一个已过期的键
		t.Helper()
		issued, err := store.IssueKey(context.Background(), dbtest.Account(t, store, limit), []string{"m"}, ledger.Rate{})
		if err != nil {
			t.Fatal(err)
		}
		k, err := store.KeyByHash(context.Background(), apikey.Hash(issued.Secret))
		if err != nil || k == nil {
			t.Fatalf("KeyByHash = %v, %v", k, err)
		}
		r := ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: k.AccountID, KeyID: k.ID, Purpose: "chat", Alias: "m",
			Upstream: "u", UpstreamModel: "um", EstInput: 1, EstOutput: 1, IdempotencyKey: "k", Fingerprint: make([]byte, 32)}
		if _, err := store.Reserve(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(context.Background(), r, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.NotSent}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec("UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE account_id = ?", k.AccountID); err != nil {
			t.Fatal(err)
		}
		return k.AccountID
	}
	purged := regexp.MustCompile(`^purged [1-9]\d* expired idempotency records\n$`) // 共用库里别的账户也可能有刚过期的键：不数整库，看各账户剩下的
	stuck, other := expiredKey(1000), expiredKey(1000)

	holder, err := admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback() })
	if _, err := holder.Exec("SELECT 1 FROM budgets WHERE account_id = ? FOR UPDATE", stuck); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := qfctl(env, "idempotency", "purge")
	if code != 1 || !purged.MatchString(stdout) || !strings.Contains(stderr, "1 transactions failed") {
		t.Fatalf("purge with a locked account: exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
	left := func(account int64) (n int) {
		t.Helper()
		if err := admin.QueryRow("SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", account).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if left(other) != 0 || left(stuck) != 1 {
		t.Fatalf("left: other %d, stuck %d; want 0, 1", left(other), left(stuck))
	}

	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if code, stdout, stderr := qfctl(env, "idempotency", "purge"); code != 0 || !purged.MatchString(stdout) || stderr != "" {
		t.Fatalf("purge after the lock is released: exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
	if left(stuck) != 0 {
		t.Fatalf("%d keys left in the formerly locked account", left(stuck))
	}
}

// reconcile 和 unknown：从列出未知用量的调用，到用账单预演、执行、再执行一遍；退出 0 只表示没有要人看的东西。
func TestReconcileCommand(t *testing.T) {
	env := map[string]string{"QFCTL_MYSQL_DSN": dbtest.AppDSN(t)}
	ctx := context.Background()
	store := dbtest.Store(t)
	name := dbtest.Account(t, store, 1_000_000)
	issued, err := store.IssueKey(ctx, name, []string{"m"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.KeyByHash(ctx, apikey.Hash(issued.Secret))
	if err != nil {
		t.Fatal(err)
	}
	newCall := func(status ledger.Status) string {
		t.Helper()
		r := ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: key.AccountID, KeyID: key.ID, Purpose: "chat",
			Alias: "m", Upstream: "up", UpstreamModel: "um", EstInput: 10, EstOutput: 10}
		if _, err := store.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkSent(ctx, r); err != nil {
			t.Fatal(err)
		}
		if status != ledger.InFlight {
			if err := store.Finish(ctx, r, ledger.Result{Status: status, HTTPStatus: 504, ErrorCode: "upstream_timeout"}); err != nil {
				t.Fatal(err)
			}
		}
		return r.CallID
	}
	unknown, inFlight, missing := newCall(ledger.UsageUnknown), newCall(ledger.InFlight), "qf-"+rand.Text()
	usageNow := func() string { _, out, _ := qfctl(env, "usage", "-account", name); return out }

	_, list, _ := qfctl(env, "unknown", "-account", name)
	if !strings.HasPrefix(list, "call_id\taccount\tcreated_at\tendpoint\tmodel_alias\tupstream\tupstream_model\treserved_tokens\terror_code\n") ||
		!regexp.MustCompile(`\n`+unknown+`\t`+name+`\t\d{4}-\d\d-\d\dT[\d:.]+Z\tchat\tm\tup\tum\t20\tupstream_timeout\n$`).MatchString(list) {
		t.Fatalf("unknown = %q", list)
	}

	bill := "call_id,consumed,prompt_tokens,completion_tokens,total_tokens\n" + unknown + ",yes,7,3,10\n"
	before := usageNow()
	code, out, stderr := qfctlStdin(env, bill, "reconcile", "-bill", "-")
	if code != 0 || out != "would-settle\t"+unknown+"\t"+name+"\tUSAGE_UNKNOWN\tbill: prompt=7 completion=3 total=10\n"+
		"dry run: 1 to settle, 0 to release, 0 consistent, 0 inconsistent, 0 in progress, 0 not found, 0 failed; nothing was changed (use -apply)\n" {
		t.Fatalf("dry run: exit %d, %q, %q", code, out, stderr)
	}
	if usageNow() != before {
		t.Fatal("the dry run changed the ledger")
	}
	file := filepath.Join(t.TempDir(), "bill.csv")
	if err := os.WriteFile(file, []byte(bill), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := qfctl(env, "reconcile", "-bill", file, "-apply"); code != 0 || !strings.HasPrefix(out, "settled\t"+unknown) ||
		!strings.HasSuffix(out, "applied: 1 settled, 0 released, 0 consistent, 0 inconsistent, 0 in progress, 0 not found, 0 failed\n") {
		t.Fatalf("apply: exit %d, %q", code, out)
	}
	if code, out, _ := qfctl(env, "reconcile", "-bill", file, "-apply"); code != 0 || !strings.HasPrefix(out, "consistent\t"+unknown) ||
		!strings.HasSuffix(out, "nothing to change: 1 consistent, 0 inconsistent, 0 in progress, 0 not found, 0 failed\n") {
		t.Fatalf("second apply: exit %d, %q", code, out)
	}
	if _, list, _ := qfctl(env, "unknown", "-account", name); strings.Contains(list, unknown) {
		t.Fatalf("still unknown: %q", list)
	}
	// 已经结算的调用：账单的 total 相同、prompt 和 completion 的拆分不同，也是不一致，退出 1，不改
	split := "call_id,consumed,prompt_tokens,completion_tokens,total_tokens\n" + unknown + ",yes,6,4,10\n"
	before = usageNow()
	for _, args := range [][]string{{"reconcile", "-bill", "-"}, {"reconcile", "-bill", "-", "-apply"}} {
		if code, out, stderr := qfctlStdin(env, split, args...); code != 1 ||
			!strings.HasPrefix(out, "INCONSISTENT\t"+unknown+"\t"+name+"\tSETTLED\tbill: prompt=6 completion=4 total=10; the ledger settled prompt=7 completion=3 total=10\n") {
			t.Fatalf("%q: exit %d, %q, %q", args, code, out, stderr)
		}
	}
	if usageNow() != before {
		t.Fatal("inconsistent evidence changed the ledger")
	}

	// 还在进行中、账本里没有、不一致的：如实报告，退出 1；预演和执行一样
	worse := "call_id,consumed,prompt_tokens,completion_tokens,total_tokens\n" + inFlight + ",yes,1,1,2\n" + missing + ",no,,,\n" + unknown + ",no,,,\n"
	for _, args := range [][]string{{"reconcile", "-bill", "-"}, {"reconcile", "-bill", "-", "-apply"}} {
		code, out, stderr := qfctlStdin(env, worse, args...)
		if code != 1 || !strings.Contains(out, "in-progress\t"+inFlight+"\t"+name+"\tIN_FLIGHT\t") || !strings.Contains(out, "not-found\t"+missing+"\t\t\t") ||
			!strings.Contains(out, "INCONSISTENT\t"+unknown+"\t"+name+"\tSETTLED\tbill: not consumed; the ledger settled prompt=7 completion=3 total=10\n") ||
			!strings.Contains(stderr, "3 calls need a look") {
			t.Fatalf("%q: exit %d, %q, %q", args, code, out, stderr)
		}
	}

	// 坏格式退出 2，不连库也不改库
	before = usageNow()
	for _, bad := range []string{"", "call_id,consumed\n", "call_id,consumed,prompt_tokens,completion_tokens,total_tokens\n" + unknown + ",yes,,,\n"} {
		if code, out, stderr := qfctlStdin(env, bad, "reconcile", "-bill", "-", "-apply"); code != 2 || out != "" || !strings.Contains(stderr, "bad evidence") {
			t.Errorf("bad bill %q: exit %d, %q, %q", bad, code, out, stderr)
		}
	}
	if code, _, stderr := qfctl(env, "reconcile", "-log", filepath.Join(t.TempDir(), "missing.log")); code != 2 || !strings.Contains(stderr, "bad evidence") {
		t.Errorf("missing log file: exit %d, %q", code, stderr)
	}
	if usageNow() != before {
		t.Fatal("a bad evidence file changed the ledger")
	}

	// 日志当证据：结局未知的行不算，单独报出条数
	log := `{"msg":"call","request_id":"` + inFlight + `","outcome":"USAGE_UNKNOWN","usage":"unknown"}` + "\n"
	if code, out, _ := qfctlStdin(env, log, "reconcile", "-log", "-"); code != 0 ||
		out != "1 log lines with an unknown outcome are not evidence; ignored\nnothing to change: 0 consistent, 0 inconsistent, 0 in progress, 0 not found, 0 failed\n" {
		t.Fatalf("log: exit %d, %q", code, out)
	}
	dbtest.Verify(t, store)
}

// usage-reconcile：在路上的照实列出、退出 0；全部消费之后 all equal；消费者的汇总被改了就是 MISMATCH、退出 1。
func TestUsageReconcileCommand(t *testing.T) {
	env := map[string]string{"QFCTL_MYSQL_DSN": dbtest.AppDSN(t)}
	ctx := context.Background()
	store := dbtest.Store(t)
	name := dbtest.Account(t, store, 1_000_000)
	issued, err := store.IssueKey(ctx, name, []string{"m"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.KeyByHash(ctx, apikey.Hash(issued.Secret))
	if err != nil {
		t.Fatal(err)
	}
	r := ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: key.AccountID, KeyID: key.ID, Purpose: "chat",
		Alias: "m", Upstream: "up", UpstreamModel: "um", EstInput: 10, EstOutput: 10}
	if _, err := store.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSent(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200,
		Usage: &ledger.Usage{Prompt: 7, Completion: 3, Total: 10, HasCompletion: true}}); err != nil {
		t.Fatal(err)
	}
	day := dbtest.Now(t, dbtest.Admin(t)).Format(time.DateOnly)
	header := "day\taccount\tpurpose\tledger_calls\tconsumer_calls\tledger_tokens\tconsumer_tokens\tunsent\tunconsumed\tresult\n"

	code, out, _ := qfctl(env, "usage-reconcile", "-account", name)
	if code != 0 || !strings.HasPrefix(out, header+day+"\t"+name+"\tchat\t1\t0\t10\t0\t1\t0\tin-transit\nunsent\t"+r.CallID+"\t") ||
		!strings.Contains(out, "\nin transit: 1 unsent, 0 not consumed yet") {
		t.Fatalf("unsent: exit %d, %q", code, out)
	}
	var events []ledger.UsageEvent
	if _, err := store.ForwardOutbox(ctx, func(_ context.Context, batch []ledger.UsageEvent) error {
		events = append(events, batch...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.AccountID == key.AccountID {
			if _, err := store.ApplyUsageEvent(ctx, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	code, out, _ = qfctl(env, "usage-reconcile", "-account", name, "-day", day)
	if code != 0 || out != header+day+"\t"+name+"\tchat\t1\t1\t10\t10\t0\t0\tsame\nall equal: 1 rows\n" {
		t.Fatalf("consumed: exit %d, %q", code, out)
	}
	if _, err := dbtest.Admin(t).Exec("UPDATE usage_daily SET total_tokens = total_tokens + 1 WHERE account_id = ?", key.AccountID); err != nil {
		t.Fatal(err)
	}
	code, out, _ = qfctl(env, "usage-reconcile", "-account", name)
	if code != 1 || !strings.Contains(out, "\t10\t11\t0\t0\tMISMATCH\n") || !strings.HasSuffix(out, "MISMATCH: 1 of 1 rows (0 unsent, 0 not consumed yet)\n") {
		t.Fatalf("tampered totals: exit %d, %q", code, out)
	}
}
