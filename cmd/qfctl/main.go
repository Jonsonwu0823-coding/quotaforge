// qfctl 是 QuotaForge 的管理工具，直连账本数据库，不开 HTTP 管理接口：迁移、账户和额度、发放和吊销 Key、查用量、
// 校验账本、清理过期的幂等记录、列出用量未知的调用、凭证据对账。
//
// 连接串来自环境变量 QFCTL_MYSQL_DSN，或者 QFCTL_MYSQL_DSN_FILE 指向的文件（容器里的 secret），正好设一个。
// 迁移要用能建表的账号；其它子命令用只能增删改查的账号也够。任何输出都不带连接串和密码。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/gateway"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

const usage = `usage: qfctl <command> [flags]

  migrate                                      migrate the database to this build's schema
  account create -name NAME -limit TOKENS [-rpm N] [-tpm N]
                                               create an account with a token limit and rate limits
  account set-limit -name NAME -limit TOKENS   change an account's token limit
  account set-rate -name NAME -rpm N -tpm N    change an account's rate limits
  key issue -account NAME -models A,B [-rpm N] [-tpm N]
                                               issue a key; the key is printed only this once
  key list -account NAME                       list an account's keys (id, prefix, status, models, rate limits)
  key revoke -id ID                            revoke a key
  key set-rate -id ID -rpm N -tpm N            change a key's rate limits
  usage [-account NAME]                        limit, reserved, consumed, remaining, calls per status and rate limits
  verify                                       check the ledger invariants; exit 1 if any fails
  idempotency purge                            delete expired idempotency records and their snapshots
  unknown [-account NAME]                      list calls whose usage is unknown (they still hold their reservation)
  reconcile (-bill FILE | -log FILE) [-apply]  settle unknown usage on evidence: a QF bill CSV or the QF call log
                                               (FILE - reads stdin); a dry run unless -apply; exit 1 if any call
                                               needs a look (inconsistent, in progress, not found, failed)

Rate limits are requests (-rpm) and tokens (-tpm) per minute, from 0 (no limit) to 1000000000000.
To stop a key, revoke it: a rate limit of 0 means no limit.
`

// 一次管理操作的上限；校验账本要扫全表，给得宽一些。
const timeout = 2 * time.Minute

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr))
}

// errUsage 表示命令行用错了，退出码 2。
var errUsage = errors.New("usage")

// run 执行一条命令，返回退出码：0 成功，1 失败（包括账本校验不通过、对账有要人看的调用），
// 2 用法错误或者输入文件（对账的证据）读不了、格式不对。
func run(args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := dispatch(args, getenv, stdin, stdout)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprint(stderr, usage)
		return 2
	case errors.Is(err, ledger.ErrBadEvidence):
		fmt.Fprintln(stderr, "qfctl:", err)
		return 2
	default:
		fmt.Fprintln(stderr, "qfctl:", err)
		return 1
	}
}

func dispatch(args []string, getenv func(string) string, stdin io.Reader, out io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	name := args[0]
	if len(args) > 1 && (name == "account" || name == "key" || name == "idempotency") {
		name += " " + args[1]
		args = args[1:]
	}
	cmd, ok := commands[name]
	if !ok {
		return errUsage
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	exec := cmd(fs, stdin)
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return errUsage
	}
	dsn, err := config.Secret("QFCTL_MYSQL_DSN", "QFCTL_MYSQL_DSN_FILE", setName(getenv, "QFCTL_MYSQL_DSN"), getenv("QFCTL_MYSQL_DSN_FILE"), getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return exec(ctx, dsn, out)
}

// setName 在环境变量有值时返回它的名字，否则返回空串（表示没用这种来源）。
func setName(getenv func(string) string, name string) string {
	if getenv(name) == "" {
		return ""
	}
	return name
}

// command 在 fs 上定义自己的参数，返回执行函数。stdin 只给读证据的 reconcile 用。
type command func(fs *flag.FlagSet, stdin io.Reader) func(ctx context.Context, dsn string, out io.Writer) error

var commands = map[string]command{
	"migrate":           migrateCmd,
	"account create":    accountCreate,
	"account set-limit": accountSetLimit,
	"account set-rate":  accountSetRate,
	"key issue":         keyIssue,
	"key list":          keyList,
	"key revoke":        keyRevoke,
	"key set-rate":      keySetRate,
	"usage":             usageCmd,
	"verify":            verifyCmd,
	"idempotency purge": idempotencyPurge,
	"unknown":           unknownCmd,
	"reconcile":         reconcileCmd,
}

// withStore 打开账本执行 fn，结束时关闭。
func withStore(ctx context.Context, dsn string, fn func(*ledger.Store) error) error {
	s, err := ledger.Open(dsn)
	if err != nil {
		return err
	}
	defer s.Close()
	return fn(s)
}

func migrateCmd(*flag.FlagSet, io.Reader) func(context.Context, string, io.Writer) error {
	return func(ctx context.Context, dsn string, out io.Writer) error {
		from, to, err := ledger.Migrate(ctx, dsn)
		if err != nil {
			return err
		}
		if from == to {
			fmt.Fprintf(out, "schema version %d: up to date\n", to)
		} else {
			fmt.Fprintf(out, "schema version %d -> %d\n", from, to)
		}
		return nil
	}
}

// required 检查必填的字符串参数。
func required(values ...*string) error {
	for _, v := range values {
		if *v == "" {
			return errUsage
		}
	}
	return nil
}

// rateFlags 定义 -rpm、-tpm，返回读取并校验它们的函数。def 是没写时的值：建账户、发 Key 时是 0（不限）；
// set-rate 要求两个都写，用 -1 表示没写，校验时就不合法。范围和配置文件里上游的限额是同一条规则。
func rateFlags(fs *flag.FlagSet, def int64) func() (ledger.Rate, error) {
	rpm := fs.Int64("rpm", def, "requests per minute (0: no limit)")
	tpm := fs.Int64("tpm", def, "tokens per minute (0: no limit)")
	return func() (ledger.Rate, error) {
		if !config.ValidRate(*rpm) || !config.ValidRate(*tpm) {
			return ledger.Rate{}, errUsage
		}
		return ledger.Rate{RPM: *rpm, TPM: *tpm}, nil
	}
}

func accountCreate(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	name := fs.String("name", "", "account name")
	limit := fs.Int64("limit", -1, "token limit")
	rate := rateFlags(fs, 0)
	return func(ctx context.Context, dsn string, out io.Writer) error {
		r, err := rate()
		if err != nil || required(name) != nil || *limit < 0 {
			return errUsage
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			if err := s.CreateAccount(ctx, *name, *limit, r); err != nil {
				return err
			}
			fmt.Fprintf(out, "account %s created, limit %d tokens, rpm %d, tpm %d\n", *name, *limit, r.RPM, r.TPM)
			return nil
		})
	}
}

func accountSetRate(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	name := fs.String("name", "", "account name")
	rate := rateFlags(fs, -1)
	return func(ctx context.Context, dsn string, out io.Writer) error {
		r, err := rate()
		if err != nil || required(name) != nil {
			return errUsage
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			if err := s.SetAccountRate(ctx, *name, r); err != nil {
				return err
			}
			fmt.Fprintf(out, "account %s rate set to rpm %d, tpm %d\n", *name, r.RPM, r.TPM)
			return nil
		})
	}
}

func accountSetLimit(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	name := fs.String("name", "", "account name")
	limit := fs.Int64("limit", -1, "token limit")
	return func(ctx context.Context, dsn string, out io.Writer) error {
		if err := required(name); err != nil || *limit < 0 {
			return errUsage
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			if err := s.SetLimit(ctx, *name, *limit); err != nil {
				return err
			}
			fmt.Fprintf(out, "account %s limit set to %d tokens\n", *name, *limit)
			return nil
		})
	}
}

func keyIssue(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	account := fs.String("account", "", "account name")
	models := fs.String("models", "", "comma-separated model aliases the key may use")
	rate := rateFlags(fs, 0)
	return func(ctx context.Context, dsn string, out io.Writer) error {
		r, err := rate()
		if err != nil || required(account, models) != nil {
			return errUsage
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			k, err := s.IssueKey(ctx, *account, strings.Split(*models, ","), r)
			if err != nil {
				return err
			}
			// key: 这一行是 Key 本身，只出现这一次；脚本按这个前缀取。
			fmt.Fprintf(out, "id: %d\nprefix: %s\nkey: %s\n", k.ID, k.Prefix, k.Secret)
			return nil
		})
	}
}

func keyList(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	account := fs.String("account", "", "account name")
	return func(ctx context.Context, dsn string, out io.Writer) error {
		if err := required(account); err != nil {
			return err
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			keys, err := s.ListKeys(ctx, *account)
			if err != nil {
				return err
			}
			// 限流限额放在最后：前面各列的位置不变，按列号读的脚本不受影响。
			fmt.Fprintln(out, "id\tprefix\tstatus\tmodels\trpm_limit\ttpm_limit")
			for _, k := range keys {
				fmt.Fprintf(out, "%d\t%s\t%s\t%s\t%d\t%d\n", k.ID, k.Prefix, k.Status, strings.Join(k.Models, ","), k.Rate.RPM, k.Rate.TPM)
			}
			return nil
		})
	}
}

// keyID 读 -id：正整数。
func keyID(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, errUsage
	}
	return n, nil
}

func keyRevoke(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	id := fs.String("id", "", "key id")
	return func(ctx context.Context, dsn string, out io.Writer) error {
		n, err := keyID(*id)
		if err != nil {
			return err
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			if err := s.RevokeKey(ctx, n); err != nil {
				return err
			}
			fmt.Fprintf(out, "key %d revoked\n", n)
			return nil
		})
	}
}

func keySetRate(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	id := fs.String("id", "", "key id")
	rate := rateFlags(fs, -1)
	return func(ctx context.Context, dsn string, out io.Writer) error {
		n, err := keyID(*id)
		if err != nil {
			return err
		}
		r, err := rate()
		if err != nil {
			return err
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			if err := s.SetKeyRate(ctx, n, r); err != nil {
				return err
			}
			fmt.Fprintf(out, "key %d rate set to rpm %d, tpm %d\n", n, r.RPM, r.TPM)
			return nil
		})
	}
}

func usageCmd(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	account := fs.String("account", "", "account name (default: all accounts)")
	return func(ctx context.Context, dsn string, out io.Writer) error {
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			list, err := s.Usage(ctx, *account)
			if err != nil {
				return err
			}
			// 新列（IN_FLIGHT，之后是账户的限流限额）都加在最后：前面各列的位置不变，按列号读的脚本不受影响。
			fmt.Fprintln(out, "account\tlimit\treserved\tconsumed\tremaining\tRESERVED\tSETTLED\tCANCELLED\tUSAGE_UNKNOWN\tIN_FLIGHT\trpm_limit\ttpm_limit")
			for _, u := range list {
				fmt.Fprintf(out, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", u.Account, u.Limit, u.Reserved, u.Consumed, u.Remaining,
					u.Calls[ledger.Reserved], u.Calls[ledger.Settled], u.Calls[ledger.Cancelled], u.Calls[ledger.UsageUnknown],
					u.Calls[ledger.InFlight], u.Rate.RPM, u.Rate.TPM)
			}
			return nil
		})
	}
}

func verifyCmd(*flag.FlagSet, io.Reader) func(context.Context, string, io.Writer) error {
	return func(ctx context.Context, dsn string, out io.Writer) error {
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			violations, err := s.Verify(ctx)
			if err != nil {
				return err
			}
			for _, v := range violations {
				fmt.Fprintln(out, "FAIL", v)
			}
			if len(violations) > 0 {
				return fmt.Errorf("ledger verification failed: %d violations", len(violations))
			}
			fmt.Fprintln(out, "ledger ok")
			return nil
		})
	}
}

func idempotencyPurge(*flag.FlagSet, io.Reader) func(context.Context, string, io.Writer) error {
	return func(ctx context.Context, dsn string, out io.Writer) error {
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			n, err := s.PurgeIdempotency(ctx, config.LedgerTimeout)
			fmt.Fprintf(out, "purged %d expired idempotency records\n", n) // 部分成功时也先报删掉的条数，再以错误退出
			return err
		})
	}
}

func unknownCmd(fs *flag.FlagSet, _ io.Reader) func(context.Context, string, io.Writer) error {
	account := fs.String("account", "", "account name (default: all accounts)")
	return func(ctx context.Context, dsn string, out io.Writer) error {
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			list, err := s.UnknownCalls(ctx, *account)
			if err != nil {
				return err
			}
			fmt.Fprintln(out, "call_id\taccount\tcreated_at\tendpoint\tmodel_alias\tupstream\tupstream_model\treserved_tokens\terror_code")
			for _, c := range list {
				fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", c.ID, c.Account, c.CreatedAt, c.Purpose, c.Alias,
					c.Upstream, c.UpstreamModel, c.Reserved, c.ErrorCode)
			}
			return nil
		})
	}
}

// reconcileCmd 读证据（账单或 QF 的调用日志），逐条处理，打印每一条和汇总。默认预演，-apply 才改库。
// 证据整份先读完：格式不对就退出 2，不连库。
func reconcileCmd(fs *flag.FlagSet, stdin io.Reader) func(context.Context, string, io.Writer) error {
	bill := fs.String("bill", "", "QF bill CSV (- for stdin)")
	callLog := fs.String("log", "", "QF call log, one JSON line per call (- for stdin)")
	apply := fs.Bool("apply", false, "change the ledger (default: dry run)")
	return func(ctx context.Context, dsn string, out io.Writer) error {
		if (*bill == "") == (*callLog == "") {
			return errUsage
		}
		proofs, ignored, err := readEvidence(*bill, *callLog, stdin)
		if err != nil {
			return err
		}
		return withStore(ctx, dsn, func(s *ledger.Store) error {
			items := s.Reconcile(ctx, proofs, *apply)
			if *callLog != "" {
				fmt.Fprintf(out, "%d log lines with an unknown outcome are not evidence; ignored\n", ignored)
			}
			return report(out, items, *apply)
		})
	}
}

// readEvidence 读 -bill 或 -log 指定的文件（- 是标准输入）。ignored 是日志里结局未知、不算证据的行数。
func readEvidence(bill, callLog string, stdin io.Reader) (proofs []ledger.Proof, ignored int, err error) {
	path := bill + callLog
	r := stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %v", ledger.ErrBadEvidence, err)
		}
		defer f.Close()
		r = f
	}
	if bill != "" {
		proofs, err = ledger.ReadBill(r)
		return proofs, 0, err
	}
	return gateway.ReadCallLog(r)
}

// actions 是每种判断在预演、执行时打印的动作。
var actions = map[ledger.Verdict][2]string{
	ledger.Settle:       {"would-settle", "settled"},
	ledger.Release:      {"would-release", "released"},
	ledger.Consistent:   {"consistent", "consistent"},
	ledger.Inconsistent: {"INCONSISTENT", "INCONSISTENT"},
	ledger.InProgress:   {"in-progress", "in-progress"},
	ledger.NotFound:     {"not-found", "not-found"},
	ledger.Failed:       {"FAILED", "FAILED"},
}

// report 打印每一条（动作、调用号、账户、处理前的状态、证据和原因）和汇总。只有调用号、账户、状态和数字，没有正文和 Key。
// 结清（或预演里的将要结清）和一致之外的任何一条，都让命令退出 1：退出 0 就是没有要人看的东西。
func report(out io.Writer, items []ledger.Item, apply bool) error {
	counts := map[ledger.Verdict]int{}
	mode := 0
	if apply {
		mode = 1
	}
	for _, it := range items {
		counts[it.Verdict]++
		detail := it.Proof.String()
		if it.Reason != "" {
			detail += "; " + it.Reason
		}
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\n", actions[it.Verdict][mode], it.CallID, it.Account, it.Before, detail)
	}
	changes := counts[ledger.Settle] + counts[ledger.Release]
	rest := fmt.Sprintf("%d consistent, %d inconsistent, %d in progress, %d not found, %d failed", counts[ledger.Consistent],
		counts[ledger.Inconsistent], counts[ledger.InProgress], counts[ledger.NotFound], counts[ledger.Failed])
	switch {
	case changes == 0:
		fmt.Fprintf(out, "nothing to change: %s\n", rest)
	case apply:
		fmt.Fprintf(out, "applied: %d settled, %d released, %s\n", counts[ledger.Settle], counts[ledger.Release], rest)
	default:
		fmt.Fprintf(out, "dry run: %d to settle, %d to release, %s; nothing was changed (use -apply)\n",
			counts[ledger.Settle], counts[ledger.Release], rest)
	}
	if needsLook := len(items) - changes - counts[ledger.Consistent]; needsLook > 0 {
		return fmt.Errorf("reconcile: %d calls need a look", needsLook)
	}
	return nil
}
