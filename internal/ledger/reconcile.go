package ledger

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
)

// callIDPattern 是 QF 调用号的格式：qf- 加 26 个 base32 字符（网关用 crypto/rand.Text 生成）。
var callIDPattern = regexp.MustCompile(`^qf-[A-Z2-7]{26}$`)

// ValidCallID 报告 s 是不是 QF 调用号的格式。外部给的调用号（查询接口的路径、证据文件）先过这一关。
func ValidCallID(s string) bool { return callIDPattern.MatchString(s) }

// Proof 是证据里关于一个调用的一条：上游有没有消费、消费了多少，从哪种证据来。
// Status 是 Settled（Usage 必有）或 Cancelled（明确没有消费）。Line 是它在证据文件里的行号，只用于报错。
type Proof struct {
	CallID string
	Line   int
	Status Status
	Usage  *Usage
	Source Evidence
}

func (p Proof) String() string {
	if p.Status == Cancelled {
		return string(p.Source) + ": not consumed"
	}
	return fmt.Sprintf("%s: %s", p.Source, p.Usage)
}

func (u *Usage) String() string {
	if u == nil {
		return "usage unknown"
	}
	if !u.HasCompletion {
		return fmt.Sprintf("prompt=%d total=%d", u.Prompt, u.Total)
	}
	return fmt.Sprintf("prompt=%d completion=%d total=%d", u.Prompt, u.Completion, u.Total)
}

// ErrBadEvidence 表示证据文件的格式不对；整份文件都不用，库里什么都不改。
var ErrBadEvidence = errors.New("bad evidence")

// BillHeader 是 QF 自己的账单格式的表头：每行一个调用号；consumed 是 yes（后面三列是上游按调用给的用量，
// 嵌入可以没有 completion_tokens）或 no（明确没有消费，三列都空）。
var BillHeader = []string{"call_id", "consumed", "prompt_tokens", "completion_tokens", "total_tokens"}

// ReadBill 读一份账单。表头、调用号、用量任何一处不对，或者同一个调用号出现两次，都返回 ErrBadEvidence。
func ReadBill(r io.Reader) ([]Proof, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = len(BillHeader)
	header, err := cr.Read()
	if err != nil || !slices.Equal(header, BillHeader) {
		return nil, fmt.Errorf("%w: the first line must be %v", ErrBadEvidence, BillHeader)
	}
	var out []Proof
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil { // csv 的解析错误自己带行号、列号
			return nil, fmt.Errorf("%w: %v", ErrBadEvidence, err)
		}
		line, _ := cr.FieldPos(0) // 只在读成功之后取：解析出错时还没有字段位置，FieldPos 会 panic
		p, err := billRow(rec)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %v", ErrBadEvidence, line, err)
		}
		p.Line = line
		out = append(out, p)
	}
	return out, CheckDistinct(out)
}

func billRow(rec []string) (Proof, error) {
	p := Proof{CallID: rec[0], Source: Bill}
	if !ValidCallID(p.CallID) {
		return p, errors.New("call_id is not a QF call id")
	}
	nums := rec[2:]
	switch rec[1] {
	case "no":
		if nums[0] != "" || nums[1] != "" || nums[2] != "" {
			return p, errors.New("a call that consumed nothing has no token counts")
		}
		p.Status = Cancelled
		return p, nil
	case "yes":
	default:
		return p, errors.New(`consumed must be "yes" or "no"`)
	}
	var u Usage
	var err error
	if u.Prompt, err = tokenCount(nums[0]); err != nil {
		return p, fmt.Errorf("prompt_tokens: %v", err)
	}
	if u.Total, err = tokenCount(nums[2]); err != nil {
		return p, fmt.Errorf("total_tokens: %v", err)
	}
	if nums[1] != "" {
		if u.Completion, err = tokenCount(nums[1]); err != nil {
			return p, fmt.Errorf("completion_tokens: %v", err)
		}
		u.HasCompletion = true
	}
	p.Status, p.Usage = Settled, &u
	return p, nil
}

func tokenCount(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, errors.New("must be a non-negative integer")
	}
	return n, nil
}

// CheckDistinct 要求每个调用号在证据里只出现一次：同一个调用两条证据，不知道该信哪条。
func CheckDistinct(proofs []Proof) error {
	seen := map[string]int{}
	for _, p := range proofs {
		if first, ok := seen[p.CallID]; ok {
			return fmt.Errorf("%w: call %s appears on lines %d and %d", ErrBadEvidence, p.CallID, first, p.Line)
		}
		seen[p.CallID] = p.Line
	}
	return nil
}

// Verdict 是对一条证据的判断。
type Verdict string

const (
	Settle       Verdict = "settle"       // 未知用量的调用，按证据结算
	Release      Verdict = "release"      // 未知用量的调用，按证据释放
	Consistent   Verdict = "consistent"   // 已经结清，和证据一致
	Inconsistent Verdict = "inconsistent" // 和证据不一致，什么都不改
	InProgress   Verdict = "in-progress"  // 还在 RESERVED、IN_FLIGHT：等回收先处理
	NotFound     Verdict = "not-found"    // 账本里没有这个调用号
	Failed       Verdict = "failed"       // 处理这个调用时账本出错
)

// Item 是对账的一行结果：证据、调用的账户和处理前的状态、判断、是否已经改了库、不一致或失败的原因。
type Item struct {
	Proof
	Account string
	Before  Status
	Verdict Verdict
	Applied bool
	Reason  string
}

// Reconcile 逐条处理证据。预演（apply 为 false）只读，什么都不改；执行时只结清 USAGE_UNKNOWN 的调用，
// 每个调用一个事务，锁顺序和收尾一样，锁住之后按同一个 judge 再判断一次。已经结清的只比对，不改：
// 同样的证据执行第二遍时都是 Consistent，什么都不写。一个调用出错不影响其它的。
func (s *Store) Reconcile(ctx context.Context, proofs []Proof, apply bool) []Item {
	out := make([]Item, 0, len(proofs))
	for _, p := range proofs {
		it := Item{Proof: p}
		c, account, err := s.readCall(ctx, p.CallID)
		switch {
		case errors.Is(err, ErrCallNotFound):
			it.Verdict = NotFound
		case err != nil:
			it.Verdict, it.Reason = Failed, err.Error()
		default:
			it.Account, it.Before = account, c.status
			it.Verdict, it.Reason = judge(c, p)
			if apply && (it.Verdict == Settle || it.Verdict == Release) {
				s.resolve(ctx, c.accountID, &it)
			}
		}
		out = append(out, it)
	}
	return out
}

// judge 是对账的判断，预演和执行共用：c 是账本里的调用，p 是证据。
func judge(c *lockedCall, p Proof) (Verdict, string) {
	switch c.status {
	case Reserved, InFlight:
		return InProgress, "wait for the reclaim to mark it first"
	case UsageUnknown:
		if p.Status == Cancelled {
			// CANCELLED 的意思是确定上游没有消费；QF 却从上游拿到过 2xx，这两者矛盾，释放还会让幂等键空出来重新执行。
			if c.http.Valid && c.http.Int64 >= 200 && c.http.Int64 < 300 {
				return Inconsistent, fmt.Sprintf("QF returned HTTP %d for this call, but the evidence says nothing was consumed", c.http.Int64)
			}
			return Release, ""
		}
		if !p.Usage.Valid(c.purpose) {
			return Inconsistent, fmt.Sprintf("the usage is not valid for a %s call", c.purpose)
		}
		return Settle, ""
	case Settled:
		if p.Status == Settled && c.usage != nil && *c.usage == *p.Usage {
			return Consistent, ""
		}
		return Inconsistent, fmt.Sprintf("the ledger settled %s", c.usage)
	default: // Cancelled
		if p.Status == Cancelled {
			return Consistent, ""
		}
		return Inconsistent, "the ledger released this call"
	}
}

// readCall 不加锁地读出调用（对账的第一遍判断），连同账户名。
func (s *Store) readCall(ctx context.Context, callID string) (*lockedCall, string, error) {
	c := lockedCall{id: callID}
	var account string
	var prompt, completion, total sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT c.account_id, a.name, c.status, c.purpose, c.reserved_tokens, c.http_status,
			c.usage_prompt_tokens, c.usage_completion_tokens, c.usage_total_tokens
		FROM calls c JOIN accounts a ON a.id = c.account_id WHERE c.id = ?`, callID).Scan(
		&c.accountID, &account, &c.status, &c.purpose, &c.reserved, &c.http, &prompt, &completion, &total)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, "", ErrCallNotFound
	case err != nil:
		return nil, "", fmt.Errorf("read call: %w", err)
	}
	c.usage = scanUsage(prompt, completion, total)
	return &c, account, nil
}

// resolve 在一个事务里按证据结清 it 对应的调用：锁住之后再判断一次，状态在两次读之间变了的，按新的判断报告。
func (s *Store) resolve(ctx context.Context, accountID int64, it *Item) {
	err := s.inTx(ctx, nil, func(tx *sql.Tx) error {
		c, err := lockCall(ctx, tx, accountID, it.CallID)
		if err != nil {
			return err
		}
		it.Before = c.status
		if it.Verdict, it.Reason = judge(c, it.Proof); it.Verdict != Settle && it.Verdict != Release {
			return nil
		}
		if err := checkTransition(Reconcile, c, it.Status); err != nil {
			return err
		}
		set, args := "", []any(nil)
		if u := it.Usage; u != nil {
			set = "usage_prompt_tokens = ?, usage_completion_tokens = ?, usage_total_tokens = ?"
			args = []any{u.Prompt, sql.NullInt64{Int64: u.Completion, Valid: u.HasCompletion}, u.Total}
		}
		if err := setStatus(ctx, tx, it.CallID, c.status, it.Status, Reconcile, it.Source, set, args...); err != nil {
			return err
		}
		if err := closeBudget(ctx, tx, accountID, it.CallID, it.Status, c.reserved, it.Usage); err != nil {
			return err
		}
		it.Applied = true
		return nil
	})
	if err != nil {
		it.Verdict, it.Reason, it.Applied = Failed, err.Error(), false
	}
}
