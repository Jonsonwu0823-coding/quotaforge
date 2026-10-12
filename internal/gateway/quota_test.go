package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// allowAll 是只放行的账本替身：认 clientKey 和 limitedKey，预留和收尾一律成功，记下收到的预留。
// 它不判断额度、不记余额，额度的规则只在 MySQL 里有一份，由下面用真库的测试检查。
type allowAll struct {
	mu       sync.Mutex
	reserved []ledger.Reservation
}

func newAllowAll() *allowAll { return &allowAll{} }

func (a *allowAll) KeyByHash(_ context.Context, hash string) (*ledger.Key, error) {
	switch hash {
	case apikey.Hash(clientKey):
		return &ledger.Key{ID: 1, AccountID: 1, Account: "acct-full", Models: allAliases}, nil
	case apikey.Hash(limitedKey):
		return &ledger.Key{ID: 2, AccountID: 2, Account: "acct-limited", Models: []string{"shared"}}, nil
	}
	return nil, nil
}

func (a *allowAll) Reserve(_ context.Context, r ledger.Reservation) (*ledger.Replay, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reserved = append(a.reserved, r)
	return nil, nil
}

func (a *allowAll) MarkSent(context.Context, ledger.Reservation) error { return nil }

func (a *allowAll) Finish(context.Context, ledger.Reservation, ledger.Result) error { return nil }

func (a *allowAll) Call(context.Context, int64, string) (*ledger.CallInfo, error) { return nil, nil }

func (a *allowAll) lastReservation(t *testing.T) ledger.Reservation {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.reserved) == 0 {
		t.Fatal("nothing was reserved")
	}
	return a.reserved[len(a.reserved)-1]
}

// faulty 包住一个账本，让某一步失败，或者在预留成功之后执行 afterReserve。
// markSentErr 不碰账本、直接失败；markSentCommits 时先真的落发送意图、再返回这个错误（提交成功了、网关却收到错误）。
type faulty struct {
	Ledger
	keyErr, reserveErr, markSentErr, finishErr, lookupErr error
	markSentCommits                                       bool
	afterReserve                                          func()
}

func (f *faulty) KeyByHash(ctx context.Context, hash string) (*ledger.Key, error) {
	if f.keyErr != nil {
		return nil, f.keyErr
	}
	return f.Ledger.KeyByHash(ctx, hash)
}

func (f *faulty) Reserve(ctx context.Context, r ledger.Reservation) (*ledger.Replay, error) {
	if f.reserveErr != nil {
		return nil, f.reserveErr
	}
	replay, err := f.Ledger.Reserve(ctx, r)
	if err == nil && replay == nil && f.afterReserve != nil {
		f.afterReserve()
	}
	return replay, err
}

func (f *faulty) MarkSent(ctx context.Context, r ledger.Reservation) error {
	switch {
	case f.markSentErr == nil:
		return f.Ledger.MarkSent(ctx, r)
	case f.markSentCommits:
		if err := f.Ledger.MarkSent(ctx, r); err != nil {
			return err
		}
	}
	return f.markSentErr
}

func (f *faulty) Call(ctx context.Context, accountID int64, id string) (*ledger.CallInfo, error) {
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return f.Ledger.Call(ctx, accountID, id)
}

func (f *faulty) Finish(ctx context.Context, r ledger.Reservation, res ledger.Result) error {
	if f.finishErr != nil {
		return f.finishErr
	}
	return f.Ledger.Finish(ctx, r, res)
}

func errorCode(t *testing.T, r reply) string {
	t.Helper()
	var e struct{ Error struct{ Code string } }
	_ = json.Unmarshal(r.body, &e)
	return e.Error.Code
}

// 估算：输入是转发给上游的请求体字节数，输出是 max_tokens、没写（或 null）时模型的默认值，embeddings 为 0。
func TestEstimate(t *testing.T) {
	stub := newAllowAll()
	h := newHarnessWith(t, stub)
	cases := []struct {
		name, path, body, alias, upstreamModel string
		output                                 int64
	}{
		{"max_tokens", "/v1/chat/completions", `{"model":"shared","max_tokens":300}`, "shared", "fake-chat", 300},
		{"max_tokens zero", "/v1/chat/completions", `{"model":"shared","max_tokens":0}`, "shared", "fake-chat", 0},
		{"max_tokens at the limit", "/v1/chat/completions", `{"model":"shared","max_tokens":2147483647}`, "shared", "fake-chat", 2147483647},
		{"default", "/v1/chat/completions", `{"model":"shared"}`, "shared", "fake-chat", defaultMaxTokens},
		{"null is the default", "/v1/chat/completions", `{"model":"shared","max_tokens":null}`, "shared", "fake-chat", defaultMaxTokens},
		{"embeddings", "/v1/embeddings", `{"model":"shared","input":["a"],"max_tokens":99}`, "shared", "fake-embed", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := h.post(context.Background(), tc.path, clientKey, tc.body)
			if r.err != nil || r.status != http.StatusOK {
				t.Fatalf("status = %d, err = %v", r.status, r.err)
			}
			got := stub.lastReservation(t)
			forwarded := strings.Replace(tc.body, `"`+tc.alias+`"`, `"`+tc.upstreamModel+`"`, 1)
			if got.EstInput != int64(len(forwarded)) || got.EstOutput != tc.output || got.Amount() != int64(len(forwarded))+tc.output {
				t.Fatalf("estimate = %d + %d, want %d + %d", got.EstInput, got.EstOutput, len(forwarded), tc.output)
			}
			if got.CallID != requestID(t, r) || got.Alias != tc.alias || got.UpstreamModel != tc.upstreamModel || got.KeyID != 1 {
				t.Fatalf("reservation = %+v", got)
			}
		})
	}
}

// max_tokens 和 model、stream 同一套规则：必须是 0 到 2^31-1 的整数，歧义写法一律 400，都不调上游。
func TestMaxTokensRejected(t *testing.T) {
	h := newHarness(t)
	for _, body := range []string{
		`{"model":"shared","max_tokens":1.5}`,
		`{"model":"shared","max_tokens":1.0}`,
		`{"model":"shared","max_tokens":1e3}`,
		`{"model":"shared","max_tokens":"8"}`,
		`{"model":"shared","max_tokens":true}`,
		`{"model":"shared","max_tokens":-1}`,
		`{"model":"shared","max_tokens":2147483648}`,
		`{"model":"shared","max_tokens":99999999999999999999}`,
		`{"model":"shared","max_tokens":8,"max_tokens":100000}`,
		`{"model":"shared","max_tokens":1,"MAX_TOKENS":100000}`,
		`{"model":"shared","Max_Tokens":100000}`,
		`{"model":"shared","max_toKens":100000}`, // KELVIN SIGN 折叠成 k
		`{"model":"shared","max_tokens":1,"max_toKens":100000}`,
	} {
		r := h.chat(t, body)
		if r.status != http.StatusBadRequest || errorCode(t, r) != "invalid_request" {
			t.Errorf("%s: status = %d, code = %s, want 400 invalid_request", body, r.status, errorCode(t, r))
		}
	}
	if n := h.fake.Stats().ChatCompletions; n != 0 {
		t.Fatalf("upstream got %d requests", n)
	}
}

// 账本出错（查 Key、预留）都是 503 ledger_unavailable，不调上游、不放行；额度不够是 429 并带 Retry-After。
func TestLedgerErrorsWithStub(t *testing.T) {
	cases := []struct {
		name   string
		ledger *faulty
		status int
		code   string
	}{
		{"key lookup fails", &faulty{Ledger: newAllowAll(), keyErr: errors.New("dial tcp: connection refused")}, 503, "ledger_unavailable"},
		{"reserve fails", &faulty{Ledger: newAllowAll(), reserveErr: errors.New("Error 1205: Lock wait timeout exceeded")}, 503, "ledger_unavailable"},
		{"quota exhausted", &faulty{Ledger: newAllowAll(), reserveErr: ledger.ErrQuotaExhausted}, 429, "quota_exhausted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarnessWith(t, tc.ledger)
			r := h.chat(t, `{"model":"shared"}`)
			if r.status != tc.status || errorCode(t, r) != tc.code {
				t.Fatalf("status = %d, code = %s, want %d %s", r.status, errorCode(t, r), tc.status, tc.code)
			}
			if h.fake.Stats().ChatCompletions != 0 {
				t.Fatal("request reached the upstream")
			}
			ra := r.header.Get("Retry-After")
			if tc.status == 429 && ra != "5" { // 较大的上游超时：embeddings 的 5 秒
				t.Fatalf("Retry-After = %q, want 5", ra)
			}
			if tc.status == 503 && ra != "" {
				t.Fatalf("503 has Retry-After %q", ra)
			}
			l := h.logLine(t, requestID(t, r))
			if l["error_code"] != tc.code || l["status"] != float64(tc.status) {
				t.Fatalf("log = %v", l)
			}
			if _, ok := l["call_status"]; ok {
				t.Fatalf("a rejected request logged call_status %v", l["call_status"])
			}
		})
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	for _, tc := range []struct {
		chat, emb int64
		want      string
	}{{12000, 25000, "25"}, {30000, 25000, "30"}, {500, 1200, "2"}} {
		cfg := testConfig(t, tc.chat, tc.emb)
		if got := retryAfterSeconds(cfg); got != tc.want {
			t.Errorf("timeouts %d/%d ms: Retry-After = %s, want %s", tc.chat, tc.emb, got, tc.want)
		}
	}
}

// --- 以下用真的 MySQL ---

// dbEnv 是一个用真账本的 QF：一个只属于当前测试的账户，key 能用全部别名，limited 只能用 shared。
type dbEnv struct {
	*harness
	store   *ledger.Store
	admin   *sql.DB
	account string
	key     string
	limited string
}

// newDBEnv 起一个用真账本的 QF。wrap 不为 nil 时用它包住账本（注入故障）。
func newDBEnv(t *testing.T, limit int64, wrap func(Ledger) Ledger) *dbEnv {
	t.Helper()
	return newDBEnvLimited(t, limit, wrap, &stubLimiter{})
}

// newDBEnvLimited 同 newDBEnv，限流器用给定的。
func newDBEnvLimited(t *testing.T, limit int64, wrap func(Ledger) Ledger, lim Limiter) *dbEnv {
	t.Helper()
	e := &dbEnv{store: dbtest.Store(t), admin: dbtest.Admin(t)}
	var l Ledger = e.store
	if wrap != nil {
		l = wrap(l)
	}
	e.harness = newHarnessLimited(t, l, lim) // 先起 harness：allAliases 在这里生成
	e.account = dbtest.Account(t, e.store, limit)
	e.key = e.issue(t, allAliases)
	e.limited = e.issue(t, []string{"shared"})
	return e
}

func (e *dbEnv) issue(t *testing.T, models []string) string {
	t.Helper()
	k, err := e.store.IssueKey(context.Background(), e.account, models, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	return k.Secret
}

func (e *dbEnv) send(t *testing.T, path, body string, headers ...string) reply {
	t.Helper()
	r := e.post(context.Background(), path, e.key, body, headers...)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r
}

func (e *dbEnv) budget(t *testing.T) (limit, reserved, consumed int64) {
	t.Helper()
	return dbtest.Budget(t, e.admin, e.account)
}

// callRow 是 calls 里的一行和它的账本。
type callRow struct {
	status, errorCode, clientRequestID, runID, upstreamXRequestID, responseID, responseModel sql.NullString
	reserved                                                                                 int64
	httpStatus                                                                               sql.NullInt64
	usagePrompt, usageCompletion, usageTotal                                                 sql.NullInt64
	kinds                                                                                    []string
}

func (e *dbEnv) call(t *testing.T, id string) callRow {
	t.Helper()
	var c callRow
	err := e.admin.QueryRow(`SELECT status, reserved_tokens, http_status, error_code, client_request_id, run_id,
		upstream_x_request_id, upstream_response_id, response_model, usage_prompt_tokens, usage_completion_tokens,
		usage_total_tokens FROM calls WHERE id = ?`, id).Scan(&c.status, &c.reserved, &c.httpStatus, &c.errorCode,
		&c.clientRequestID, &c.runID, &c.upstreamXRequestID, &c.responseID, &c.responseModel,
		&c.usagePrompt, &c.usageCompletion, &c.usageTotal)
	if err != nil {
		t.Fatalf("calls row %s: %v", id, err)
	}
	rows, err := e.admin.Query("SELECT kind FROM ledger WHERE call_id = ? ORDER BY id", id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		c.kinds = append(c.kinds, k)
	}
	return c
}

func (e *dbEnv) callCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(`SELECT COUNT(*) FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = ?`, e.account).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 经真的 MySQL 走完一次请求：鉴权、预留、转发、结算、写回。
func TestFullRequestThroughMySQL(t *testing.T) {
	e := newDBEnv(t, 100000, nil)
	r := e.send(t, "/v1/chat/completions", withModel(toolsBody, `"shared"`),
		"X-Client-Request-Id", "crid-1", "X-Run-Id", "run-1")
	if r.status != http.StatusOK {
		t.Fatalf("status = %d: %s", r.status, r.body)
	}
	var resp struct{ ID, Model string }
	if err := json.Unmarshal(r.body, &resp); err != nil {
		t.Fatal(err)
	}
	var u struct {
		Usage struct {
			Prompt     int64 `json:"prompt_tokens"`
			Completion int64 `json:"completion_tokens"`
			Total      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	_ = json.Unmarshal(r.body, &u)
	id := requestID(t, r)
	c := e.call(t, id)
	forwarded := withModel(toolsBody, `"fake-chat"`)
	if c.status.String != "SETTLED" || c.reserved != int64(len(forwarded))+256 || !slices.Equal(c.kinds, []string{"reserve", "settle"}) {
		t.Fatalf("call = %+v (reserved want %d)", c, len(forwarded)+256)
	}
	if c.usagePrompt.Int64 != u.Usage.Prompt || c.usageCompletion.Int64 != u.Usage.Completion || c.usageTotal.Int64 != u.Usage.Total || u.Usage.Total == 0 {
		t.Fatalf("usage in calls = %v/%v/%v, response %+v", c.usagePrompt, c.usageCompletion, c.usageTotal, u.Usage)
	}
	if c.httpStatus.Int64 != 200 || c.errorCode.Valid || c.clientRequestID.String != "crid-1" || c.runID.String != "run-1" ||
		c.upstreamXRequestID.String != "fake-chat-1" || c.responseID.String != resp.ID || c.responseModel.String != resp.Model {
		t.Fatalf("call = %+v", c)
	}
	if limit, reserved, consumed := e.budget(t); limit != 100000 || reserved != 0 || consumed != u.Usage.Total {
		t.Fatalf("budget = %d/%d/%d", limit, reserved, consumed)
	}
	l := e.logLine(t, id)
	if l["call_status"] != "SETTLED" || l["reserved_tokens"] != float64(c.reserved) || l["account"] != e.account {
		t.Fatalf("log = %v", l)
	}
	dbtest.Verify(t, e.store)
}

func TestAuthWithDatabaseKeys(t *testing.T) {
	e := newDBEnv(t, 100000, nil)
	ctx := context.Background()
	if r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`); r.status != http.StatusOK {
		t.Fatalf("valid key: %d", r.status)
	}
	if r := e.post(ctx, "/v1/chat/completions", "qfk_never-issued-key", `{"model":"shared"}`); r.status != 401 || errorCode(t, r) != "invalid_api_key" {
		t.Fatalf("unknown key: %d %s", r.status, r.body)
	}
	if r := e.post(ctx, "/v1/chat/completions", e.limited, `{"model":"fake-tool-call"}`); r.status != 403 || errorCode(t, r) != "model_not_allowed" {
		t.Fatalf("model not allowed: %d %s", r.status, r.body)
	}
	if r := e.post(ctx, "/v1/chat/completions", e.limited, `{"model":"shared"}`); r.status != 200 {
		t.Fatalf("limited key on an allowed model: %d", r.status)
	}
	keys, err := e.store.ListKeys(ctx, e.account)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.RevokeKey(ctx, keys[0].ID); err != nil {
		t.Fatal(err)
	}
	if r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`); r.status != 401 || errorCode(t, r) != "invalid_api_key" {
		t.Fatalf("revoked key: %d %s", r.status, r.body)
	}
	if n := e.callCount(t); n != 2 {
		t.Fatalf("%d calls rows, want 2: rejected requests must not reach the ledger", n)
	}
	dbtest.Verify(t, e.store)
}

// 收尾表的每一行：调用状态、账本行、额度行。
func TestFinishTable(t *testing.T) {
	cases := []struct {
		name, path, body string
		status           int
		callStatus       string
		kinds            []string
	}{
		{"2xx with usage", "/v1/chat/completions", `{"model":"shared"}`, 200, "SETTLED", []string{"reserve", "settle"}},
		{"embeddings with usage", "/v1/embeddings", `{"model":"shared","input":["abc"]}`, 200, "SETTLED", []string{"reserve", "settle"}},
		{"2xx without usage", "/v1/chat/completions", `{"model":"fake-no-usage"}`, 200, "USAGE_UNKNOWN", []string{"reserve"}},
		{"2xx with invalid usage", "/v1/chat/completions", `{"model":"no-completion"}`, 200, "USAGE_UNKNOWN", []string{"reserve"}},
		{"invalid response", "/v1/chat/completions", `{"model":"fake-not-json"}`, 502, "USAGE_UNKNOWN", []string{"reserve"}},
		{"redirect", "/v1/chat/completions", `{"model":"redirect"}`, 502, "USAGE_UNKNOWN", []string{"reserve"}},
		{"upstream 500", "/v1/chat/completions", `{"model":"fake-500"}`, 502, "USAGE_UNKNOWN", []string{"reserve"}},
		{"upstream 503", "/v1/chat/completions", `{"model":"status-503"}`, 502, "USAGE_UNKNOWN", []string{"reserve"}},
		{"timeout after sending", "/v1/chat/completions", `{"model":"slow"}`, 504, "USAGE_UNKNOWN", []string{"reserve"}},
		{"connection refused", "/v1/chat/completions", `{"model":"dead"}`, 502, "CANCELLED", []string{"reserve", "release"}},
		{"upstream 400", "/v1/chat/completions", `{"model":"status-400"}`, 400, "CANCELLED", []string{"reserve", "release"}},
		{"upstream 401", "/v1/chat/completions", `{"model":"fake-401"}`, 502, "CANCELLED", []string{"reserve", "release"}},
		{"upstream 403", "/v1/chat/completions", `{"model":"status-403"}`, 502, "CANCELLED", []string{"reserve", "release"}},
		{"upstream 429", "/v1/chat/completions", `{"model":"fake-429"}`, 503, "CANCELLED", []string{"reserve", "release"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newDBEnv(t, 100000, nil)
			r := e.send(t, tc.path, tc.body)
			if r.status != tc.status {
				t.Fatalf("status = %d, want %d", r.status, tc.status)
			}
			c := e.call(t, requestID(t, r))
			if c.status.String != tc.callStatus || !slices.Equal(c.kinds, tc.kinds) || c.httpStatus.Int64 != int64(tc.status) {
				t.Fatalf("call = %s %v http %v, want %s %v", c.status.String, c.kinds, c.httpStatus, tc.callStatus, tc.kinds)
			}
			if code := errorCode(t, r); code != c.errorCode.String {
				t.Fatalf("calls.error_code = %q, response code %q", c.errorCode.String, code)
			}
			_, reserved, consumed := e.budget(t)
			switch tc.callStatus {
			case "SETTLED":
				if reserved != 0 || !c.usageTotal.Valid || consumed != c.usageTotal.Int64 {
					t.Fatalf("budget reserved %d consumed %d, usage %v", reserved, consumed, c.usageTotal)
				}
			case "CANCELLED":
				if reserved != 0 || consumed != 0 || c.usageTotal.Valid {
					t.Fatalf("budget reserved %d consumed %d, usage %v", reserved, consumed, c.usageTotal)
				}
			case "USAGE_UNKNOWN": // 继续占着预留，不补 0
				if reserved != c.reserved || consumed != 0 || c.usageTotal.Valid {
					t.Fatalf("budget reserved %d (call %d) consumed %d, usage %v", reserved, c.reserved, consumed, c.usageTotal)
				}
			}
			if l := e.logLine(t, requestID(t, r)); l["call_status"] != tc.callStatus || l["reserved_tokens"] != float64(c.reserved) {
				t.Fatalf("log call_status = %v, reserved_tokens = %v", l["call_status"], l["reserved_tokens"])
			}
			dbtest.Verify(t, e.store)
		})
	}
}

// 实际用量超过预留：如实结算，已消费可以超过上限，下一次请求立即 429，上游收不到。
func TestOveruse(t *testing.T) {
	const body = `{"model":"fake-overuse","max_tokens":10}`
	e := newDBEnv(t, int64(len(body))+10+5, nil)
	r := e.send(t, "/v1/chat/completions", body)
	if r.status != http.StatusOK {
		t.Fatalf("status = %d", r.status)
	}
	limit, reserved, consumed := e.budget(t)
	if reserved != 0 || consumed != fakeupstream.OveruseTokens+10 || consumed <= limit {
		t.Fatalf("budget = %d/%d/%d", limit, reserved, consumed)
	}
	before := e.fake.Stats().ChatCompletions
	start := time.Now()
	r = e.send(t, "/v1/chat/completions", `{"model":"shared","max_tokens":0}`)
	elapsed := time.Since(start)
	if r.status != http.StatusTooManyRequests || errorCode(t, r) != "quota_exhausted" || r.header.Get("Retry-After") != "5" {
		t.Fatalf("after overuse: status = %d, code = %s, Retry-After = %q", r.status, errorCode(t, r), r.header.Get("Retry-After"))
	}
	if e.fake.Stats().ChatCompletions != before {
		t.Fatal("a 429 request reached the upstream")
	}
	t.Logf("429 after overuse took %v", elapsed)
	dbtest.Verify(t, e.store)
}

// 额度正好够预留 10 个请求，50 个请求同时发，上游先挂住：恰好 10 个被接受、40 个立即 429；
// 过程中反复读额度行，任何时刻 已预留 + 已消费 ≤ 上限；放开上游后 10 个都结算，账本校验通过。
func TestConcurrentReservations(t *testing.T) {
	const n, fit = 50, 10
	const body = `{"model":"emb-hold","input":["concurrency"]}`
	amount := int64(len(strings.Replace(body, "emb-hold", "fake-hold", 1))) // embeddings 没有输出估算
	e := newDBEnv(t, fit*amount, nil)

	stop := make(chan struct{})
	var samples atomic.Int64
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			limit, reserved, consumed := dbtest.Budget(t, e.admin, e.account)
			if reserved+consumed > limit {
				t.Errorf("reserved %d + consumed %d > limit %d", reserved, consumed, limit)
			}
			samples.Add(1)
		}
	})

	start := make(chan struct{})
	replies := make([]reply, n)
	took := make([]time.Duration, n)
	var done atomic.Int64
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			t0 := time.Now()
			replies[i] = e.post(context.Background(), "/v1/embeddings", e.key, body)
			took[i] = time.Since(t0)
			done.Add(1)
		})
	}
	close(start)
	// 等 40 个 429 都返回、10 个都挂在上游；不按时间判断对错，只设一个很宽的上限。
	deadline := time.Now().Add(30 * time.Second)
	for done.Load() < n-fit || e.fake.Stats().Held < fit {
		if time.Now().After(deadline) {
			t.Fatalf("returned %d, held %d", done.Load(), e.fake.Stats().Held)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if h := e.fake.Stats().Held; h != fit {
		t.Fatalf("held %d upstream requests, want %d", h, fit)
	}
	if _, reserved, _ := e.budget(t); reserved != fit*amount {
		t.Fatalf("reserved = %d, want %d", reserved, fit*amount)
	}
	e.fake.Release()
	wg.Wait()
	close(stop)
	sampler.Wait()

	var ok int
	var rejected []time.Duration
	for i, r := range replies {
		switch {
		case r.err != nil:
			t.Fatalf("request %d: %v", i, r.err)
		case r.status == http.StatusOK:
			ok++
		case r.status == http.StatusTooManyRequests && errorCode(t, r) == "quota_exhausted" && r.header.Get("Retry-After") != "":
			rejected = append(rejected, took[i])
		default: // 503（含死锁、锁等待超时）不是正常结果
			t.Fatalf("request %d: status %d %s; log %v", i, r.status, r.body, e.logLine(t, requestID(t, r)))
		}
	}
	if ok != fit || len(rejected) != n-fit {
		t.Fatalf("%d accepted, %d rejected; want %d and %d", ok, len(rejected), fit, n-fit)
	}
	if strings.Contains(e.logs.String(), "ledger_error") {
		t.Fatal("ledger errors in the call log")
	}
	if samples.Load() == 0 {
		t.Fatal("the budget row was never sampled")
	}
	slices.Sort(rejected)
	t.Logf("429 latency: p50 %v, p99 %v, max %v (%d samples of the budget row)",
		rejected[len(rejected)/2], rejected[len(rejected)*99/100], rejected[len(rejected)-1], samples.Load())
	if _, reserved, consumed := e.budget(t); reserved != 0 || consumed == 0 {
		t.Fatalf("after release: reserved %d, consumed %d", reserved, consumed)
	}
	dbtest.Verify(t, e.store)
}

// 随机：很多调用并发进行，结果在结算、超用、释放、未知之间随机；一部分客户端在随机的时刻断开；同时有一个回收按真实的上界
// 一直在跑（它不该碰任何活着的调用）。结束后用假上游的账单对账，再对一遍什么都不改，账本校验通过，
// 而且每个调用的结局和假上游的“真相”一致：结算的用量等于账单；释放的上游没有消费；还未知的只能是假上游没见过的；
// 每个账户的已消费 = 账单里这个账户的调用消费的合计。种子打印出来，设 QUOTAFORGE_TEST_SEED 可以复现。
// 一部分调用带幂等键：各账户在一小组键里挑，请求体多数是这个键固定的那个，少数是新的；
// 结束后每个键最多一次可能被计费的上游调用（不是 CANCELLED 的调用）。
func TestRandomCalls(t *testing.T) {
	rng := testRand(t)

	// 别名 → 允许的 HTTP 状态（429 是额度不够）
	outcomes := map[string][]int{
		"shared":        {200, 429},
		"fake-overuse":  {200, 429},
		"fake-no-usage": {200, 429},
		"fake-500":      {502, 429},
		"fake-not-json": {502, 429},
		"status-400":    {400, 429},
		"fake-401":      {502, 429},
		"fake-429":      {503, 429},
		"dead":          {502, 429},
		"quick":         {200, 429}, // 这个别名的请求，客户端有一半会在随机的时刻断开
	}
	aliases := make([]string, 0, len(outcomes))
	for a := range outcomes {
		aliases = append(aliases, a)
	}
	slices.Sort(aliases)

	// 一次 fake-overuse 用掉约 100 万：上限取 100 万到 500 万，各账户跑到中途才开始 429。
	limit := func() int64 { return int64(1_000_000 + rng.IntN(4_000_000)) }
	base := newDBEnv(t, limit(), nil)
	envs := []*dbEnv{base}
	for range 3 { // 同一个 QF 上再建 3 个账户
		e := *base
		e.account = dbtest.Account(t, base.store, limit())
		e.key = e.issue(t, allAliases)
		envs = append(envs, &e)
	}
	type job struct {
		env       int
		body, key string
		cancelAt  time.Duration // 大于 0 时客户端在这之后断开
	}
	jobs := make(chan job)
	var mu sync.Mutex
	statuses := map[int]int{}
	reclaimDone := make(chan struct{})
	stopReclaim := make(chan struct{})
	go func() { // 一直按真实的上界回收：活着的调用都在上界之内，一个都不该被它碰
		defer close(reclaimDone)
		for {
			select {
			case <-stopReclaim:
				return
			default:
			}
			if _, err := base.store.Reclaim(context.Background(), testReclaimAfter, config.LedgerTimeout); err != nil {
				t.Errorf("reclaim: %v", err)
			}
		}
	}()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for j := range jobs {
				e := envs[j.env]
				var headers []string
				if j.key != "" {
					headers = []string{"Idempotency-Key", j.key}
				}
				ctx, cancel := context.Background(), context.CancelFunc(func() {})
				if j.cancelAt > 0 {
					ctx, cancel = context.WithTimeout(ctx, j.cancelAt)
				}
				r := e.post(ctx, "/v1/chat/completions", e.key, j.body, headers...)
				cancel()
				if r.err != nil {
					if j.cancelAt == 0 {
						t.Errorf("%s: %v", j.body, r.err)
					}
					continue
				}
				var p struct{ Model string }
				_ = json.Unmarshal([]byte(j.body), &p)
				keyed := j.key != "" && (r.status == 409 || r.status == 422)
				if !keyed && !slices.Contains(outcomes[p.Model], r.status) || errorCode(t, r) == "ledger_unavailable" {
					t.Errorf("%s: status %d %s", j.body, r.status, r.body)
					continue
				}
				mu.Lock()
				statuses[r.status]++
				mu.Unlock()
			}
		})
	}
	randomBody := func() string {
		return fmt.Sprintf(`{"model":%q,"max_tokens":%d,"messages":[{"role":"user","content":%q}]}`,
			aliases[rng.IntN(len(aliases))], rng.IntN(200), strings.Repeat("x", rng.IntN(50)))
	}
	keyBodies := map[string]string{} // 账户和键 → 这个键固定的请求体
	for i := range 300 {
		j := job{env: i % len(envs), body: randomBody()}
		if strings.Contains(j.body, `"quick"`) && rng.IntN(2) == 0 {
			j.cancelAt = time.Duration(1+rng.IntN(250)) * time.Millisecond
		}
		if rng.IntN(2) == 0 {
			j.key = fmt.Sprintf("rk-%d", rng.IntN(8))
			id := fmt.Sprintf("%d/%s", j.env, j.key)
			if keyBodies[id] == "" {
				keyBodies[id] = j.body
			}
			if rng.IntN(4) > 0 {
				j.body = keyBodies[id]
			}
		}
		jobs <- j
	}
	close(jobs)
	wg.Wait()
	// 客户端断开的请求，客户端先返回了，QF 还在等上游、收尾（有界结算）。关掉 QF 的测试服务器：Close 等所有进行中的
	// 请求处理完才返回（确定的事件），之后再停回收、读账单。后面只用账本和日志，不再发请求。
	base.qf.Close()
	close(stopReclaim)
	<-reclaimDone

	accounts := []any{envs[0].account, envs[1].account, envs[2].account, envs[3].account}
	in := "a.name IN (?, ?, ?, ?)"
	if strings.Contains(base.logs.String(), "ledger_error") {
		t.Fatal("ledger errors in the call log")
	}
	t.Logf("responses by status: %v", statuses)
	var billable, reclaimed int
	if err := base.admin.QueryRow(`SELECT COUNT(*) FROM (SELECT 1 FROM calls c JOIN accounts a ON a.id = c.account_id
		WHERE `+in+` AND c.idempotency_key IS NOT NULL AND c.status <> 'CANCELLED'
		GROUP BY c.account_id, c.idempotency_key HAVING COUNT(*) > 1) x`, accounts...).Scan(&billable); err != nil || billable != 0 {
		t.Errorf("%d keys have more than one billable upstream call (%v)", billable, err)
	}
	if err := base.admin.QueryRow(`SELECT COUNT(*) FROM calls c JOIN accounts a ON a.id = c.account_id
		WHERE `+in+` AND (c.resolved_by = 'reclaim' OR c.error_code LIKE 'reclaimed%')`, accounts...).Scan(&reclaimed); err != nil || reclaimed != 0 {
		t.Errorf("the reclaim touched %d live calls (%v)", reclaimed, err)
	}

	// 用假上游的账单对账：执行一遍，再执行一遍什么都不改
	bill := base.bill(t)
	applied := map[ledger.Verdict]int{}
	for _, it := range base.store.Reconcile(context.Background(), bill, true) {
		applied[it.Verdict]++
		if it.Verdict == ledger.Inconsistent || it.Verdict == ledger.InProgress || it.Verdict == ledger.Failed {
			t.Errorf("reconcile: %+v", it)
		}
	}
	t.Logf("clients that disconnected: %d; reconcile on the fake bill: %v", strings.Count(base.logs.String(), `"client_disconnected":true`), applied)
	for _, it := range base.store.Reconcile(context.Background(), bill, true) {
		if it.Applied || (it.Verdict != ledger.Consistent && it.Verdict != ledger.NotFound) {
			t.Errorf("second reconcile: %+v", it)
		}
	}
	// 每个调用的结局和假上游的“真相”一致
	rows, err := base.admin.Query(`SELECT a.name, c.id, c.status, COALESCE(c.usage_total_tokens, -1), c.upstream FROM calls c
		JOIN accounts a ON a.id = c.account_id WHERE `+in, accounts...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	billed := map[string]int64{} // 账户 → 账单里的消费
	for rows.Next() {
		var account, id, status, upstream string
		var total int64
		if err := rows.Scan(&account, &id, &status, &total, &upstream); err != nil {
			t.Fatal(err)
		}
		row := billRow(bill, id)
		if row != nil && row.Status == ledger.Settled {
			billed[account] += row.Usage.Total
		}
		switch {
		case status == "SETTLED" && (row == nil || row.Status != ledger.Settled || row.Usage.Total != total),
			status == "CANCELLED" && row != nil && row.Status != ledger.Cancelled,
			status == "USAGE_UNKNOWN" && (row != nil || upstream == "fake"),
			status == "RESERVED" || status == "IN_FLIGHT":
			t.Errorf("call %s (%s): %s total %d, bill %+v; log %v", id, upstream, status, total, row, base.logLine(t, id))
		}
	}
	for _, e := range envs {
		if _, reserved, got := e.budget(t); got != billed[e.account] || reserved < 0 {
			t.Errorf("account %s: consumed %d, the bill adds up to %d; reserved %d", e.account, got, billed[e.account], reserved)
		}
	}
	dbtest.Verify(t, base.store)
}

// 数据库不可用：503，上游收不到请求，什么都不放行。
func TestDatabaseDown(t *testing.T) {
	down, err := ledger.Open("qf_app:test-only@tcp(127.0.0.1:1)/quotaforge_test?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	e := newDBEnv(t, 100000, nil)
	h := newHarnessWith(t, down)
	r := h.post(context.Background(), "/v1/chat/completions", e.key, `{"model":"shared"}`)
	if r.err != nil || r.status != http.StatusServiceUnavailable || errorCode(t, r) != "ledger_unavailable" {
		t.Fatalf("status = %d, body = %s", r.status, r.body)
	}
	if h.fake.Stats().ChatCompletions != 0 {
		t.Fatal("request reached the upstream")
	}
	l := h.logLine(t, requestID(t, r))
	if msg, _ := l["ledger_error"].(string); msg == "" || strings.Contains(msg, "test-only") {
		t.Fatalf("ledger_error = %q", msg)
	}

	// 预留这一步出错：同样 503，库里没有这次调用
	f := newDBEnv(t, 100000, func(l Ledger) Ledger {
		return &faulty{Ledger: l, reserveErr: errors.New("Error 1205 (HY000): Lock wait timeout exceeded")}
	})
	r = f.send(t, "/v1/chat/completions", `{"model":"shared"}`)
	if r.status != http.StatusServiceUnavailable || f.fake.Stats().ChatCompletions != 0 || f.callCount(t) != 0 {
		t.Fatalf("reserve error: status = %d, upstream %d, calls %d", r.status, f.fake.Stats().ChatCompletions, f.callCount(t))
	}
}

// 收尾的事务失败：上游的 2xx 照样返回，调用留在 RESERVED、继续占着预留，调用日志里有 usage 和失败原因。
func TestFinishFailureKeepsResponse(t *testing.T) {
	e := newDBEnv(t, 100000, func(l Ledger) Ledger { return &faulty{Ledger: l, finishErr: errors.New("commit: invalid connection")} })
	r := e.send(t, "/v1/chat/completions", `{"model":"shared","messages":[{"role":"user","content":"hello"}]}`)
	if r.status != http.StatusOK || !strings.Contains(string(r.body), `"fake reply"`) {
		t.Fatalf("status = %d, body = %s", r.status, r.body)
	}
	id := requestID(t, r)
	c := e.call(t, id)
	// 发送意图已经落库：收尾失败的调用留在 IN_FLIGHT（不再是 RESERVED），由回收标成未知；日志的 outcome 是网关自己的结局。
	if c.status.String != "IN_FLIGHT" || !slices.Equal(c.kinds, []string{"reserve"}) {
		t.Fatalf("call = %s %v", c.status.String, c.kinds)
	}
	if _, reserved, _ := e.budget(t); reserved != c.reserved {
		t.Fatalf("reserved = %d, want %d", reserved, c.reserved)
	}
	l := e.logLine(t, id)
	u, _ := l["usage"].(map[string]any)
	if l["call_status"] != "IN_FLIGHT" || l["outcome"] != "SETTLED" || l["ledger_error"] != "commit: invalid connection" || u["total_tokens"] != float64(15) {
		t.Fatalf("log = %v", l)
	}
	dbtest.Verify(t, e.store)
}

// 客户端在预留之后、转发之前断开：不调上游，释放预留。直接调处理器，预留一成功就取消请求的 context，
// 时序是确定的（经真连接的话，服务端什么时候发现断开不确定）。
func TestClientGoneBeforeForward(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newDBEnv(t, 100000, func(l Ledger) Ledger { return &faulty{Ledger: l, afterReserve: cancel} })
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"shared"}`))
	req.Header.Set("Authorization", "Bearer "+e.key)
	rec := httptest.NewRecorder()
	e.qf.Config.Handler.ServeHTTP(rec, req)
	if rec.Body.Len() != 0 {
		t.Fatalf("wrote a response after the client left: %s", rec.Body)
	}
	id := rec.Header().Get("x-request-id")
	if l := e.logLine(t, id); l["error_code"] != "client_canceled" || l["call_status"] != "CANCELLED" || l["client_disconnected"] != true {
		t.Fatalf("log = %v", l)
	}
	if n := e.fake.Stats().ChatCompletions; n != 0 {
		t.Fatalf("upstream got %d requests", n)
	}
	c := e.call(t, id)
	if c.status.String != "CANCELLED" || !slices.Equal(c.kinds, []string{"reserve", "release"}) || c.httpStatus.Valid {
		t.Fatalf("call = %s %v http %v", c.status.String, c.kinds, c.httpStatus)
	}
	if _, reserved, consumed := e.budget(t); reserved != 0 || consumed != 0 {
		t.Fatalf("budget %d/%d", reserved, consumed)
	}
	dbtest.Verify(t, e.store)
}

// 发出之后客户端断开：上游可能已经在生成，用量未知，继续占着预留。
// 收尾的 context 如果随客户端取消，这里会留在 RESERVED。
func TestClientGoneAfterSending(t *testing.T) {
	e := newDBEnv(t, 100000, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan reply)
	go func() { done <- e.post(ctx, "/v1/chat/completions", e.key, `{"model":"fake-hang"}`) }()
	waitFor(t, func() bool { return e.fake.Stats().ChatCompletions == 1 })
	cancel()
	<-done
	// 有界结算：上游请求不随客户端取消，等到上游超时（504 upstream_timeout）照常收尾，只是不写回。
	var id string
	waitFor(t, func() bool {
		for _, l := range e.logs.lines(t) {
			if l["error_code"] == "upstream_timeout" && l["client_disconnected"] == true && l["status"] == nil {
				id, _ = l["request_id"].(string)
			}
		}
		return id != ""
	})
	c := e.call(t, id)
	if c.status.String != "USAGE_UNKNOWN" || !slices.Equal(c.kinds, []string{"reserve"}) || c.errorCode.String != "upstream_timeout" {
		t.Fatalf("call = %s %v %s", c.status.String, c.kinds, c.errorCode.String)
	}
	if _, reserved, _ := e.budget(t); reserved != c.reserved {
		t.Fatalf("reserved = %d, want %d", reserved, c.reserved)
	}
	dbtest.Verify(t, e.store)
}

// 客户端头进账本和进日志是同一条规则：不超过 128 个字符、没有控制字符、合法 UTF-8，不合法就存 NULL，请求照常。
func TestClientHeadersInCalls(t *testing.T) {
	e := newDBEnv(t, 100000, nil)
	long := strings.Repeat("中", 128)
	r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`, "X-Client-Request-Id", long, "X-Run-Id", "run-"+strings.Repeat("a", 64))
	c := e.call(t, requestID(t, r))
	if r.status != 200 || c.clientRequestID.String != long || c.runID.String != "run-"+strings.Repeat("a", 64) {
		t.Fatalf("status %d, stored %q / %q", r.status, c.clientRequestID.String, c.runID.String)
	}
	for _, bad := range []string{strings.Repeat("中", 129), "bad\tvalue", "bad\xffutf8"} {
		r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`, "X-Client-Request-Id", bad, "X-Run-Id", bad)
		c := e.call(t, requestID(t, r))
		if r.status != 200 || c.clientRequestID.Valid || c.runID.Valid {
			t.Fatalf("%q: status %d, stored %v / %v", bad, r.status, c.clientRequestID, c.runID)
		}
	}
	dbtest.Verify(t, e.store)
}

// testConfig 是只设了两个超时的最小配置。
func testConfig(t *testing.T, chatMS, embeddingsMS int64) *config.Config {
	t.Helper()
	data := fmt.Sprintf(`{"timeouts":{"chat_ms":%d,"embeddings_ms":%d},
		"upstreams":[{"name":"u","base_url":"http://127.0.0.1:1/v1","api_key_env":"K"}],
		"models":[{"alias":"m","purpose":"embeddings","upstream":"u","upstream_model":"m"}],
		"database":{"dsn_env":"K"},"redis":{"url_env":"K"}}`, chatMS, embeddingsMS)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path, func(string) string { return "x" })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// deadlines 包住一个账本，记下每次调用收到的 context：有没有截止时间、离截止还剩多久、调用时是否已被取消。
type deadlines struct {
	Ledger
	mu    sync.Mutex
	calls []ledgerCall
}

type ledgerCall struct {
	method   string
	has      bool
	left     time.Duration
	canceled bool
}

func (d *deadlines) record(ctx context.Context, method string) {
	dl, has := ctx.Deadline()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, ledgerCall{method: method, has: has, left: time.Until(dl), canceled: ctx.Err() != nil})
}

func (d *deadlines) KeyByHash(ctx context.Context, hash string) (*ledger.Key, error) {
	d.record(ctx, "KeyByHash")
	return d.Ledger.KeyByHash(ctx, hash)
}

func (d *deadlines) Reserve(ctx context.Context, r ledger.Reservation) (*ledger.Replay, error) {
	d.record(ctx, "Reserve")
	return d.Ledger.Reserve(ctx, r)
}

func (d *deadlines) MarkSent(ctx context.Context, r ledger.Reservation) error {
	d.record(ctx, "MarkSent")
	return d.Ledger.MarkSent(ctx, r)
}

func (d *deadlines) Finish(ctx context.Context, r ledger.Reservation, res ledger.Result) error {
	d.record(ctx, "Finish")
	return d.Ledger.Finish(ctx, r, res)
}

func (d *deadlines) Call(ctx context.Context, accountID int64, id string) (*ledger.CallInfo, error) {
	d.record(ctx, "Call")
	return d.Ledger.Call(ctx, accountID, id)
}

func (d *deadlines) take() []ledgerCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.calls
	d.calls = nil
	return c
}

// 查 Key、预留、发送意图、收尾各有一个不超过 2 秒的截止时间：数据库断网时请求在 2 秒内得到 503，而不是挂到 TCP 超时。
// 发出后客户端断开时，收尾的 context 仍然有截止时间，而且没有被取消。只读截止时间，不等它到期。
func TestLedgerCallsHaveTwoSecondDeadline(t *testing.T) {
	d := &deadlines{Ledger: newAllowAll()}
	h := newHarnessWith(t, d)
	check := func(t *testing.T, calls []ledgerCall) {
		t.Helper()
		var methods []string
		for _, c := range calls {
			methods = append(methods, c.method)
			if !c.has || c.left <= 0 || c.left > 2*time.Second || c.canceled {
				t.Errorf("%s: deadline %v, %v left, canceled %v; want a deadline at most 2s away", c.method, c.has, c.left, c.canceled)
			}
		}
		if !slices.Equal(methods, []string{"KeyByHash", "Reserve", "MarkSent", "Finish"}) {
			t.Fatalf("ledger calls = %v", methods)
		}
	}

	t.Run("normal request", func(t *testing.T) {
		if r := h.chat(t, `{"model":"shared"}`); r.status != http.StatusOK {
			t.Fatalf("status = %d", r.status)
		}
		check(t, d.take())
	})

	t.Run("client gone after sending", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan reply)
		go func() { done <- h.post(ctx, "/v1/chat/completions", clientKey, `{"model":"fake-hang"}`) }()
		waitFor(t, func() bool { return h.fake.Stats().ChatCompletions == 2 }) // 第一个子测试已经调过一次
		cancel()
		<-done
		var calls []ledgerCall
		waitFor(t, func() bool {
			calls = append(calls, d.take()...)
			return len(calls) == 4
		})
		check(t, calls)
	})
}
