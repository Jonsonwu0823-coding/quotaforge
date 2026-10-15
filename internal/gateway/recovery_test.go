package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// 故障窗口矩阵、被杀点、调用状态查询、幂等的衔接、日志当证据。都用真的 MySQL 和假上游。

const testReclaimAfter = 36 * time.Second // 和默认超时算出来的上界一样，见 config.ReclaimAfter

// audit 是一个调用的发送意图和结清的审计列。
type audit struct {
	sent                 bool
	resolvedBy, evidence string
}

func (e *dbEnv) audit(t *testing.T, id string) audit {
	t.Helper()
	var a audit
	if err := e.admin.QueryRow(`SELECT sent_at IS NOT NULL, COALESCE(resolved_by, ''), COALESCE(resolution_evidence, '')
		FROM calls WHERE id = ?`, id).Scan(&a.sent, &a.resolvedBy, &a.evidence); err != nil {
		t.Fatal(err)
	}
	return a
}

// expect 核对调用的状态、账本行和审计列。
func (e *dbEnv) expect(t *testing.T, id, status string, kinds []string, want audit) callRow {
	t.Helper()
	c := e.call(t, id)
	if got := e.audit(t, id); c.status.String != status || !slices.Equal(c.kinds, kinds) || got != want {
		t.Fatalf("call %s: %s %v %+v, want %s %v %+v", id, c.status.String, c.kinds, got, status, kinds, want)
	}
	return c
}

// reclaim 把调用的 created_at 改到上界之前，跑一轮回收：造出“进程卡住或被杀、过了上界”的样子，不用真的等。
func (e *dbEnv) reclaim(t *testing.T, id string) {
	t.Helper()
	if _, err := e.admin.Exec("UPDATE calls SET created_at = UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND WHERE id = ?",
		(testReclaimAfter + time.Second).Microseconds(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Reclaim(context.Background(), testReclaimAfter, config.LedgerTimeout); err != nil {
		t.Fatal(err)
	}
}

// bill 是假上游到现在为止的账单，按 QF 的账单格式解析。
func (e *dbEnv) bill(t *testing.T) []ledger.Proof {
	t.Helper()
	proofs, err := ledger.ReadBill(bytes.NewReader(e.fake.Bill()))
	if err != nil {
		t.Fatal(err)
	}
	return proofs
}

func billRow(proofs []ledger.Proof, id string) *ledger.Proof {
	for i := range proofs {
		if proofs[i].CallID == id {
			return &proofs[i]
		}
	}
	return nil
}

// reconcileOne 用证据里关于 id 的那一条执行对账，返回判断。
func (e *dbEnv) reconcileOne(t *testing.T, proofs []ledger.Proof, id string) ledger.Item {
	t.Helper()
	p := billRow(proofs, id)
	if p == nil {
		t.Fatalf("no evidence for %s", id)
	}
	return e.store.Reconcile(context.Background(), []ledger.Proof{*p}, true)[0]
}

func (e *dbEnv) latestCall(t *testing.T) string {
	t.Helper()
	var id string
	if err := e.admin.QueryRow(`SELECT c.id FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = ?
		ORDER BY c.created_at DESC LIMIT 1`, e.account).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// 发送意图写失败：503，不发；调用留在 RESERVED，回收之后释放（确定没发出）。日志的 outcome 是网关判定的“释放”。
func TestWindowSendIntentFails(t *testing.T) {
	e := newDBEnv(t, 100000, func(l Ledger) Ledger {
		return &faulty{Ledger: l, markSentErr: errors.New("mark sent: invalid connection")}
	})
	r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`)
	wantCode(t, r, 503, "ledger_unavailable")
	id := requestID(t, r)
	if n := e.upstreamCalls(); n != 0 {
		t.Fatalf("upstream got %d requests", n)
	}
	e.expect(t, id, "RESERVED", []string{"reserve"}, audit{})
	if l := e.logLine(t, id); l["call_status"] != "RESERVED" || l["outcome"] != "CANCELLED" || l["ledger_error"] == nil {
		t.Fatalf("log = %v", l)
	}
	e.reclaim(t, id)
	e.expect(t, id, "CANCELLED", []string{"reserve", "release"}, audit{resolvedBy: "reclaim", evidence: "not_sent"})
	if _, reserved, _ := e.budget(t); reserved != 0 {
		t.Fatalf("reserved = %d", reserved)
	}
	dbtest.Verify(t, e.store)
}

// 发送意图其实提交成功了、网关却收到错误：不发；库里是 IN_FLIGHT，回收标成未知；账单里没有它（从没发出），
// 只有凭日志（outcome 是确定的释放）才能结清：reconcile -log 释放成 CANCELLED。
func TestWindowSendIntentCommitUnknown(t *testing.T) {
	e := newDBEnv(t, 100000, func(l Ledger) Ledger {
		return &faulty{Ledger: l, markSentErr: errors.New("commit: invalid connection"), markSentCommits: true}
	})
	r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`)
	wantCode(t, r, 503, "ledger_unavailable")
	id := requestID(t, r)
	if n := e.upstreamCalls(); n != 0 {
		t.Fatalf("upstream got %d requests", n)
	}
	e.expect(t, id, "IN_FLIGHT", []string{"reserve"}, audit{sent: true})
	e.reclaim(t, id)
	e.expect(t, id, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
	if billRow(e.bill(t), id) != nil {
		t.Fatal("the bill has a call that was never sent")
	}
	e.logLine(t, id)
	proofs, _, err := ReadCallLog(strings.NewReader(e.logs.String()))
	if err != nil {
		t.Fatal(err)
	}
	if it := e.reconcileOne(t, proofs, id); it.Verdict != ledger.Release || !it.Applied {
		t.Fatalf("reconcile = %+v", it)
	}
	e.expect(t, id, "CANCELLED", []string{"reserve", "release"}, audit{sent: true, resolvedBy: "reconcile", evidence: "call_log"})
	dbtest.Verify(t, e.store)
}

// 发送意图被回收抢先（实例在预留和发送意图之间卡住超过上界）：不发，503；调用已经被回收释放。
func TestWindowSendIntentPreempted(t *testing.T) {
	var e *dbEnv
	e = newDBEnv(t, 100000, func(l Ledger) Ledger {
		return &faulty{Ledger: l, afterReserve: func() { e.reclaim(t, e.latestCall(t)) }}
	})
	r := e.send(t, "/v1/chat/completions", `{"model":"shared"}`)
	wantCode(t, r, 503, "ledger_unavailable")
	id := requestID(t, r)
	if n := e.upstreamCalls(); n != 0 {
		t.Fatalf("upstream got %d requests", n)
	}
	e.expect(t, id, "CANCELLED", []string{"reserve", "release"}, audit{resolvedBy: "reclaim", evidence: "not_sent"})
	if l := e.logLine(t, id); !strings.Contains(l["ledger_error"].(string), "no longer RESERVED") || l["outcome"] != "CANCELLED" {
		t.Fatalf("log = %v", l)
	}
	dbtest.Verify(t, e.store)
}

// 上游结果怎么收尾（docs/ledger.md 的表）：每一行一个子测试，核对 HTTP 结果、状态、账本行、审计列和假上游的账单。
func TestWindowUpstreamOutcomes(t *testing.T) {
	e := newDBEnv(t, 10_000_000, nil)
	cases := []struct {
		alias, code string
		status      int
		call        string
		kinds       []string
		audit       audit
		billed      string // 假上游账单里这一行的 consumed；空表示没有这一行
	}{
		{"shared", "", 200, "SETTLED", []string{"reserve", "settle"}, audit{sent: true, resolvedBy: "gateway", evidence: "response"}, "yes"},
		{"dead", "upstream_unavailable", 502, "CANCELLED", []string{"reserve", "release"}, audit{sent: true, resolvedBy: "gateway", evidence: "not_sent"}, ""},
		{"fake-401", "upstream_auth_failed", 502, "CANCELLED", []string{"reserve", "release"}, audit{sent: true, resolvedBy: "gateway", evidence: "response"}, "no"},
		{"fake-429", "upstream_rate_limited", 503, "CANCELLED", []string{"reserve", "release"}, audit{sent: true, resolvedBy: "gateway", evidence: "response"}, "no"},
		{"status-400", "upstream_bad_request", 400, "CANCELLED", []string{"reserve", "release"}, audit{sent: true, resolvedBy: "gateway", evidence: "response"}, ""},
		{"fake-500", "upstream_error", 502, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true}, "no"},
		{"fake-hang", "upstream_timeout", 504, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true}, "no"},
		{"fake-not-json", "upstream_invalid_response", 502, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true}, "no"},
		{"fake-no-usage", "", 200, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true}, "yes"},
		// 3xx：QF 不跟随重定向；3xx 不保证请求没执行（例如 303），记未知，不释放
		{"redirect", "upstream_invalid_response", 502, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.alias, func(t *testing.T) {
			r := e.send(t, "/v1/chat/completions", `{"model":"`+tc.alias+`"}`)
			if r.status != tc.status || (tc.code != "" && errorCode(t, r) != tc.code) {
				t.Fatalf("got %d %s", r.status, r.body)
			}
			id := requestID(t, r)
			e.expect(t, id, tc.call, tc.kinds, tc.audit)
			if l := e.logLine(t, id); l["outcome"] != tc.call || l["call_status"] != tc.call {
				t.Fatalf("log = %v", l)
			}
			var row *ledger.Proof
			if tc.billed != "" { // 被取消的请求，假上游在连接断开之后才记账
				waitFor(t, func() bool { row = billRow(e.bill(t), id); return row != nil })
			} else {
				row = billRow(e.bill(t), id)
			}
			switch {
			case tc.billed == "" && row != nil, tc.billed != "" && row == nil:
				t.Fatalf("bill row = %+v, want %q", row, tc.billed)
			case tc.billed == "yes" && row.Status != ledger.Settled, tc.billed == "no" && row.Status != ledger.Cancelled:
				t.Fatalf("bill row = %+v, want %q", row, tc.billed)
			}
		})
	}
	dbtest.Verify(t, e.store)
}

// 发出之后客户端断开，上游在超时内返回：照常结算，不写回（日志没有 status、有 client_disconnected），带键时存快照，
// 同一个键再来是重放。
func TestWindowClientGoneUpstreamAnswers(t *testing.T) {
	e := newDBEnv(t, 100000, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan reply, 1)
	body := `{"model":"emb-hold","input":["bounded"]}`
	go func() { done <- e.post(ctx, "/v1/embeddings", e.key, body, "Idempotency-Key", "gone") }()
	waitFor(t, func() bool { return e.fake.Stats().Held == 1 })
	cancel()
	if r := <-done; r.err == nil {
		t.Fatalf("got %d after cancel", r.status)
	}
	stays(t, func() bool { return e.fake.Stats().Canceled == 0 }, 200*time.Millisecond) // 见 TestClientDisconnectDoesNotCancelUpstream
	e.fake.Release()
	var id string
	waitFor(t, func() bool {
		for _, l := range e.logs.lines(t) {
			if l["client_disconnected"] == true {
				id, _ = l["request_id"].(string)
			}
		}
		return id != ""
	})
	l := e.logLine(t, id)
	if l["status"] != nil || l["outcome"] != "SETTLED" || l["call_status"] != "SETTLED" {
		t.Fatalf("log = %v", l)
	}
	c := e.expect(t, id, "SETTLED", []string{"reserve", "settle"}, audit{sent: true, resolvedBy: "gateway", evidence: "response"})
	if c.usageTotal.Int64 != int64(len("bounded")) || c.httpStatus.Int64 != 200 {
		t.Fatalf("usage %v, http %v", c.usageTotal, c.httpStatus)
	}
	r := e.sendKey(t, "/v1/embeddings", "gone", body)
	if r.status != 200 || r.header.Get("Idempotent-Replayed") != "true" || r.header.Get("x-request-id") != id {
		t.Fatalf("retry: %d replayed %q id %q", r.status, r.header.Get("Idempotent-Replayed"), r.header.Get("x-request-id"))
	}
	dbtest.Verify(t, e.store)
}

// 拿到结果之后收尾失败（账本出错，或者别的事务锁着额度行、收尾等到超时）：调用留在 IN_FLIGHT，回收标成未知；
// 日志里有用量和确定的结局，reconcile -log 凭它结算，用量和日志里的一样。
func TestWindowFinishFails(t *testing.T) {
	check := func(t *testing.T, e *dbEnv, id string) {
		t.Helper()
		e.expect(t, id, "IN_FLIGHT", []string{"reserve"}, audit{sent: true})
		l := e.logLine(t, id)
		if l["outcome"] != "SETTLED" || l["call_status"] != "IN_FLIGHT" || l["ledger_error"] == nil {
			t.Fatalf("log = %v", l)
		}
		e.reclaim(t, id)
		e.expect(t, id, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
		proofs, unknown, err := ReadCallLog(strings.NewReader(e.logs.String()))
		if err != nil || unknown != 0 {
			t.Fatalf("ReadCallLog: %v, %d unknown", err, unknown)
		}
		if it := e.reconcileOne(t, proofs, id); it.Verdict != ledger.Settle || !it.Applied {
			t.Fatalf("reconcile = %+v", it)
		}
		c := e.expect(t, id, "SETTLED", []string{"reserve", "settle"}, audit{sent: true, resolvedBy: "reconcile", evidence: "call_log"})
		u, _ := l["usage"].(map[string]any)
		if float64(c.usageTotal.Int64) != u["total_tokens"] || float64(c.usagePrompt.Int64) != u["prompt_tokens"] {
			t.Fatalf("settled %v/%v, log %v", c.usagePrompt, c.usageTotal, u)
		}
		dbtest.Verify(t, e.store)
	}
	t.Run("ledger error", func(t *testing.T) {
		e := newDBEnv(t, 100000, func(l Ledger) Ledger { return &faulty{Ledger: l, finishErr: errors.New("commit: invalid connection")} })
		r := e.send(t, "/v1/chat/completions", `{"model":"shared","messages":[{"role":"user","content":"hi"}]}`)
		if r.status != 200 {
			t.Fatal(r.status)
		}
		check(t, e, requestID(t, r))
	})
	t.Run("budget row locked", func(t *testing.T) {
		e := newDBEnv(t, 100000, nil)
		done := make(chan reply, 1)
		go func() {
			done <- e.post(context.Background(), "/v1/embeddings", e.key, `{"model":"emb-hold","input":["x"]}`)
		}()
		waitFor(t, func() bool { return e.fake.Stats().Held == 1 })
		holder, err := e.admin.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback() }()
		if _, err := holder.Exec(`SELECT 1 FROM budgets b JOIN accounts a ON a.id = b.account_id WHERE a.name = ? FOR UPDATE`, e.account); err != nil {
			t.Fatal(err)
		}
		e.fake.Release()
		r := <-done // 收尾等锁，config.LedgerTimeout 之后失败；上游的 2xx 照样返回
		if r.status != 200 {
			t.Fatal(r.status)
		}
		_ = holder.Rollback()
		check(t, e, requestID(t, r))
	})
}

// 四个被杀点里的三个在进程内模拟（第三个“发出之后、拿到响应之前”在 cmd/quotaforge 里真的杀进程）。
// 每次账本操作都是自己提交的事务，两次提交之间进程不持有打开的事务；在两次提交之间被杀，库里留下的就是最后一次提交的状态，
// 和“停下、不再调用账本”留下的一样。重启后回收，再用假上游的账单对账。
func TestKillPointsSimulated(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	key, err := e.store.KeyByHash(context.Background(), apikey.Hash(e.key))
	if err != nil {
		t.Fatal(err)
	}
	reservation := func() ledger.Reservation {
		return ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: key.AccountID, KeyID: key.ID, Purpose: "chat",
			Alias: "shared", Upstream: "fake", UpstreamModel: "fake-chat", EstInput: 10, EstOutput: 10}
	}
	ctx := context.Background()

	t.Run("after reserve, before the send intent", func(t *testing.T) {
		r := reservation()
		if _, err := e.store.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.reclaim(t, r.CallID)
		e.expect(t, r.CallID, "CANCELLED", []string{"reserve", "release"}, audit{resolvedBy: "reclaim", evidence: "not_sent"})
	})
	t.Run("after the send intent, before sending", func(t *testing.T) {
		r := reservation()
		if _, err := e.store.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := e.store.MarkSent(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.reclaim(t, r.CallID)
		e.expect(t, r.CallID, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
		// 上游从没收到它，账单里没有：对账没有证据，保持未知
		items := e.store.Reconcile(ctx, e.bill(t), true)
		for _, it := range items {
			if it.CallID == r.CallID {
				t.Fatalf("the bill has the call: %+v", it)
			}
		}
		e.expect(t, r.CallID, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
	})
	// 换上游：改道已经提交、还没发给新上游时被杀。回收把它标成未知，上游是新的那个（偏保守：新上游其实没收到）；
	// 用量未知的清单上是新上游，运维向它要账单；它的账单里没有这次调用，对账没有证据，保持未知。
	t.Run("after the reroute, before sending", func(t *testing.T) {
		r := reservation()
		if _, err := e.store.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := e.store.MarkSent(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := e.store.Reroute(ctx, r, "fake-b", "fake-chat"); err != nil {
			t.Fatal(err)
		}
		e.reclaim(t, r.CallID)
		e.expect(t, r.CallID, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
		unknown, err := e.store.UnknownCalls(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(unknown, func(u ledger.UnknownCall) bool { return u.ID == r.CallID })
		if i < 0 || unknown[i].Upstream != "fake-b" || unknown[i].UpstreamModel != "fake-chat" {
			t.Fatalf("unknown calls = %+v", unknown)
		}
		if billRow(e.bill(t), r.CallID) != nil || strings.Contains(string(e.fakeB.Bill()), r.CallID) {
			t.Fatal("no upstream received the call")
		}
	})
	t.Run("after the response, before finishing", func(t *testing.T) {
		killed := newDBEnv(t, 1_000_000, func(l Ledger) Ledger { return &faulty{Ledger: l, finishErr: errors.New("killed")} })
		r := killed.send(t, "/v1/chat/completions", `{"model":"shared","messages":[{"role":"user","content":"abc"}]}`)
		id := requestID(t, r)
		killed.reclaim(t, id)
		killed.expect(t, id, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
		if it := killed.reconcileOne(t, killed.bill(t), id); it.Verdict != ledger.Settle || !it.Applied {
			t.Fatalf("reconcile = %+v", it)
		}
		c := killed.expect(t, id, "SETTLED", []string{"reserve", "settle"}, audit{sent: true, resolvedBy: "reconcile", evidence: "bill"})
		if c.usageTotal.Int64 != 3+10 {
			t.Fatalf("usage total %v, want 13 (the bill)", c.usageTotal)
		}
	})
	dbtest.Verify(t, e.store)
}

func (h *harness) get(path, key string) reply {
	req, err := http.NewRequest(http.MethodGet, h.qf.URL+path, nil)
	if err != nil {
		return reply{err: err}
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, header: resp.Header, body: b, err: err}
}

var timestamp = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$`)

// 调用状态查询：自己账户的调用 200，字段正确；别的账户、不存在、格式不对都是同一个 404；Key 不对 401；账本出错 503；
// 正文里没有请求体、响应体和键的值；查询的日志行不进用量导出。
func TestCallLookup(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	const marker = "lookup-marker-7c1f"
	settled := e.send(t, "/v1/chat/completions", `{"model":"shared","messages":[{"role":"user","content":"`+marker+`"}]}`,
		"Idempotency-Key", "lookup-key-value")
	unknown := e.send(t, "/v1/chat/completions", `{"model":"fake-500"}`)
	emb := e.send(t, "/v1/embeddings", `{"model":"shared","input":["abc"]}`)

	var body map[string]any
	lookup := func(id, key string) reply {
		t.Helper()
		r := e.get("/v1/calls/"+id, key)
		if r.err != nil {
			t.Fatal(r.err)
		}
		body = nil
		_ = json.Unmarshal(r.body, &body)
		return r
	}
	r := lookup(requestID(t, settled), e.key)
	if r.status != 200 || r.header.Get("Content-Type") != "application/json" || body["id"] != requestID(t, settled) ||
		body["object"] != "call" || body["endpoint"] != "chat" || body["model"] != "shared" || body["status"] != "SETTLED" ||
		body["http_status"] != float64(200) || body["error_code"] != nil {
		t.Fatalf("settled: %d %s", r.status, r.body)
	}
	for _, f := range []string{"created_at", "sent_at", "finished_at"} {
		if s, _ := body[f].(string); !timestamp.MatchString(s) {
			t.Fatalf("%s = %v", f, body[f])
		}
	}
	usage, _ := body["usage"].(map[string]any)
	res, _ := body["resolution"].(map[string]any)
	if usage["prompt_tokens"] == nil || usage["completion_tokens"] != float64(10) || res["by"] != "gateway" || res["evidence"] != "response" ||
		!timestamp.MatchString(res["at"].(string)) {
		t.Fatalf("settled usage %v resolution %v", usage, res)
	}
	for _, leak := range []string{marker, "fake reply", "lookup-key-value", e.key} {
		if bytes.Contains(r.body, []byte(leak)) {
			t.Fatalf("the body contains %q", leak)
		}
	}
	if r := lookup(requestID(t, unknown), e.key); r.status != 200 || body["status"] != "USAGE_UNKNOWN" || body["usage"] != nil ||
		body["resolution"] != nil || body["http_status"] != float64(502) || body["error_code"] != "upstream_error" {
		t.Fatalf("unknown: %s", r.body)
	}
	if r := lookup(requestID(t, emb), e.key); r.status != 200 || body["endpoint"] != "embeddings" {
		t.Fatalf("embeddings: %s", r.body)
	} else if u := body["usage"].(map[string]any); u["completion_tokens"] != nil || u["total_tokens"] != float64(3) {
		t.Fatalf("embeddings usage: %v", u)
	}

	// 别的账户的调用、不存在的、格式不对的：同一个 404，正文逐字节相同
	other := *e
	other.account = dbtest.Account(t, e.store, 1000)
	otherKey := other.issue(t, allAliases)
	notFound := []reply{lookup(requestID(t, settled), otherKey), lookup("qf-"+rand.Text(), e.key), lookup("not-a-call-id", e.key)}
	for _, r := range notFound {
		wantCode(t, r, 404, "call_not_found")
		if !bytes.Equal(r.body, notFound[0].body) {
			t.Fatalf("404 bodies differ: %s vs %s", r.body, notFound[0].body)
		}
	}
	wantCode(t, lookup(requestID(t, settled), "qfk_wrong"), 401, "invalid_api_key")

	// 不算一次模型调用：日志行是 lookup，用量导出不导出它
	var lookups int
	for _, l := range e.logs.lines(t) {
		if l["msg"] == "lookup" {
			lookups++
			if l["account"] == nil && l["status"] != float64(401) {
				t.Fatalf("lookup line = %v", l)
			}
		}
	}
	var csv bytes.Buffer
	if _, err := ExportUsage(strings.NewReader(e.logs.String()), &csv); err != nil {
		t.Fatal(err)
	}
	if rows := strings.Count(csv.String(), "\n") - 1; lookups != 7 || rows != 3 {
		t.Fatalf("%d lookup lines, %d exported rows; want 7, 3", lookups, rows)
	}
	if strings.Contains(e.logs.String(), "not-a-call-id") {
		t.Fatal("a malformed call id was logged")
	}
}

func TestCallLookupLedgerDown(t *testing.T) {
	e := newDBEnv(t, 1_000_000, func(l Ledger) Ledger { return &faulty{Ledger: l, lookupErr: errors.New("connection refused")} })
	wantCode(t, e.get("/v1/calls/qf-"+rand.Text(), e.key), 503, "ledger_unavailable")
}

// 幂等和未知用量恢复的衔接。
func TestIdempotencyAfterRecovery(t *testing.T) {
	ctx := context.Background()
	t.Run("in flight is in progress", func(t *testing.T) {
		e := newDBEnv(t, 1_000_000, nil)
		body := `{"model":"emb-hold","input":["x"]}`
		done := make(chan reply, 1)
		go func() { done <- e.post(ctx, "/v1/embeddings", e.key, body, "Idempotency-Key", "k") }()
		waitFor(t, func() bool { return e.fake.Stats().Held == 1 })
		if c := e.call(t, e.latestCall(t)); c.status.String != "IN_FLIGHT" {
			t.Fatalf("held call is %s", c.status.String)
		}
		r := e.sendKey(t, "/v1/embeddings", "k", body)
		wantCode(t, r, 409, "idempotency_in_progress")
		if r.header.Get("Retry-After") != "1" {
			t.Fatalf("Retry-After = %q", r.header.Get("Retry-After"))
		}
		e.fake.Release()
		<-done
	})
	t.Run("released on the bill runs again", func(t *testing.T) {
		e := newDBEnv(t, 1_000_000, nil)
		body := `{"model":"fake-500"}`
		first := e.sendKey(t, "/v1/chat/completions", "k", body)
		wantCode(t, e.sendKey(t, "/v1/chat/completions", "k", body), 409, "idempotency_outcome_unknown")
		if it := e.reconcileOne(t, e.bill(t), requestID(t, first)); it.Verdict != ledger.Release || !it.Applied {
			t.Fatalf("reconcile = %+v", it)
		}
		before := e.upstreamCalls()
		again := e.sendKey(t, "/v1/chat/completions", "k", body)
		if again.status != 502 || requestID(t, again) == requestID(t, first) || e.upstreamCalls() != before+1 {
			t.Fatalf("after release: %d, upstream %d → %d", again.status, before, e.upstreamCalls())
		}
		dbtest.Verify(t, e.store)
	})
	t.Run("settled with a snapshot replays", func(t *testing.T) {
		e := newDBEnv(t, 1_000_000, nil)
		body := `{"model":"fake-no-usage"}` // 2xx 缺 usage：有快照、用量未知
		first := e.sendKey(t, "/v1/chat/completions", "k", body)
		if it := e.reconcileOne(t, e.bill(t), requestID(t, first)); it.Verdict != ledger.Settle || !it.Applied {
			t.Fatalf("reconcile = %+v", it)
		}
		wantReplay(t, e.sendKey(t, "/v1/chat/completions", "k", body), first)
		dbtest.Verify(t, e.store)
	})
	t.Run("settled without a snapshot does not run again", func(t *testing.T) {
		e := newDBEnv(t, 1_000_000, nil)
		body := `{"model":"fake-billed-hang"}` // 上游照样计费，QF 超时，没有响应
		first := e.sendKey(t, "/v1/chat/completions", "k", body)
		wantCode(t, first, 504, "upstream_timeout")
		if it := e.reconcileOne(t, e.bill(t), requestID(t, first)); it.Verdict != ledger.Settle || !it.Applied {
			t.Fatalf("reconcile = %+v", it)
		}
		before := e.upstreamCalls()
		wantCode(t, e.sendKey(t, "/v1/chat/completions", "k", body), 409, "idempotency_response_unavailable")
		if e.upstreamCalls() != before {
			t.Fatal("ran again")
		}
		dbtest.Verify(t, e.store)
	})
	t.Run("late result stores the snapshot", func(t *testing.T) {
		e := newDBEnv(t, 1_000_000, nil)
		body := `{"model":"emb-hold","input":["late"]}`
		done := make(chan reply, 1)
		go func() { done <- e.post(ctx, "/v1/embeddings", e.key, body, "Idempotency-Key", "k") }()
		waitFor(t, func() bool { return e.fake.Stats().Held == 1 })
		id := e.latestCall(t)
		e.reclaim(t, id) // 实例卡住超过上界：回收先标未知
		e.expect(t, id, "USAGE_UNKNOWN", []string{"reserve"}, audit{sent: true})
		e.fake.Release()
		first := <-done
		if first.status != 200 {
			t.Fatal(first.status)
		}
		e.expect(t, id, "SETTLED", []string{"reserve", "settle"}, audit{sent: true, resolvedBy: "gateway", evidence: "late_response"})
		wantReplay(t, e.sendKey(t, "/v1/embeddings", "k", body), first)
		dbtest.Verify(t, e.store)
	})
}

// 日志当证据：只采用结局确定的调用行；结局未知的只计数；别的行跳过；格式不对整份不用。
func TestReadCallLog(t *testing.T) {
	s, c, u := "qf-"+rand.Text(), "qf-"+rand.Text(), "qf-"+rand.Text()
	good := strings.Join([]string{
		`{"msg":"listening","addr":"0.0.0.0:8080"}`,
		`{"msg":"call","request_id":"` + "qf-" + rand.Text() + `","status":401,"error_code":"invalid_api_key"}`,
		`{"msg":"lookup","request_id":"` + "qf-" + rand.Text() + `","status":200}`,
		`{"msg":"call","request_id":"` + s + `","outcome":"SETTLED","call_status":"IN_FLIGHT","usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}`,
		``,
		`{"msg":"call","request_id":"` + c + `","outcome":"CANCELLED","call_status":"RESERVED"}`,
		`{"msg":"call","request_id":"` + u + `","outcome":"USAGE_UNKNOWN","usage":"unknown"}`,
		// 缓存命中：写入时就已结清、从没发给上游，不是任何调用的证据，跳过
		`{"msg":"call","request_id":"` + "qf-" + rand.Text() + `","outcome":"CACHED","call_status":"CACHED","cache":"hit"}`,
	}, "\n")
	proofs, unknown, err := ReadCallLog(strings.NewReader(good))
	if err != nil || unknown != 1 || len(proofs) != 2 {
		t.Fatalf("ReadCallLog = %+v, %d, %v", proofs, unknown, err)
	}
	if p := proofs[0]; p.CallID != s || p.Status != ledger.Settled || p.Source != ledger.CallLog || p.Line != 4 ||
		*p.Usage != (ledger.Usage{Prompt: 5, Completion: 10, Total: 15, HasCompletion: true}) {
		t.Fatalf("settled proof = %+v", p)
	}
	if p := proofs[1]; p.CallID != c || p.Status != ledger.Cancelled || p.Usage != nil {
		t.Fatalf("cancelled proof = %+v", p)
	}
	for name, log := range map[string]string{
		"not json":            "not json",
		"settled no usage":    `{"msg":"call","request_id":"` + s + `","outcome":"SETTLED","usage":"unknown"}`,
		"unknown outcome":     `{"msg":"call","request_id":"` + s + `","outcome":"DONE"}`,
		"not a call id":       `{"msg":"call","request_id":"req-1","outcome":"CANCELLED"}`,
		"twice the same call": `{"msg":"call","request_id":"` + c + `","outcome":"CANCELLED"}` + "\n" + `{"msg":"call","request_id":"` + c + `","outcome":"CANCELLED"}`,
	} {
		if _, _, err := ReadCallLog(strings.NewReader(log)); !errors.Is(err, ledger.ErrBadEvidence) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
