package ledger_test

import (
	"crypto/rand"
	"errors"
	"slices"
	"testing"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// settledEmbedding 是一次结算了的嵌入调用（用量 7，没有 completion_tokens）：缓存命中要指向的原来那次。
func (f *fixture) settledEmbedding(t *testing.T) ledger.Reservation {
	t.Helper()
	r := f.reservation(20)
	r.Purpose = "embeddings"
	if _, err := f.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.finish(t, r, ledger.Settled, 7)
	return r
}

// cachedCall 是一次指向 from 的缓存命中，用量和 from 相同。
func (f *fixture) cachedCall(from string) ledger.CachedCall {
	return ledger.CachedCall{
		CallID: "qf-" + rand.Text(), AccountID: f.key.AccountID, KeyID: f.key.ID, Purpose: "embeddings", Alias: "m",
		Upstream: "u", UpstreamModel: "um", ClientRequestID: "crid", CachedFrom: from,
		Usage: ledger.Usage{Prompt: 7, Total: 7}, ResponseID: "resp-1", ResponseModel: "um-v1",
	}
}

func (f *fixture) recordCached(t *testing.T, from string) ledger.CachedCall {
	t.Helper()
	c := f.cachedCall(from)
	if err := f.s.RecordCached(ctx, c); err != nil {
		t.Fatal(err)
	}
	return c
}

// 一次命中只写一行 CACHED：预留量 0、没有账本行、不动额度，写入时就已结清（gateway、cache），没有 sent_at。
// 查询接口查得到它；Verify 干净。
func TestRecordCached(t *testing.T) {
	f := newFixture(t, 1000)
	from := f.settledEmbedding(t)
	_, reserved, consumed := f.budget(t)
	c := f.recordCached(t, from.CallID)

	if _, r2, c2 := f.budget(t); r2 != reserved || c2 != consumed {
		t.Fatalf("budget moved: reserved %d -> %d, consumed %d -> %d", reserved, r2, consumed, c2)
	}
	st := f.state(t, c.CallID)
	if st.status != "CACHED" || st.resolvedBy != "gateway" || st.evidence != "cache" || !st.resolvedAt || !st.finished ||
		st.sent || st.httpStatus.Int64 != 200 || len(st.kinds) != 0 ||
		st.usage[0].Int64 != 7 || st.usage[1].Valid || st.usage[2].Int64 != 7 {
		t.Fatalf("cached call row = %+v", st)
	}
	var est, amount int64
	var from2, upstream, model, respID, respModel, crid string
	if err := f.admin.QueryRow(`SELECT est_input_tokens + est_output_tokens, reserved_tokens, cached_from, upstream, upstream_model,
			upstream_response_id, response_model, client_request_id FROM calls WHERE id = ?`, c.CallID).Scan(
		&est, &amount, &from2, &upstream, &model, &respID, &respModel, &crid); err != nil {
		t.Fatal(err)
	}
	if est != 0 || amount != 0 || from2 != from.CallID || upstream != "u" || model != "um" || respID != "resp-1" ||
		respModel != "um-v1" || crid != "crid" {
		t.Fatalf("columns: est %d, reserved %d, cached_from %s, upstream %s/%s, response %s/%s, crid %s",
			est, amount, from2, upstream, model, respID, respModel, crid)
	}
	info, err := f.s.Call(ctx, f.key.AccountID, c.CallID)
	if err != nil || info == nil {
		t.Fatalf("Call = %v, %v", info, err)
	}
	if info.Status != ledger.Cached || info.ResolvedBy != ledger.Gateway || info.Evidence != ledger.Cache || info.SentAt != "" ||
		info.HTTPStatus != 200 || info.Usage == nil || info.Usage.Total != 7 || info.Usage.HasCompletion {
		t.Fatalf("Call = %+v", info)
	}
	if got := rules(t, f); len(got) != 0 {
		t.Fatalf("Verify: %v", got)
	}
}

// 改坏 CACHED（或者给别的调用写上 cached_from），Verify 都能报出对应的规则。
func TestVerifyFindsCachedTampering(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(t *testing.T, f *fixture, cached, from string) []string
		want   []string
	}{
		{"a ledger entry", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES ('" + cached + "', 'reserve', 0, 0, UTC_TIMESTAMP(6))"}
		}, []string{"cached"}},
		{"a reserved amount", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET reserved_tokens = 5, est_input_tokens = 5 WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"sent", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET sent_at = UTC_TIMESTAMP(6) WHERE id = '" + cached + "'"}
		}, []string{"sent"}},
		{"unknown usage", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET usage_prompt_tokens = NULL, usage_total_tokens = NULL WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"not a 200", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET http_status = 500 WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"other evidence", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET resolution_evidence = 'response' WHERE id = '" + cached + "'"}
		}, []string{"resolution", "sent"}}, // 凭上游回答结清的一定发出过
		{"an idempotency key", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET idempotency_key = 'k' WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"no origin", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET cached_from = 'qf-NOSUCHCALLAAAAAAAAAAAAAAAA' WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"no cached_from", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET cached_from = NULL WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"origin in another account", func(t *testing.T, _ *fixture, cached, _ string) []string {
			other := newFixture(t, 1000)
			return []string{"UPDATE calls SET cached_from = '" + other.settledEmbedding(t).CallID + "' WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"origin not settled", func(t *testing.T, f *fixture, cached, _ string) []string {
			r := f.reserve(t, 10)
			return []string{"UPDATE calls SET cached_from = '" + r.CallID + "' WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"different usage", func(_ *testing.T, _ *fixture, cached, _ string) []string {
			return []string{"UPDATE calls SET usage_prompt_tokens = 8, usage_total_tokens = 8 WHERE id = '" + cached + "'"}
		}, []string{"cached"}},
		{"cached_from on a settled call", func(_ *testing.T, _ *fixture, _, from string) []string {
			return []string{"UPDATE calls SET cached_from = '" + from + "' WHERE id = '" + from + "'"}
		}, []string{"cached"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 1000)
			from := f.settledEmbedding(t)
			c := f.recordCached(t, from.CallID)
			if got := rules(t, f); len(got) != 0 {
				t.Fatalf("before tampering: %v", got)
			}
			exec(t, f.admin, tc.tamper(t, f, c.CallID, from.CallID)...)
			if got := rules(t, f); !slices.Equal(got, tc.want) {
				t.Errorf("rules = %v, want %v", got, tc.want)
			}
			deleteAccount(t, f) // 改坏的账户不留给后面跑 Verify 的测试
		})
	}
}

// 回收、对账、未知用量的清单、收尾都不碰 CACHED：它不占预留、从没发给上游。
func TestCachedIsLeftAlone(t *testing.T) {
	f := newFixture(t, 1000)
	from := f.settledEmbedding(t)
	c := f.recordCached(t, from.CallID)
	if _, err := f.admin.Exec("UPDATE calls SET created_at = UTC_TIMESTAMP(6) - INTERVAL 1 HOUR WHERE id = ?", c.CallID); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot(t)

	f.reclaim(t)
	items := f.s.Reconcile(ctx, []ledger.Proof{
		{CallID: c.CallID, Status: ledger.Settled, Usage: &ledger.Usage{Prompt: 7, Total: 7}, Source: ledger.Bill},
		{CallID: c.CallID, Status: ledger.Cancelled, Source: ledger.CallLog},
	}, true)
	for _, it := range items {
		if it.Verdict != ledger.Inconsistent || it.Applied || it.Before != ledger.Cached {
			t.Errorf("reconcile %s = %+v, want inconsistent and not applied", it.Proof, it)
		}
	}
	unknown, err := f.s.UnknownCalls(ctx, f.account)
	if err != nil || len(unknown) != 0 {
		t.Errorf("UnknownCalls = %v, %v", unknown, err)
	}
	r := ledger.Reservation{CallID: c.CallID, AccountID: f.key.AccountID}
	for _, res := range []ledger.Result{
		{Status: ledger.Settled, Evidence: ledger.Answered, Usage: &ledger.Usage{Prompt: 1, Total: 1}, HTTPStatus: 200},
		{Status: ledger.Cancelled, Evidence: ledger.NotSent},
		{Status: ledger.UsageUnknown},
	} {
		if err := f.s.Finish(ctx, r, res); !errors.Is(err, ledger.ErrNotAllowed) {
			t.Errorf("Finish(%s) on a CACHED call = %v, want ErrNotAllowed", res.Status, err)
		}
	}
	if err := f.s.MarkSent(ctx, r); !errors.Is(err, ledger.ErrNotReserved) {
		t.Errorf("MarkSent on a CACHED call = %v", err)
	}
	if after := f.snapshot(t); after != before {
		t.Fatalf("CACHED call changed:\n%s\nvs\n%s", before, after)
	}
	dbtest.Verify(t, f.s)
}

// unmigrate5 回到版本 4 的表结构：两个 ENUM 去掉末尾的值，去掉 cached_from。
var unmigrate5 = []string{
	`ALTER TABLE calls DROP COLUMN cached_from,
		MODIFY COLUMN status ENUM('RESERVED', 'SETTLED', 'CANCELLED', 'USAGE_UNKNOWN', 'IN_FLIGHT') NOT NULL,
		MODIFY COLUMN resolution_evidence ENUM('response', 'not_sent', 'late_response', 'call_log', 'bill') NULL`,
	"DELETE FROM schema_migrations WHERE version = 5",
}

// 迁移 0005：版本 4 的库里已有的调用原样保留，迁移后能写 CACHED、Verify 干净；再迁移一遍什么都不做。
func TestMigration5Cached(t *testing.T) {
	dropAll(t)
	dsn := dbtest.MigrateDSN(t)
	if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, 1000)
	from := f.settledEmbedding(t)
	exec(t, f.admin, unmigrate5...)
	before := f.snapshot(t)
	for range 2 {
		from5, to, err := ledger.Migrate(ctx, dsn)
		if err != nil || to != 5 {
			t.Fatalf("Migrate = %d, %d, %v", from5, to, err)
		}
	}
	wantAllVersions(t)
	if after := f.snapshot(t); after != before {
		t.Fatalf("existing rows changed by the migration:\n%s\nvs\n%s", before, after)
	}
	f.recordCached(t, from.CallID)
	dbtest.Verify(t, f.s)
}
