package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// callBody 是调用状态查询的正文。没有请求体、响应体、快照、幂等键和 Key。库里是 NULL 的字段写 null。
type callBody struct {
	ID         string          `json:"id"`
	Object     string          `json:"object"`
	Endpoint   string          `json:"endpoint"`
	Model      string          `json:"model"`
	Status     ledger.Status   `json:"status"`
	CreatedAt  string          `json:"created_at"`
	SentAt     *string         `json:"sent_at"`
	FinishedAt *string         `json:"finished_at"`
	Usage      *usageBody      `json:"usage"`
	HTTPStatus *int            `json:"http_status"`
	ErrorCode  *string         `json:"error_code"`
	Resolution *resolutionBody `json:"resolution"`
}

type usageBody struct {
	Prompt     int64  `json:"prompt_tokens"`
	Completion *int64 `json:"completion_tokens,omitempty"` // 嵌入没有
	Total      int64  `json:"total_tokens"`
}

// resolutionBody 是谁、凭什么、什么时候结清的；没结清时整个是 null。
type resolutionBody struct {
	By       ledger.Actor    `json:"by"`
	Evidence ledger.Evidence `json:"evidence"`
	At       string          `json:"at"`
}

// lookup 是 GET /v1/calls/{id}：按 QF 的调用号查这次调用的结局。鉴权和转发接口一样；调用必须属于这把 Key 的账户，
// 属于别的账户、不存在、调用号格式不对都是同一个 404，不透露调用是否存在。不预留、不调上游，日志也不算一次调用。
func (g *gateway) lookup(w http.ResponseWriter, r *http.Request) {
	c := &call{id: r.Context().Value(requestIDKey{}).(string), start: time.Now()}
	id := r.PathValue("id")
	defer g.logLookup(c, id)
	if e := g.authenticate(r, c); e != nil {
		writeError(w, c, e)
		return
	}
	if !ledger.ValidCallID(id) {
		writeError(w, c, errCallNotFound)
		return
	}
	ctx, cancel := ledgerContext(r.Context())
	defer cancel()
	info, err := g.ledger.Call(ctx, c.key.AccountID, id)
	switch {
	case err != nil:
		c.ledgerError = err.Error()
		writeError(w, c, errLedgerUnavailable)
	case info == nil:
		writeError(w, c, errCallNotFound)
	default:
		c.status = http.StatusOK
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(newCallBody(info))
	}
}

func newCallBody(c *ledger.CallInfo) callBody {
	b := callBody{ID: c.ID, Object: "call", Endpoint: c.Purpose, Model: c.Alias, Status: c.Status, CreatedAt: c.CreatedAt,
		SentAt: nonEmpty(c.SentAt), FinishedAt: nonEmpty(c.FinishedAt), ErrorCode: nonEmpty(c.ErrorCode)}
	if u := c.Usage; u != nil {
		b.Usage = &usageBody{Prompt: u.Prompt, Total: u.Total}
		if u.HasCompletion {
			b.Usage.Completion = &u.Completion
		}
	}
	if c.HTTPStatus != 0 {
		b.HTTPStatus = &c.HTTPStatus
	}
	if c.ResolvedBy != "" {
		b.Resolution = &resolutionBody{By: c.ResolvedBy, Evidence: c.Evidence, At: c.ResolvedAt}
	}
	return b
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// logLookup 每次查询写一行，msg 是 lookup，不是 call：usage-export 和 reconcile -log 都不把它当成一次模型调用。
// 被查的调用号只记格式合格的。
func (g *gateway) logLookup(c *call, id string) {
	attrs := []slog.Attr{
		slog.String("request_id", c.id),
		slog.String("endpoint", "calls"),
		slog.Int64("duration_ms", time.Since(c.start).Milliseconds()),
	}
	if ledger.ValidCallID(id) {
		attrs = append(attrs, slog.String("call_id", id))
	}
	if c.status != 0 {
		attrs = append(attrs, slog.Int("status", c.status))
	}
	if c.key != nil {
		attrs = append(attrs, slog.Int64("key_id", c.key.ID), slog.String("account", c.key.Account))
	}
	if c.errorCode != "" {
		attrs = append(attrs, slog.String("error_code", c.errorCode))
	}
	if c.ledgerError != "" {
		attrs = append(attrs, slog.String("ledger_error", c.ledgerError))
	}
	g.log.LogAttrs(context.Background(), slog.LevelInfo, "lookup", attrs...)
}
