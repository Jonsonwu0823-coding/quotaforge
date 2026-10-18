// Package usageevents 把账本里结清调用的用量事件发到消息队列（Kafka），再由消费者按天汇总。
//
// 投递是至少一次：转发在发成功之后、标记之前死了，事件会再发一遍；消费者在写库之后、提交位点之前死了，会再收到一次。
// 正确性靠消费者按调用号去重（ledger.ApplyUsageEvent，去重和汇总在同一个事务里），不靠生产者的幂等：
// 生产者幂等只让 Kafka 里的重复少一些。SQL 都在 ledger 里；这里是编解码、两个循环和 Kafka 的适配。
package usageevents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// Version 是事件格式的版本；消费者只认这一个，别的都当格式不对。改字段的含义或去掉字段要换版本。
const Version = 1

// timeLayout 是结清时间的格式：UTC、微秒、带 Z，和账本里的 DATETIME(6) 一一对应。
const timeLayout = "2006-01-02T15:04:05.000000Z"

// wire 是消息的正文（JSON）。只有汇总和对账要用的字段：没有 Key、请求和响应正文、客户端给的请求号和 run。
type wire struct {
	V             int        `json:"v"`
	CallID        string     `json:"call_id"`
	AccountID     int64      `json:"account_id"`
	Purpose       string     `json:"purpose"`
	ModelAlias    string     `json:"model_alias"`
	Upstream      string     `json:"upstream"`
	UpstreamModel string     `json:"upstream_model"`
	Status        string     `json:"status"`
	Usage         *wireUsage `json:"usage"`
	ResolvedAt    string     `json:"resolved_at"`
	CachedFrom    *string    `json:"cached_from"`
}

type wireUsage struct {
	Prompt     int64  `json:"prompt_tokens"`
	Completion *int64 `json:"completion_tokens,omitempty"` // 嵌入没有
	Total      int64  `json:"total_tokens"`
}

// Encode 是一个事件的消息正文。
func Encode(e ledger.UsageEvent) []byte {
	w := wire{V: Version, CallID: e.CallID, AccountID: e.AccountID, Purpose: e.Purpose, ModelAlias: e.Alias,
		Upstream: e.Upstream, UpstreamModel: e.UpstreamModel, Status: string(e.Status),
		ResolvedAt: e.ResolvedAt.UTC().Format(timeLayout)}
	if u := e.Usage; u != nil {
		w.Usage = &wireUsage{Prompt: u.Prompt, Total: u.Total}
		if u.HasCompletion {
			w.Usage.Completion = &u.Completion
		}
	}
	if e.CachedFrom != "" {
		w.CachedFrom = &e.CachedFrom
	}
	b, _ := json.Marshal(w) // 只有字符串和整数，不会失败
	return b
}

// ErrMalformed 表示消息不是这个版本的合法事件。
var ErrMalformed = errors.New("malformed usage event")

// Decode 解析并校验一条消息（消息队列是信任边界）：版本、调用号的格式、字段齐全、状态是终态、用量合法、时间是 UTC。
// 不认识的字段也算格式不对。
func Decode(b []byte) (ledger.UsageEvent, error) {
	var w wire
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return ledger.UsageEvent{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	e := ledger.UsageEvent{CallID: w.CallID, AccountID: w.AccountID, Purpose: w.Purpose, Alias: w.ModelAlias,
		Upstream: w.Upstream, UpstreamModel: w.UpstreamModel, Status: ledger.Status(w.Status)}
	if w.Usage != nil {
		e.Usage = &ledger.Usage{Prompt: w.Usage.Prompt, Total: w.Usage.Total, HasCompletion: w.Usage.Completion != nil}
		if w.Usage.Completion != nil {
			e.Usage.Completion = *w.Usage.Completion
		}
	}
	if w.CachedFrom != nil {
		e.CachedFrom = *w.CachedFrom
	}
	var err error
	e.ResolvedAt, err = time.Parse(timeLayout, w.ResolvedAt)
	if reason := invalid(w, e, err); reason != "" {
		return ledger.UsageEvent{}, fmt.Errorf("%w: %s", ErrMalformed, reason)
	}
	return e, nil
}

// invalid 说明事件哪里不合法；合法时返回空串。
func invalid(w wire, e ledger.UsageEvent, timeErr error) string {
	switch {
	case w.V != Version:
		return fmt.Sprintf("version %d", w.V)
	case !ledger.ValidCallID(e.CallID): // 不合格式的调用号写不进去重表，消费者会一直重试它、卡住整个分区
		return "call_id"
	case e.AccountID <= 0 || e.Alias == "" || e.Upstream == "" || e.UpstreamModel == "":
		return "missing fields"
	case e.Purpose != "chat" && e.Purpose != "embeddings":
		return "purpose"
	case e.Status != ledger.Settled && e.Status != ledger.Cancelled && e.Status != ledger.Cached:
		return "status"
	case timeErr != nil:
		return "resolved_at"
	case e.Usage == nil && e.Status != ledger.Cancelled, e.Usage != nil && !e.Usage.Valid(e.Purpose):
		return "usage"
	case (e.CachedFrom != "") != (e.Status == ledger.Cached):
		return "cached_from"
	}
	return ""
}
