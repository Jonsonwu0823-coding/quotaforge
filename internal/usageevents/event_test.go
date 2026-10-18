package usageevents

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

func settledChat() ledger.UsageEvent {
	return ledger.UsageEvent{CallID: "qf-AAAAAAAAAAAAAAAAAAAAAAAAAA", AccountID: 7, Purpose: "chat", Alias: "m", Upstream: "u", UpstreamModel: "um",
		Status: ledger.Settled, Usage: &ledger.Usage{Prompt: 10, Completion: 3, Total: 13, HasCompletion: true},
		ResolvedAt: time.Date(2026, 10, 16, 23, 59, 59, 999999000, time.UTC)}
}

// 消息的格式是固定的：字段名、版本号、UTC 微秒的时间；没有用量是 null，嵌入没有 completion_tokens。
func TestEncode(t *testing.T) {
	want := `{"v":1,"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA","account_id":7,"purpose":"chat","model_alias":"m","upstream":"u","upstream_model":"um",` +
		`"status":"SETTLED","usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13},` +
		`"resolved_at":"2026-10-16T23:59:59.999999Z","cached_from":null}`
	if got := string(Encode(settledChat())); got != want {
		t.Fatalf("Encode =\n%s\nwant\n%s", got, want)
	}
	cancelled := settledChat()
	cancelled.Status, cancelled.Usage = ledger.Cancelled, nil
	if got := string(Encode(cancelled)); !strings.Contains(got, `"usage":null`) {
		t.Fatalf("cancelled: %s", got)
	}
}

// 编码再解码得到同一个事件：结算（chat、嵌入）、缓存命中、取消；本地时区不影响。
func TestRoundTrip(t *testing.T) {
	defer func(l *time.Location) { time.Local = l }(time.Local)
	time.Local = time.FixedZone("UTC+8", 8*3600)
	embedding := settledChat()
	embedding.Purpose, embedding.Usage = "embeddings", &ledger.Usage{Prompt: 7, Total: 7}
	cached := embedding
	cached.Status, cached.CachedFrom = ledger.Cached, "qf-BBBBBBBBBBBBBBBBBBBBBBBBBB"
	cancelled := settledChat()
	cancelled.Status, cancelled.Usage = ledger.Cancelled, nil
	for _, e := range []ledger.UsageEvent{settledChat(), embedding, cached, cancelled} {
		got, err := Decode(Encode(e))
		if err != nil || !reflect.DeepEqual(got, e) {
			t.Fatalf("round trip of %+v: %+v, %v", e, got, err)
		}
		if got.Day() != "2026-10-16" {
			t.Fatalf("day = %s", got.Day())
		}
	}
}

// 用量正好在上限（10^12）的事件照常解码。
func TestDecodeUsageAtTheLimit(t *testing.T) {
	e := settledChat()
	e.Usage = &ledger.Usage{Prompt: maxEventTokens, Completion: maxEventTokens, Total: maxEventTokens, HasCompletion: true}
	if got, err := Decode(Encode(e)); err != nil || !reflect.DeepEqual(got, e) {
		t.Fatalf("Decode = %+v, %v", got, err)
	}
}

// 消息队列是信任边界：版本不对、调用号不合格式（太长、不是 ASCII、小写）、缺字段、不认识的字段、不是终态、时间不是 UTC、
// 用量和状态对不上、用量超过上限，都是格式不对。
func TestDecodeRejects(t *testing.T) {
	good := string(Encode(settledChat()))
	for name, body := range map[string]string{
		"not json":                  "{",
		"version 2":                 strings.Replace(good, `"v":1`, `"v":2`, 1),
		"no version":                strings.Replace(good, `"v":1,`, ``, 1),
		"no call id":                strings.Replace(good, `"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA"`, `"call_id":""`, 1),
		"call id too long":          strings.Replace(good, `"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA"`, `"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAAA"`, 1),
		"call id not ascii":         strings.Replace(good, `"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA"`, `"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAé"`, 1),
		"call id lower case":        strings.Replace(good, `"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA"`, `"call_id":"qf-aaaaaaaaaaaaaaaaaaaaaaaaaa"`, 1),
		"no account":                strings.Replace(good, `"account_id":7`, `"account_id":0`, 1),
		"unknown field":             strings.Replace(good, `"v":1`, `"v":1,"run_id":"r"`, 1),
		"purpose":                   strings.Replace(good, `"chat"`, `"images"`, 1),
		"not resolved":              strings.Replace(good, `"SETTLED"`, `"USAGE_UNKNOWN"`, 1),
		"offset":                    strings.Replace(good, `59.999999Z`, `59.999999+08:00`, 1),
		"no microseconds":           strings.Replace(good, `59.999999Z`, `59Z`, 1),
		"settled without usage":     strings.Replace(good, `"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}`, `"usage":null`, 1),
		"chat without completion":   strings.Replace(good, `"completion_tokens":3,`, ``, 1),
		"negative usage":            strings.Replace(good, `"total_tokens":13`, `"total_tokens":-1`, 1),
		"prompt over the limit":     strings.Replace(good, `"prompt_tokens":10`, `"prompt_tokens":1000000000001`, 1),
		"completion over the limit": strings.Replace(good, `"completion_tokens":3`, `"completion_tokens":1000000000001`, 1),
		"total over the limit":      strings.Replace(good, `"total_tokens":13`, `"total_tokens":1000000000001`, 1),
		"cached_from on settled":    strings.Replace(good, `"cached_from":null`, `"cached_from":"qf-B"`, 1),
		"cached without origin":     strings.Replace(good, `"SETTLED"`, `"CACHED"`, 1),
	} {
		if _, err := Decode([]byte(body)); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
}
