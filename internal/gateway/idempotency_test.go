package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// --- 规范化和指纹（不用库）---

func TestCanonicalJSON(t *testing.T) {
	same := [][2]string{
		{`{"b":1,"a":2}`, ` { "a" : 2 ,` + "\n\t" + `"b" : 1 } `},                                      // 键的顺序、空白
		{`{"s":"\u0041\n\/\u00e9<>&"}`, `{"s":"A\u000a/é\u003c\u003e\u0026"}`},                         // 字符串的转义
		{`{"a":[1,{"y":true,"x":null}]}`, `{"a":[1,{"x":null,"y":true}]}`},                             // 嵌套对象也排序
		{`{"s":"\ud83d\ude00"}`, `{"s":"😀"}`},                                                          // 成对的代理项
		{`{"s":"\\ud800"}`, `{"s":"\u005cud800"}`},                                                     // 转义的反斜杠，不是代理项
		{`{"k\u0062":1}`, `{"kb":1}`},                                                                  // 键也解码再编码
		{`{"a":1,"a":2,"b":0}`, `{"b":0,"a":1,"a":2}`},                                                 // 重复的键保持原来的先后
		{`{"B":1,"a":2}`, `{"a":2,"B":1}`},                                                             // 折叠后不同的键照常排序
		{`{"x":1,"Temperature":0.1,"temperature":0.9}`, `{"Temperature":0.1,"temperature":0.9,"x":1}`}, // 折叠后相同的键，先后不变就相同
	}
	for _, p := range same {
		a, errA := canonicalJSON([]byte(p[0]))
		b, errB := canonicalJSON([]byte(p[1]))
		if errA != nil || errB != nil || !bytes.Equal(a, b) {
			t.Errorf("%s and %s: %s / %s (%v, %v), want equal", p[0], p[1], a, b, errA, errB)
		}
	}
	different := [][2]string{
		{`{"a":[1,2]}`, `{"a":[2,1]}`}, // 数组保持顺序
		{`{"n":1}`, `{"n":1.0}`},       // 数字按字面
		{`{"n":1e2}`, `{"n":100}`},     //
		{`{"n":0}`, `{"n":-0}`},        //
		{`{"seed":12345678901234567890123}`, `{"seed":12345678901234567890124}`},           // 大整数不失真
		{`{"seed":9007199254740993}`, `{"seed":9007199254740992}`},                         // 转成 float64 会相等
		{`{"a":1,"a":2}`, `{"a":2}`},                                                       // 重复的键不去重
		{`{"a":1,"a":2}`, `{"a":2,"a":1}`},                                                 // 重复的键先后不同
		{`{"Temperature":0.1,"temperature":0.9}`, `{"temperature":0.9,"Temperature":0.1}`}, // 只差大小写的键换了先后
		{`{"stream":false,"ſtream":true}`, `{"ſtream":true,"stream":false}`},               // Unicode 折叠相同（ſ 和 s）
		{`{"max_toKens":1,"max_tokens":9}`, `{"max_tokens":9,"max_toKens":1}`},             // Unicode 折叠相同（开尔文符号和 k）
		{`{"n":1}`, `{"n":"1"}`},                                                           //
		{`{"x":null}`, `{}`},                                                               //
		{`{"b":true}`, `{"b":"true"}`},                                                     //
	}
	for _, p := range different {
		a, errA := canonicalJSON([]byte(p[0]))
		b, errB := canonicalJSON([]byte(p[1]))
		if errA != nil || errB != nil || bytes.Equal(a, b) {
			t.Errorf("%s and %s: %s / %s (%v, %v), want different", p[0], p[1], a, b, errA, errB)
		}
	}
	// 规范形式本身：按折叠后的键排序，折叠后相同的（重复的 a，只差大小写的 b 和 B）保持原来的先后，数字原样，没有空白。
	got, err := canonicalJSON([]byte(`{ "b" : [ 1.50 , 2e3 ], "a" : "x", "a" : -0.0, "B": "é" }`))
	if want := `{"a":"x","a":-0.0,"b":[1.50,2e3],"B":"é"}`; err != nil || string(got) != want {
		t.Errorf("canonical = %s, %v, want %s", got, err, want)
	}
	for _, bad := range []string{
		"{\"s\":\"\xff\"}",           // 非法 UTF-8
		"{\"s\":\"\xed\xa0\x80\"}",   // UTF-8 编码的代理项也是非法 UTF-8
		`{"s":"\ud800"}`,             // 孤立的高位
		`{"s":"\udc00"}`,             // 孤立的低位
		`{"s":"\ud800x"}`,            // 高位后面不是转义
		`{"s":"\ud800\u0041"}`,       // 高位后面不是低位
		`{"s":"\ud800\ud800\udc00"}`, // 两个高位
		`{"\udfff":1}`,               // 键里也算
		`{"s":"a"} {}`,               // 结尾多出东西
	} {
		if c, err := canonicalJSON([]byte(bad)); err == nil {
			t.Errorf("%q: canonical %s, want an error", bad, c)
		}
	}
}

// 指纹用客户端发来的原始请求体（改写 model 之前的）：别名 shared 在上游叫 fake-chat，改写前后的请求体不同。
func TestFingerprintUsesOriginalBody(t *testing.T) {
	stub := newAllowAll()
	h := newHarnessWith(t, stub)
	body := `{"model":"shared","messages":[{"role":"user","content":"hi"}]}`
	if r := h.chat(t, body, "Idempotency-Key", "k"); r.status != 200 {
		t.Fatal(r.status)
	}
	original, _ := fingerprint(config.Chat, []byte(body))
	rewritten, _ := fingerprint(config.Chat, []byte(strings.Replace(body, `"shared"`, `"fake-chat"`, 1)))
	if got := stub.lastReservation(t).Fingerprint; !bytes.Equal(got, original) || bytes.Equal(got, rewritten) {
		t.Fatalf("fingerprint %x, want %x (original body), not %x (rewritten)", got, original, rewritten)
	}
}

// 成员多于 12 个时（Go 的排序对更短的切片用插入排序，本身就稳定），折叠后相同的键仍保持原来的先后。
func TestCanonicalJSONManyDuplicateKeys(t *testing.T) {
	var body, a, b []string
	for i := range 30 { // 交错的 b、a 各 30 个，值各不相同
		body = append(body, fmt.Sprintf(`"b":%d`, i), fmt.Sprintf(`"a":%d`, i))
		a, b = append(a, fmt.Sprintf(`"a":%d`, i)), append(b, fmt.Sprintf(`"b":%d`, i))
	}
	want := "{" + strings.Join(append(a, b...), ",") + "}"
	got, err := canonicalJSON([]byte("{" + strings.Join(body, ",") + "}"))
	if err != nil || string(got) != want {
		t.Fatalf("canonical = %s, %v, want %s", got, err, want)
	}
	slices.Reverse(body)
	reversed, _ := canonicalJSON([]byte("{" + strings.Join(body, ",") + "}"))
	if bytes.Equal(got, reversed) {
		t.Fatal("reordered duplicate keys gave the same canonical form")
	}
}

// testRand 返回随机测试用的生成器：种子打印出来，设 QUOTAFORGE_TEST_SEED 可以复现。
func testRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("QUOTAFORGE_TEST_SEED"); s != "" {
		var err error
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("seed %d (rerun with QUOTAFORGE_TEST_SEED=%d)", seed, seed)
	return rand.New(rand.NewPCG(seed, seed))
}

// 随机 JSON 用到的字符串写法（引号里的原文）：键和值都从这里挑，专挑规范化容易出错的：只差大小写的、Unicode 折叠
// 相同的（ſ 和 s，开尔文符号和 k）、各种转义、成对的代理项、转义的反斜杠。
var randomStrings = []string{
	`a`, `A`, `b`, `B`, `s`, `ſ`, `k`, `\u212a`, `K`, `stream`, `ſtream`, `max_tokens`, `max_toKens`, `Temperature`,
	`temperature`, ``, `é`, `\u00e9`, `É`, `\u0041`, `\n`, `\/`, `\"`, `\\`, `\\ud800`, `\ud83d\ude00`, `😀`, `<>&`,
	`\u003c`, `a b`,
}

var randomNumbers = []string{
	"0", "-0", "-0.0", "1", "1.0", "1.50", "2e3", "2E+3", "1E-7", "-1", "12345678901234567890123", "9007199254740993",
}

// writeRandomJSON 写一个随机的 JSON 值，空白、键的先后、重复的键、数字的写法都随机。
func writeRandomJSON(r *rand.Rand, b *strings.Builder, depth int) {
	space := func() { b.WriteString([]string{"", "", " ", "\n\t", "  "}[r.IntN(5)]) }
	str := func() { b.WriteString(`"` + randomStrings[r.IntN(len(randomStrings))] + `"`) }
	space()
	switch k := r.IntN(10); {
	case depth < 6 && k < 3:
		b.WriteByte('{')
		for i := range r.IntN(6) {
			if i > 0 {
				b.WriteByte(',')
			}
			space()
			str()
			space()
			b.WriteByte(':')
			writeRandomJSON(r, b, depth+1)
		}
		space()
		b.WriteByte('}')
	case depth < 6 && k < 5:
		b.WriteByte('[')
		for i := range r.IntN(5) {
			if i > 0 {
				b.WriteByte(',')
			}
			writeRandomJSON(r, b, depth+1)
		}
		space()
		b.WriteByte(']')
	case k < 7:
		str()
	case k < 9:
		b.WriteString(randomNumbers[r.IntN(len(randomNumbers))])
	default:
		b.WriteString([]string{"true", "false", "null"}[r.IntN(3)])
	}
	space()
}

// mutate 把合法的 JSON 改坏一处：截断、插入一个会出错的字节或孤立的代理项、结尾多出数据。
func mutate(r *rand.Rand, s string) string {
	at := r.IntN(len(s) + 1)
	switch r.IntN(4) {
	case 0:
		return s[:at]
	case 1:
		return s[:at] + []string{"\xff", `"`, `\`, "}", "]", ",", ":"}[r.IntN(7)] + s[at:]
	case 2:
		return s[:at] + `"\ud800"` + s[at:]
	default:
		return s + []string{" {}", "1", ` "x"`}[r.IntN(3)]
	}
}

// sameAsQuadratic 要求新实现和对照实现对 body 的输出、出错（包括错误文字）完全相同，返回新实现的错误。
func sameAsQuadratic(t *testing.T, body string) error {
	t.Helper()
	got, err := canonicalJSON([]byte(body))
	want, wantErr := quadraticCanonicalJSON([]byte(body))
	if !bytes.Equal(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
		t.Fatalf("body %q:\ngot  %q, %v\nwant %q, %v", body, got, err, want, wantErr)
	}
	return err
}

// 新实现和线性化之前的实现，对随机的合法和非法请求体，输出和出错都逐字节相同。
func TestCanonicalJSONMatchesQuadratic(t *testing.T) {
	r := testRand(t)
	var valid, invalid int
	for range 20000 {
		var b strings.Builder
		writeRandomJSON(r, &b, 0)
		body := b.String()
		if r.IntN(4) == 0 {
			body = mutate(r, body)
		}
		if sameAsQuadratic(t, body) == nil {
			valid++
		} else {
			invalid++
		}
	}
	t.Logf("%d valid, %d invalid bodies", valid, invalid)
	if valid < 10000 || invalid < 1000 { // 生成器本身没坏：两种都要有足够多
		t.Fatalf("%d valid, %d invalid bodies", valid, invalid)
	}
}

// 解码器的嵌套上限（10000 层）附近：数组、对象和两者交替，新实现和对照实现的输出、出错都相同。
func TestCanonicalJSONDeepNesting(t *testing.T) {
	for _, depth := range []int{9999, 10000, 10001} {
		for name, body := range map[string]string{
			"arrays":  strings.Repeat("[", depth) + "1" + strings.Repeat("]", depth),
			"objects": strings.Repeat(`{"b":1,"a":`, depth) + "1" + strings.Repeat("}", depth),
			"mixed": strings.Repeat(`[{"a":`, depth/2) + strings.Repeat("[", depth%2) + "1" + strings.Repeat("]", depth%2) +
				strings.Repeat("}]", depth/2),
		} {
			t.Run(fmt.Sprintf("%s/%d", name, depth), func(t *testing.T) { t.Log(sameAsQuadratic(t, body)) })
		}
	}
}

// deepBody 是把一个 leaf 字节的字符串嵌在 depth 层对象里的请求体。
func deepBody(depth, leaf int) []byte {
	return []byte(strings.Repeat(`{"a":`, depth) + `"` + strings.Repeat("x", leaf) + `"` + strings.Repeat("}", depth))
}

// 不再是平方级：同样 64 KiB 的里层，嵌 1000 层和嵌 10 层分配的字节数差不多（只多每层固定的一点）。
// 线性化之前每层对象都把里面的值复制一遍，1000 层要分配约 64 MB。按分配的字节数判断，不看时间。
func TestCanonicalJSONLinear(t *testing.T) {
	allocated := func(body []byte) uint64 {
		least := uint64(math.MaxUint64)
		for range 3 {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if _, err := canonicalJSON(body); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			least = min(least, after.TotalAlloc-before.TotalAlloc)
		}
		return least
	}
	shallow, deep := allocated(deepBody(10, 64<<10)), allocated(deepBody(1000, 64<<10))
	t.Logf("allocated: 10 levels %d bytes, 1000 levels %d bytes", shallow, deep)
	if deep > 2*shallow+1000*1024 {
		t.Fatalf("1000 levels allocated %d bytes, 10 levels %d: grows with depth", deep, shallow)
	}
}

// 深嵌套的请求体：1000 层、1 MiB 的里层；以及请求体上限附近的最坏情况（9999 层，约 2 MiB）。
func BenchmarkCanonicalJSONDeep(b *testing.B) {
	for _, body := range []struct {
		name string
		body []byte
	}{{"depth1000", deepBody(1000, 1<<20)}, {"limit", deepBody(9999, maxRequestBody-60000)}} {
		for _, impl := range []struct {
			name string
			f    func([]byte) ([]byte, error)
		}{{"linear", canonicalJSON}, {"quadratic", quadraticCanonicalJSON}} {
			b.Run(body.name+"/"+impl.name, func(b *testing.B) {
				b.SetBytes(int64(len(body.body)))
				for b.Loop() {
					if _, err := impl.f(body.body); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestFingerprintIncludesEndpoint(t *testing.T) {
	body := []byte(`{"model":"shared","input":["a"]}`)
	chat, _ := fingerprint(config.Chat, body)
	chat2, _ := fingerprint(config.Chat, []byte(`{"input":["a"],"model":"shared"}`))
	emb, _ := fingerprint(config.Embeddings, body)
	if len(chat) != 32 || !bytes.Equal(chat, chat2) || bytes.Equal(chat, emb) {
		t.Fatalf("chat %x, chat reordered %x, embeddings %x", chat, chat2, emb)
	}
}

// --- 不用库：键的格式、错误映射 ---

// 不合法的键都是 400 invalid_idempotency_key，不预留、不调上游。直接调处理器，才能发 net/http 客户端不肯发的头。
func TestIdempotencyKeyFormat(t *testing.T) {
	stub := newAllowAll()
	h := newHarnessWith(t, stub)
	send := func(values ...string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"shared"}`))
		req.Header.Set("Authorization", "Bearer "+clientKey)
		for _, v := range values {
			req.Header.Add("Idempotency-Key", v)
		}
		rec := httptest.NewRecorder()
		h.qf.Config.Handler.ServeHTTP(rec, req)
		return rec
	}
	for _, bad := range [][]string{
		{""}, {strings.Repeat("a", 129)}, {"a b"}, {" a"}, {"a\tb"}, {"a\x01b"}, {"a\x7fb"}, {"é"}, {"a\x80"},
		{"a", "a"}, {"a", "b"},
	} {
		rec := send(bad...)
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), `"code":"invalid_idempotency_key"`) {
			t.Errorf("%q: %d %s", bad, rec.Code, rec.Body)
		}
	}
	if n := len(stub.reserved); n != 0 || h.fake.Stats().ChatCompletions != 0 {
		t.Fatalf("invalid keys reserved %d times, upstream got %d", n, h.fake.Stats().ChatCompletions)
	}
	allVisible := ""
	for b := byte(0x21); b <= 0x7e; b++ {
		allVisible += string(b)
	}
	for _, good := range []string{strings.Repeat("~", 128), allVisible, `"quoted"`, "a,b"} {
		if rec := send(good); rec.Code != 200 {
			t.Errorf("%q: %d %s", good, rec.Code, rec.Body)
			continue
		}
		if r := stub.lastReservation(t); r.IdempotencyKey != good || len(r.Fingerprint) != 32 {
			t.Errorf("%q: reservation key %q, fingerprint %x", good, r.IdempotencyKey, r.Fingerprint)
		}
	}
	// 不带键：预留里没有键和指纹
	if rec := send(); rec.Code != 200 {
		t.Fatal(rec.Code)
	}
	if r := stub.lastReservation(t); r.IdempotencyKey != "" || r.Fingerprint != nil {
		t.Fatalf("no key: reservation key %q, fingerprint %x", r.IdempotencyKey, r.Fingerprint)
	}
	// 带键时请求体里的非法 UTF-8、孤立的代理项是 400 invalid_request；不带键照常转发
	for _, body := range []string{"{\"model\":\"shared\",\"s\":\"\xff\"}", `{"model":"shared","s":"\ud800"}`} {
		r := h.chat(t, body, "Idempotency-Key", "k")
		if r.status != 400 || errorCode(t, r) != "invalid_request" {
			t.Errorf("%q with a key: %d %s", body, r.status, r.body)
		}
		if r := h.chat(t, body); r.status != 200 {
			t.Errorf("%q without a key: %d", body, r.status)
		}
	}
}

// 键的校验在鉴权、请求体、模型这些检查之后：坏的 API Key 加坏的幂等键，先回 401。
func TestBadAPIKeyBeforeIdempotencyKey(t *testing.T) {
	h := newHarness(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"shared"}`))
	req.Header.Set("Authorization", "Bearer qfk_not-a-key")
	req.Header.Set("Idempotency-Key", "has space")
	rec := httptest.NewRecorder()
	h.qf.Config.Handler.ServeHTTP(rec, req)
	if rec.Code != 401 || !strings.Contains(rec.Body.String(), `"code":"invalid_api_key"`) {
		t.Fatalf("%d %s, want 401 invalid_api_key", rec.Code, rec.Body)
	}
}

// 账本的几种幂等拒绝映射成固定的错误码，不调上游；只有还在处理的带 Retry-After。
func TestIdempotencyErrorsWithStub(t *testing.T) {
	for _, tc := range []struct {
		err              error
		status           int
		typ, code, retry string
	}{
		{ledger.ErrIdempotencyKeyReused, 422, "invalid_request_error", "idempotency_key_reused", ""},
		{ledger.ErrIdempotencyInProgress, 409, "conflict_error", "idempotency_in_progress", "1"},
		{ledger.ErrIdempotencyOutcomeUnknown, 409, "conflict_error", "idempotency_outcome_unknown", ""},
		{ledger.ErrIdempotencyResponseTooLarge, 409, "conflict_error", "idempotency_response_too_large", ""},
	} {
		h := newHarnessWith(t, &faulty{Ledger: newAllowAll(), reserveErr: tc.err})
		r := h.chat(t, `{"model":"shared"}`, "Idempotency-Key", "k")
		var e struct{ Error struct{ Type, Code string } }
		_ = json.Unmarshal(r.body, &e)
		if r.status != tc.status || e.Error.Code != tc.code || e.Error.Type != tc.typ || r.header.Get("Retry-After") != tc.retry {
			t.Errorf("%v: %d %s, Retry-After %q", tc.err, r.status, r.body, r.header.Get("Retry-After"))
		}
		if h.fake.Stats().ChatCompletions != 0 {
			t.Errorf("%v: request reached the upstream", tc.err)
		}
		if l := h.logLine(t, requestID(t, r)); l["error_code"] != tc.code || l["idempotency_key"] != "k" {
			t.Errorf("%v: log = %v", tc.err, l)
		}
	}
}

// --- 以下用真的 MySQL ---

func (e *dbEnv) sendKey(t *testing.T, path, key, body string) reply {
	t.Helper()
	return e.send(t, path, body, "Idempotency-Key", key)
}

// idemRecord 是幂等记录的元数据（不读正文）。
type idemRecord struct {
	callID       string
	expired      bool
	status, size sql.NullInt64
	bodyNull     bool
}

func (e *dbEnv) record(t *testing.T, key string) (idemRecord, bool) {
	t.Helper()
	var r idemRecord
	err := e.admin.QueryRow(`SELECT k.call_id, k.expires_at <= UTC_TIMESTAMP(6), k.response_status, k.response_bytes, k.response_body IS NULL
		FROM idempotency_keys k JOIN accounts a ON a.id = k.account_id WHERE a.name = ? AND k.idem_key = ?`, e.account, key).Scan(
		&r.callID, &r.expired, &r.status, &r.size, &r.bodyNull)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return r, true
}

func (e *dbEnv) expire(t *testing.T, key string) {
	t.Helper()
	if _, err := e.admin.Exec(`UPDATE idempotency_keys k JOIN accounts a ON a.id = k.account_id
		SET k.expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE a.name = ? AND k.idem_key = ?`, e.account, key); err != nil {
		t.Fatal(err)
	}
}

// snapshot 是这个账户在账本里的全部痕迹：调用数、账本行数、额度行，用来确认重放什么都没写。
type ledgerState struct{ calls, entries, limit, reserved, consumed int64 }

func (e *dbEnv) state(t *testing.T) ledgerState {
	t.Helper()
	var s ledgerState
	if err := e.admin.QueryRow(`SELECT COUNT(*) FROM ledger l JOIN calls c ON c.id = l.call_id JOIN accounts a ON a.id = c.account_id
		WHERE a.name = ?`, e.account).Scan(&s.entries); err != nil {
		t.Fatal(err)
	}
	s.calls = int64(e.callCount(t))
	s.limit, s.reserved, s.consumed = e.budget(t)
	return s
}

func (e *dbEnv) upstreamCalls() int64 {
	st := e.fake.Stats()
	return st.ChatCompletions + st.Embeddings
}

func wantCode(t *testing.T, r reply, status int, code string) {
	t.Helper()
	if r.status != status || errorCode(t, r) != code {
		t.Fatalf("got %d %s, want %d %s", r.status, r.body, status, code)
	}
}

// wantReplay：状态码、Content-Type、正文逐字节相同，x-request-id 是原来的调用号，带 Idempotent-Replayed。
func wantReplay(t *testing.T, got, first reply) {
	t.Helper()
	if got.status != first.status || got.header.Get("Content-Type") != first.header.Get("Content-Type") ||
		!bytes.Equal(got.body, first.body) || got.header.Get("x-request-id") != first.header.Get("x-request-id") ||
		got.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %q x-request-id %q replayed %q (%d bytes); first: %d %q x-request-id %q (%d bytes)",
			got.status, got.header.Get("Content-Type"), got.header.Get("x-request-id"), got.header.Get("Idempotent-Replayed"), len(got.body),
			first.status, first.header.Get("Content-Type"), first.header.Get("x-request-id"), len(first.body))
	}
}

// 已完成、请求相同：重放，什么都不写、不调上游；额度用完了也照样重放。重放的日志行有 replay_of、没有 usage。
func TestIdempotentReplay(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	for _, tc := range []struct{ key, path, body string }{
		{"chat", "/v1/chat/completions", withModel(toolsBody, `"shared"`)},
		{"embeddings", "/v1/embeddings", `{"model":"shared","input":["a","b"]}`},
		{"no-usage", "/v1/chat/completions", `{"model":"fake-no-usage","messages":[]}`}, // 2xx 缺 usage：USAGE_UNKNOWN，有快照
	} {
		key := tc.key
		first := e.sendKey(t, tc.path, key, tc.body)
		if first.status != 200 || first.header.Get("Idempotent-Replayed") != "" {
			t.Fatalf("first: %d %s", first.status, first.body)
		}
		id := requestID(t, first)
		if c := e.call(t, id); c.status.String == "RESERVED" {
			t.Fatalf("first call is %s", c.status.String)
		}
		upstream, before := e.upstreamCalls(), e.state(t)
		// 请求体写法不同（空白、键序）也是同一个请求
		again := e.sendKey(t, tc.path, key, " "+tc.body+"\n")
		wantReplay(t, again, first)
		if e.upstreamCalls() != upstream || e.state(t) != before {
			t.Fatalf("replay changed something: upstream %d → %d, ledger %+v → %+v", upstream, e.upstreamCalls(), before, e.state(t))
		}
		l := e.replayLine(t, id)
		if l["replay_of"] != id || l["status"] != float64(200) || l["idempotency_key"] != key {
			t.Fatalf("replay log = %v", l)
		}
		for _, field := range []string{"usage", "call_status", "reserved_tokens", "upstream_status"} {
			if _, ok := l[field]; ok {
				t.Fatalf("replay log has %s: %v", field, l)
			}
		}
	}
	// 额度用完：新的键 429、不留记录；已完成的键照样重放
	if _, err := e.admin.Exec("UPDATE budgets b JOIN accounts a ON a.id = b.account_id SET b.limit_tokens = 0 WHERE a.name = ?", e.account); err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.sendKey(t, "/v1/embeddings", "exhausted", `{"model":"shared","input":["a"]}`), 429, "quota_exhausted")
	if _, ok := e.record(t, "exhausted"); ok {
		t.Fatal("429 left a record")
	}
	first := e.sendKey(t, "/v1/embeddings", "embeddings", `{"model":"shared","input":["a","b"]}`)
	if first.header.Get("Idempotent-Replayed") != "true" || first.status != 200 {
		t.Fatalf("replay with no quota left: %d %s", first.status, first.body)
	}
	dbtest.Verify(t, e.store)
}

// replayLine 等到重放 id 的那一行调用日志出现（重放的调用号只在日志里）。
func (e *dbEnv) replayLine(t *testing.T, id string) map[string]any {
	t.Helper()
	var line map[string]any
	waitFor(t, func() bool {
		var found []map[string]any
		for _, l := range e.logs.lines(t) {
			if l["replay_of"] == id {
				found = append(found, l)
			}
		}
		if len(found) > 1 {
			t.Fatalf("%d replay lines for %s", len(found), id)
		}
		if len(found) == 1 {
			line = found[0]
		}
		return line != nil
	})
	return line
}

// 处理中：409 idempotency_in_progress，带 Retry-After: 1；处理完后重放。
func TestIdempotentInProgress(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	body := `{"model":"emb-hold","input":["held"]}`
	done := make(chan reply, 1)
	go func() { done <- e.post(context.Background(), "/v1/embeddings", e.key, body, "Idempotency-Key", "held") }()
	waitFor(t, func() bool { return e.fake.Stats().Held == 1 })
	r := e.sendKey(t, "/v1/embeddings", "held", body)
	wantCode(t, r, 409, "idempotency_in_progress")
	if r.header.Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q", r.header.Get("Retry-After"))
	}
	wantCode(t, e.sendKey(t, "/v1/embeddings", "held", `{"model":"emb-hold","input":["other"]}`), 422, "idempotency_key_reused")
	if e.fake.Stats().Embeddings != 1 || e.callCount(t) != 1 {
		t.Fatalf("upstream %d, calls %d", e.fake.Stats().Embeddings, e.callCount(t))
	}
	e.fake.Release()
	first := <-done
	if first.err != nil || first.status != 200 {
		t.Fatalf("held request: %d %v", first.status, first.err)
	}
	wantReplay(t, e.sendKey(t, "/v1/embeddings", "held", body), first)
	if e.fake.Stats().Embeddings != 1 {
		t.Fatal("replay reached the upstream")
	}
	dbtest.Verify(t, e.store)
}

// CANCELLED（上游没有生成结果）：同一个键重新执行，记录改指向新的调用；两次调用都记着这个键。
func TestIdempotentCancelledRunsAgain(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	for _, tc := range []struct {
		alias  string
		status int
		code   string
	}{
		{"fake-401", 502, "upstream_auth_failed"},
		{"fake-429", 503, "upstream_rate_limited"},
		{"dead", 502, "upstream_unavailable"}, // 连接没建立
	} {
		body := `{"model":"` + tc.alias + `"}`
		first := e.sendKey(t, "/v1/chat/completions", tc.alias, body)
		wantCode(t, first, tc.status, tc.code)
		firstID := requestID(t, first)
		if c := e.call(t, firstID); c.status.String != "CANCELLED" {
			t.Fatalf("%s: first call %s", tc.alias, c.status.String)
		}
		upstream := e.upstreamCalls()
		second := e.sendKey(t, "/v1/chat/completions", tc.alias, body)
		wantCode(t, second, tc.status, tc.code)
		secondID := requestID(t, second)
		rec, _ := e.record(t, tc.alias)
		if secondID == firstID || rec.callID != secondID || second.header.Get("Idempotent-Replayed") != "" {
			t.Fatalf("%s: second %s, first %s, record → %s", tc.alias, secondID, firstID, rec.callID)
		}
		if tc.alias != "dead" && e.upstreamCalls() != upstream+1 {
			t.Fatalf("%s: upstream %d → %d, want one more", tc.alias, upstream, e.upstreamCalls())
		}
		var n int
		if err := e.admin.QueryRow("SELECT COUNT(*) FROM calls WHERE id IN (?, ?) AND idempotency_key = ?", firstID, secondID, tc.alias).Scan(&n); err != nil || n != 2 {
			t.Fatalf("%s: %d calls carry the key (%v)", tc.alias, n, err)
		}
	}
	dbtest.Verify(t, e.store)
}

// 发出后结果未知、没有快照：409 idempotency_outcome_unknown，不带 Retry-After，不再调上游。
func TestIdempotentOutcomeUnknown(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	for _, tc := range []struct {
		alias  string
		status int
		code   string
	}{
		{"fake-500", 502, "upstream_error"},
		{"fake-hang", 504, "upstream_timeout"},
		{"fake-not-json", 502, "upstream_invalid_response"},
	} {
		body := `{"model":"` + tc.alias + `"}`
		first := e.sendKey(t, "/v1/chat/completions", tc.alias, body)
		wantCode(t, first, tc.status, tc.code)
		if c := e.call(t, requestID(t, first)); c.status.String != "USAGE_UNKNOWN" {
			t.Fatalf("%s: first call %s", tc.alias, c.status.String)
		}
		upstream, before := e.upstreamCalls(), e.state(t)
		r := e.sendKey(t, "/v1/chat/completions", tc.alias, body)
		wantCode(t, r, 409, "idempotency_outcome_unknown")
		if r.header.Get("Retry-After") != "" || e.upstreamCalls() != upstream || e.state(t) != before {
			t.Fatalf("%s: Retry-After %q, upstream %d → %d", tc.alias, r.header.Get("Retry-After"), upstream, e.upstreamCalls())
		}
	}
	dbtest.Verify(t, e.store)
}

// 指纹不同：422，不论之前的调用处于什么状态（处理中的在 TestIdempotentInProgress）；接口不同也算不同的请求。
func TestIdempotentKeyReused(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	for _, tc := range []struct{ key, path, body string }{
		{"settled", "/v1/chat/completions", `{"model":"shared"}`},
		{"no-usage", "/v1/chat/completions", `{"model":"fake-no-usage"}`},
		{"cancelled", "/v1/chat/completions", `{"model":"fake-401"}`},
		{"unknown", "/v1/chat/completions", `{"model":"fake-500"}`},
	} {
		e.sendKey(t, tc.path, tc.key, tc.body)
		upstream, before := e.upstreamCalls(), e.state(t)
		changed := strings.Replace(tc.body, "}", `,"temperature":1}`, 1)
		wantCode(t, e.sendKey(t, tc.path, tc.key, changed), 422, "idempotency_key_reused")
		if e.upstreamCalls() != upstream || e.state(t) != before {
			t.Fatalf("%s: 422 changed something", tc.key)
		}
	}
	// 同样的请求体发到另一个接口
	body := `{"model":"shared","input":["a"]}`
	if r := e.sendKey(t, "/v1/embeddings", "endpoint", body); r.status != 200 {
		t.Fatal(r.status)
	}
	wantCode(t, e.sendKey(t, "/v1/chat/completions", "endpoint", body), 422, "idempotency_key_reused")
	dbtest.Verify(t, e.store)
}

// 过期的记录当作没有：同一个键重新执行（请求体可以不同），记录指向新的调用、重新计时。
func TestIdempotentExpired(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	first := e.sendKey(t, "/v1/chat/completions", "k", `{"model":"shared"}`)
	e.expire(t, "k")
	for i, body := range []string{`{"model":"shared"}`, `{"model":"shared","temperature":1}`} {
		upstream := e.upstreamCalls()
		r := e.sendKey(t, "/v1/chat/completions", "k", body)
		rec, _ := e.record(t, "k")
		if r.status != 200 || r.header.Get("Idempotent-Replayed") != "" || e.upstreamCalls() != upstream+1 ||
			rec.callID != requestID(t, r) || rec.callID == requestID(t, first) || rec.expired {
			t.Fatalf("%d: %d replayed %q, record %+v", i, r.status, r.header.Get("Idempotent-Replayed"), rec)
		}
		e.expire(t, "k")
	}
	dbtest.Verify(t, e.store)
}

// 过期后改指向新调用时，旧快照要一起清掉：新调用 CANCELLED 或结果未知时，重试不能把过期那次的 200 重放出去。
func TestIdempotentExpiredRecordDropsOldSnapshot(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	for _, tc := range []struct {
		key, body   string
		status      int // 改指向之后第一次的结果
		code        string
		retryStatus int // 重试的结果
		retryCode   string
	}{
		// CANCELLED：重试重新执行，还是上游 401
		{"cancelled", `{"model":"fake-401"}`, 502, "upstream_auth_failed", 502, "upstream_auth_failed"},
		// 结果未知：重试 409，不重新执行
		{"unknown", `{"model":"fake-500"}`, 502, "upstream_error", 409, "idempotency_outcome_unknown"},
	} {
		if r := e.sendKey(t, "/v1/chat/completions", tc.key, `{"model":"shared"}`); r.status != 200 {
			t.Fatalf("%s: first request %d", tc.key, r.status)
		}
		if rec, _ := e.record(t, tc.key); !rec.status.Valid {
			t.Fatalf("%s: the first call has no snapshot", tc.key)
		}
		e.expire(t, tc.key)
		wantCode(t, e.sendKey(t, "/v1/chat/completions", tc.key, tc.body), tc.status, tc.code)
		if rec, _ := e.record(t, tc.key); rec.status.Valid || !rec.bodyNull {
			t.Fatalf("%s: the re-pointed record still has a snapshot: %+v", tc.key, rec)
		}
		r := e.sendKey(t, "/v1/chat/completions", tc.key, tc.body)
		wantCode(t, r, tc.retryStatus, tc.retryCode)
		if r.header.Get("Idempotent-Replayed") != "" {
			t.Fatalf("%s: the retry was a replay", tc.key)
		}
	}
	dbtest.Verify(t, e.store)
}

// 预留之前就失败的请求不留记录：429 之后调高额度、400 之后改对请求体、预留时 503 之后，同一个键都能正常执行。
func TestIdempotentNoRecordBeforeReserve(t *testing.T) {
	e := newDBEnv(t, 10, nil)
	wantCode(t, e.sendKey(t, "/v1/chat/completions", "quota", `{"model":"shared"}`), 429, "quota_exhausted")
	wantCode(t, e.sendKey(t, "/v1/chat/completions", "body", `{"model":"shared"`), 400, "invalid_request")
	wantCode(t, e.sendKey(t, "/v1/chat/completions", "model", `{"model":"nope"}`), 404, "model_not_found")
	// 预留时账本不可用：另一个连接占着额度行，预留等到时限后 503，事务回滚
	holder, err := e.admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec("SELECT 1 FROM budgets b JOIN accounts a ON a.id = b.account_id WHERE a.name = ? FOR UPDATE", e.account); err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.sendKey(t, "/v1/chat/completions", "ledger", `{"model":"shared"}`), 503, "ledger_unavailable")
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"quota", "body", "model", "ledger"} {
		if _, ok := e.record(t, k); ok {
			t.Fatalf("%s: a failed request left a record", k)
		}
	}
	if _, err := e.admin.Exec("UPDATE budgets b JOIN accounts a ON a.id = b.account_id SET b.limit_tokens = 1000000 WHERE a.name = ?", e.account); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"quota", "body", "model", "ledger"} {
		if r := e.sendKey(t, "/v1/chat/completions", k, `{"model":"shared"}`); r.status != 200 || r.header.Get("Idempotent-Replayed") != "" {
			t.Fatalf("%s: %d %s", k, r.status, r.body)
		}
	}
	dbtest.Verify(t, e.store)
}

// 范围是账户：同一个键在两个账户里互不相干；同一账户的两把 Key 共用。键区分大小写。
func TestIdempotentScope(t *testing.T) {
	a := newDBEnv(t, 1_000_000, nil)
	b := newDBEnv(t, 1_000_000, nil)
	body := `{"model":"shared"}`
	ra := a.sendKey(t, "/v1/chat/completions", "same", body)
	rb := b.sendKey(t, "/v1/chat/completions", "same", body)
	if ra.status != 200 || rb.status != 200 || rb.header.Get("Idempotent-Replayed") != "" || requestID(t, ra) == requestID(t, rb) {
		t.Fatal("two accounts share a key")
	}
	// a 的另一把 Key（只允许 shared）：重放
	r := a.post(context.Background(), "/v1/chat/completions", a.limited, body, "Idempotency-Key", "same")
	wantReplay(t, r, ra)
	// 只差大小写：另一个键
	upstream := a.upstreamCalls()
	if r := a.sendKey(t, "/v1/chat/completions", "SAME", body); r.status != 200 || r.header.Get("Idempotent-Replayed") != "" || a.upstreamCalls() != upstream+1 {
		t.Fatalf("SAME: %d replayed %q", r.status, r.header.Get("Idempotent-Replayed"))
	}
	if rec, _ := a.record(t, "SAME"); rec.callID == requestID(t, ra) {
		t.Fatal("SAME points to the call of same")
	}
	dbtest.Verify(t, a.store)
}

// 同一个键、同样的请求体，20 个请求同时出发，上游先挂住：上游恰好收到 1 次，只有 1 个调用、1 次预留，
// 其余都是 409 idempotency_in_progress；放开后再发一次，得到重放。
func TestIdempotentConcurrentSameKey(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	const n = 20
	body := `{"model":"emb-hold","input":["concurrent"]}`
	start := make(chan struct{})
	replies := make([]reply, n)
	var answered atomic.Int64
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			replies[i] = e.post(context.Background(), "/v1/embeddings", e.key, body, "Idempotency-Key", "concurrent")
			answered.Add(1)
		})
	}
	close(start)
	waitFor(t, func() bool { return answered.Load() == n-1 && e.fake.Stats().Held == 1 })
	if st := e.state(t); e.fake.Stats().Embeddings != 1 || st.calls != 1 || st.entries != 1 {
		t.Fatalf("while held: upstream %d, ledger %+v", e.fake.Stats().Embeddings, st)
	}
	e.fake.Release()
	wg.Wait()
	var ok reply
	conflicts := 0
	for _, r := range replies {
		switch {
		case r.err != nil:
			t.Fatal(r.err)
		case r.status == 200:
			ok = r
		case r.status == 409 && errorCode(t, r) == "idempotency_in_progress" && r.header.Get("Retry-After") == "1":
			conflicts++
		default:
			t.Fatalf("unexpected reply %d %s", r.status, r.body)
		}
	}
	if ok.status != 200 || conflicts != n-1 {
		t.Fatalf("one 200 and %d conflicts wanted, got 200: %v, conflicts %d", n-1, ok.status == 200, conflicts)
	}
	wantReplay(t, e.sendKey(t, "/v1/embeddings", "concurrent", body), ok)
	if e.fake.Stats().Embeddings != 1 {
		t.Fatalf("upstream got %d", e.fake.Stats().Embeddings)
	}
	dbtest.Verify(t, e.store)
}

// 8 个账户各 10 个新键同时请求：全部成功，没有死锁（1213），没有 503。查键用加锁读的话，
// 不同账户的事务会在同一个间隙上互相等成死锁（实现前的探针在可重复读下复现过）。
func TestIdempotentConcurrentAccounts(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	keys := []string{e.key}
	for range 7 {
		acct := dbtest.Account(t, e.store, 1_000_000)
		k, err := e.store.IssueKey(context.Background(), acct, allAliases, ledger.Rate{})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k.Secret)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	statuses := map[int]int{}
	for _, apiKey := range keys {
		for i := range 10 {
			wg.Go(func() {
				<-start
				r := e.post(context.Background(), "/v1/chat/completions", apiKey, `{"model":"shared"}`, "Idempotency-Key", fmt.Sprintf("new-%d", i))
				mu.Lock()
				defer mu.Unlock()
				statuses[r.status]++
				if r.err != nil || r.status != 200 {
					t.Errorf("%d %s %v", r.status, r.body, r.err)
				}
			})
		}
	}
	close(start)
	wg.Wait()
	if statuses[200] != 80 || strings.Contains(e.logs.String(), "ledger_error") {
		t.Fatalf("statuses %v; ledger errors in the log: %v", statuses, strings.Contains(e.logs.String(), "ledger_error"))
	}
	dbtest.Verify(t, e.store)
}

// 收尾失败：快照没写进去，调用留在 RESERVED；同一个键重试得到 409 idempotency_in_progress。
func TestIdempotentFinishFailure(t *testing.T) {
	e := newDBEnv(t, 1_000_000, func(l Ledger) Ledger { return &faulty{Ledger: l, finishErr: errors.New("commit: invalid connection")} })
	r := e.sendKey(t, "/v1/chat/completions", "k", `{"model":"shared"}`)
	if r.status != 200 {
		t.Fatal(r.status)
	}
	if c := e.call(t, requestID(t, r)); c.status.String != "IN_FLIGHT" { // 已落发送意图，收尾失败
		t.Fatal(c.status.String)
	}
	if rec, _ := e.record(t, "k"); rec.status.Valid || !rec.bodyNull {
		t.Fatalf("record has a snapshot: %+v", rec)
	}
	wantCode(t, e.sendKey(t, "/v1/chat/completions", "k", `{"model":"shared"}`), 409, "idempotency_in_progress")
	dbtest.Verify(t, e.store)
}

// 快照和收尾在同一个事务里：另一个连接锁住记录，收尾写快照时等锁、2 秒后超时，整个收尾回滚，
// 调用仍是 RESERVED、额度不动，重试得到 409 idempotency_in_progress。快照如果单独提交，调用会变成
// SETTLED 却没有快照：重试得到 idempotency_outcome_unknown，校验账本也会报 idem_settled_snapshot。
func TestSnapshotInFinishTransaction(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	body := `{"model":"emb-hold","input":["x"]}`
	done := make(chan reply, 1)
	go func() {
		done <- e.post(context.Background(), "/v1/embeddings", e.key, body, "Idempotency-Key", "locked")
	}()
	waitFor(t, func() bool { return e.fake.Stats().Held == 1 })
	before := e.state(t)
	holder, err := e.admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	// 只锁这一条记录：联表加锁会连 accounts 的行也锁住，收尾的其它语句也可能被挡住，就分不出快照在不在同一个事务里。
	var accountID int64
	if err := e.admin.QueryRow("SELECT id FROM accounts WHERE name = ?", e.account).Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec("SELECT 1 FROM idempotency_keys WHERE account_id = ? AND idem_key = 'locked' FOR UPDATE", accountID); err != nil {
		t.Fatal(err)
	}
	e.fake.Release()
	r := <-done // 上游的 2xx 照样返回；收尾在等锁，config.LedgerTimeout 之后失败
	if r.status != 200 {
		t.Fatal(r.status)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if c := e.call(t, requestID(t, r)); c.status.String != "IN_FLIGHT" || len(c.kinds) != 1 { // 已落发送意图，收尾失败
		t.Fatalf("call = %s %v", c.status.String, c.kinds)
	}
	if after := e.state(t); after != before {
		t.Fatalf("ledger %+v → %+v", before, after)
	}
	wantCode(t, e.sendKey(t, "/v1/embeddings", "locked", body), 409, "idempotency_in_progress")
	dbtest.Verify(t, e.store)
}

// timedLedger 记下每次收尾和预留用了多久。
type timedLedger struct {
	Ledger
	mu              sync.Mutex
	finish, reserve time.Duration
}

func (l *timedLedger) Reserve(ctx context.Context, r ledger.Reservation) (*ledger.Replay, error) {
	start := time.Now()
	defer func() { l.mu.Lock(); l.reserve = time.Since(start); l.mu.Unlock() }()
	return l.Ledger.Reserve(ctx, r)
}

func (l *timedLedger) Finish(ctx context.Context, r ledger.Reservation, res ledger.Result) error {
	start := time.Now()
	defer func() { l.mu.Lock(); l.finish = time.Since(start); l.mu.Unlock() }()
	return l.Ledger.Finish(ctx, r, res)
}

func (l *timedLedger) took() (finish, reserve time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.finish, l.reserve
}

// 快照的上限：1 MiB 和正好 8 MiB 逐字节重放；8 MiB 加 1 字节和 32 MiB 照常结算、不存正文，
// 同一个键再来是 409 idempotency_response_too_large。收尾都在账本操作的时限内完成。
func TestSnapshotSizeLimit(t *testing.T) {
	timed := &timedLedger{}
	e := newDBEnv(t, 1<<40, func(l Ledger) Ledger { timed.Ledger = l; return timed })
	for _, size := range []int{1 << 20, 8 << 20, 8<<20 + 1, 32 << 20} {
		alias := fmt.Sprintf("size-%d", size)
		body := `{"model":"` + alias + `","input":["x"]}`
		first := e.sendKey(t, "/v1/embeddings", alias, body)
		finish, _ := timed.took()
		if first.status != 200 || len(first.body) != size {
			t.Fatalf("%s: %d, %d bytes", alias, first.status, len(first.body))
		}
		c := e.call(t, requestID(t, first))
		if c.status.String != "SETTLED" || c.usageTotal.Int64 != 1 {
			t.Fatalf("%s: call %s usage %v", alias, c.status.String, c.usageTotal)
		}
		rec, _ := e.record(t, alias)
		if rec.status.Int64 != 200 || rec.size.Int64 != int64(size) || rec.bodyNull != (size > ledger.MaxSnapshotBody) {
			t.Fatalf("%s: record %+v", alias, rec)
		}
		upstream := e.state(t)
		r := e.sendKey(t, "/v1/embeddings", alias, body)
		_, reserve := timed.took()
		if size <= ledger.MaxSnapshotBody {
			wantReplay(t, r, first)
		} else {
			wantCode(t, r, 409, "idempotency_response_too_large")
			if r.header.Get("Retry-After") != "" {
				t.Fatal("too large has Retry-After")
			}
		}
		if e.state(t) != upstream {
			t.Fatalf("%s: the retry changed the ledger", alias)
		}
		t.Logf("%s: finish %v, second reserve %v", alias, finish.Round(time.Millisecond), reserve.Round(time.Millisecond))
	}
	dbtest.Verify(t, e.store)
}

// 清理和带同一个过期键的请求同时进行：都不出错，请求照常执行，记录指向新的调用、不过期。
func TestPurgeDuringRequests(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	for round := range 20 {
		key := fmt.Sprintf("purge-%d", round)
		e.sendKey(t, "/v1/chat/completions", key, `{"model":"shared"}`)
		e.expire(t, key)
		start := make(chan struct{})
		var r reply
		var purgeErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			r = e.post(context.Background(), "/v1/chat/completions", e.key, `{"model":"shared","n":1}`, "Idempotency-Key", key)
		})
		wg.Go(func() {
			<-start
			for range 3 {
				if _, err := e.store.PurgeIdempotency(context.Background(), config.LedgerTimeout); err != nil {
					purgeErr = err
				}
			}
		})
		close(start)
		wg.Wait()
		rec, ok := e.record(t, key)
		if purgeErr != nil || r.err != nil || r.status != 200 || !ok || rec.expired || rec.callID != requestID(t, r) {
			t.Fatalf("round %d: purge %v, reply %d %v, record %v %+v", round, purgeErr, r.status, r.err, ok, rec)
		}
	}
	dbtest.Verify(t, e.store)
}

// 用量导出：重放的行有 replay_of、usage 为空，不算成新的用量。
func TestExportUsageReplay(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	body := `{"model":"shared","messages":[{"content":"abc"}]}`
	first := e.sendKey(t, "/v1/chat/completions", "k", body)
	e.sendKey(t, "/v1/chat/completions", "k", body)
	id := requestID(t, first)
	e.replayLine(t, id)
	var out bytes.Buffer
	if invalid, err := ExportUsage(strings.NewReader(e.logs.String()), &out); err != nil || invalid != 0 {
		t.Fatal(invalid, err)
	}
	records, err := csv.NewReader(&out).ReadAll()
	if err != nil || len(records) != 3 {
		t.Fatalf("%d records, %v", len(records), err)
	}
	cells := func(rec []string, names ...string) string {
		var got []string
		for _, n := range names {
			got = append(got, rec[slices.Index(usageColumns, n)])
		}
		return strings.Join(got, ",")
	}
	const cols = "status,prompt_tokens,completion_tokens,total_tokens,replay_of"
	var original, replay string
	for _, rec := range records[1:] {
		if rec[slices.Index(usageColumns, "request_id")] == id {
			original = cells(rec, strings.Split(cols, ",")...)
		} else {
			replay = cells(rec, strings.Split(cols, ",")...)
		}
	}
	if original != "200,3,10,13," || replay != "200,,,,"+id {
		t.Fatalf("original %q, replay %q", original, replay)
	}
}

// --- 对照：线性化之前的实现，原样（只改了名字），只用在测试里 ---
//
// 规范化的输出存在幂等记录里，24 小时内同一个键的重试要和它比，所以新实现对任意输入的输出和出错都必须和它逐字节相同。
// 这份是自成一体的副本（连 foldKey、writeString 也复制了），新实现里改坏任何一处，对照都能发现。

func quadraticCanonicalJSON(body []byte) ([]byte, error) {
	if !utf8.Valid(body) {
		return nil, errors.New("body is not valid UTF-8")
	}
	if quadraticHasLoneSurrogate(body) {
		return nil, errors.New("body has an unpaired surrogate escape")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var out bytes.Buffer
	if err := quadraticWriteCanonical(dec, &out); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing data after the JSON value")
	}
	return out.Bytes(), nil
}

// quadraticWriteCanonical 读一个完整的 JSON 值，写出它的规范形式。
func quadraticWriteCanonical(dec *json.Decoder, out *bytes.Buffer) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch v := tok.(type) {
	case json.Delim: // [ 或 {；Token 已经保证配对
		if v == '[' {
			out.WriteByte('[')
			for i := 0; dec.More(); i++ {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := quadraticWriteCanonical(dec, out); err != nil {
					return err
				}
			}
			out.WriteByte(']')
		} else if err := quadraticWriteObject(dec, out); err != nil {
			return err
		}
		_, err := dec.Token() // 结尾的 ] 或 }
		return err
	case string:
		quadraticWriteString(out, v)
	case json.Number:
		out.WriteString(string(v))
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case nil:
		out.WriteString("null")
	}
	return nil
}

func quadraticWriteObject(dec *json.Decoder, out *bytes.Buffer) error {
	type member struct {
		key, folded string
		value       []byte
	}
	var members []member
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		var value bytes.Buffer
		if err := quadraticWriteCanonical(dec, &value); err != nil {
			return err
		}
		members = append(members, member{key.(string), quadraticFoldKey(key.(string)), value.Bytes()})
	}
	slices.SortStableFunc(members, func(a, b member) int { return strings.Compare(a.folded, b.folded) })
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		quadraticWriteString(out, m.key)
		out.WriteByte(':')
		out.Write(m.value)
	}
	out.WriteByte('}')
	return nil
}

// quadraticFoldKey 把键里每个字符换成它的 unicode.SimpleFold 环里最小的那个。有的解码器（例如 Go 的 encoding/json
// 解到结构体）认键名时不区分大小写、做 Unicode 简单折叠，后出现的为准：{"Temperature":0.1,"temperature":0.9}
// 和换了先后的写法，它读到的值不同。所以排序只看折叠后的键，折叠后相同的成员保持请求体里的先后，
// 不能再按原始字节分先后，否则这两种写法又会排成同一个规范形式。
func quadraticFoldKey(key string) string {
	return strings.Map(func(r rune) rune {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			least = min(least, f)
		}
		return least
	}, key)
}

func quadraticWriteString(out *bytes.Buffer, s string) {
	enc, _ := json.Marshal(s) // 合法 UTF-8 的字符串编码不会失败
	out.Write(enc)
}

// quadraticHasLoneSurrogate 报告请求体里有没有孤立的代理项转义：高位后面没跟低位，或者低位单独出现。
// 合法的 JSON 里反斜杠只出现在字符串里，所以从头扫一遍，每个反斜杠连同它的转义一起跳过就行。
func quadraticHasLoneSurrogate(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		r, ok := quadraticUnicodeEscape(b, i)
		switch {
		case !ok: // 其它转义：跳过反斜杠后面那个字符
			i++
		case r >= 0xd800 && r < 0xdc00:
			if low, ok := quadraticUnicodeEscape(b, i+6); !ok || low < 0xdc00 || low >= 0xe000 {
				return true
			}
			i += 11
		case r >= 0xdc00 && r < 0xe000:
			return true
		default:
			i += 5
		}
	}
	return false
}

// quadraticUnicodeEscape 读 b[i:] 开头的 \uXXXX。
func quadraticUnicodeEscape(b []byte, i int) (rune, bool) {
	if i+6 > len(b) || b[i] != '\\' || b[i+1] != 'u' {
		return 0, false
	}
	n, err := strconv.ParseUint(string(b[i+2:i+6]), 16, 32)
	return rune(n), err == nil
}
