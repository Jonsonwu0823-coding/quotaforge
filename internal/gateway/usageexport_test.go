package gateway

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
)

// 用 QF 真实写出的日志做输入：日志格式一变，这里就会失败。
func TestExportUsage(t *testing.T) {
	h := newHarness(t)
	ids := map[string]string{}
	call := func(name, path, key, body string, headers ...string) {
		r := h.post(context.Background(), path, key, body, headers...)
		if r.err != nil {
			t.Fatal(r.err)
		}
		ids[name] = requestID(t, r)
	}
	call("chat", "/v1/chat/completions", clientKey, `{"model":"shared","messages":[{"content":"abc"}]}`,
		"X-Client-Request-Id", "cr-1", "X-Run-Id", "run,with \"comma\"")
	call("embeddings", "/v1/embeddings", clientKey, `{"model":"shared","input":["一二三","ab"]}`)
	call("no usage", "/v1/chat/completions", clientKey, `{"model":"fake-no-usage"}`)
	call("upstream 500", "/v1/chat/completions", clientKey, `{"model":"fake-500"}`)
	call("not found", "/v1/chat/completions", clientKey, `{"model":"gpt-x"}`)
	call("bad key", "/v1/embeddings", "qfk_wrong", `{"model":"shared"}`)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"shared"}`))
	req.Header.Set("Authorization", "Bearer "+clientKey)
	rec := httptest.NewRecorder()
	h.qf.Config.Handler.ServeHTTP(rec, req)
	ids["canceled"] = rec.Header().Get("x-request-id")
	waitFor(t, func() bool { return len(h.logs.lines(t)) == len(ids) })

	// 混进启动日志、别的非调用日志、空行、不是 JSON 的行和字段不对的调用行。
	input := `{"time":"t","level":"INFO","msg":"listening","addr":"0.0.0.0:8080"}` + "\n\n" +
		`{"level":"ERROR","msg":"run","err":"x"}` + "\n" +
		"quotaforge-1  | not json\n" +
		h.logs.String() +
		`{"msg":"call","request_id":"qf-bad-usage","usage":0}` + "\n" +
		`{"msg":"call","request_id":"qf-bad-usage-string","usage":"0"}` + "\n" +
		`{"msg":"call","request_id":"qf-usage-missing-total","usage":{"prompt_tokens":1}}` + "\n" +
		`{"msg":"call","status":"200","request_id":"qf-bad-status"}` + "\n" +
		`{"msg":"call","endpoint":"chat"}` + "\n" +
		`{"msg":"call",` + "\n"
	var out bytes.Buffer
	invalid, err := ExportUsage(strings.NewReader(input), &out)
	if err != nil {
		t.Fatal(err)
	}
	if invalid != 7 {
		t.Errorf("invalid = %d, want 7", invalid)
	}

	records, err := csv.NewReader(&out).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(records[0], ",") != strings.Join(usageColumns, ",") {
		t.Fatalf("header = %v", records[0])
	}
	if len(records)-1 != len(ids) {
		t.Fatalf("%d rows, want %d", len(records)-1, len(ids))
	}
	rows := map[string]map[string]string{}
	for _, rec := range records[1:] {
		row := map[string]string{}
		for i, col := range usageColumns {
			row[col] = rec[i]
		}
		rows[row["request_id"]] = row
	}

	// 列：status, error_code, prompt, completion, total
	want := map[string][5]string{
		"chat":         {"200", "", "3", "10", "13"},
		"embeddings":   {"200", "", "5", "", "5"},
		"no usage":     {"200", "", "unknown", "unknown", "unknown"},
		"upstream 500": {"502", "upstream_error", "unknown", "unknown", "unknown"},
		"not found":    {"404", "model_not_found", "", "", ""},
		"bad key":      {"401", "invalid_api_key", "", "", ""},
		// 转发之前客户端已经断开：不调上游（预留随即释放），没写回状态，usage 留空。
		"canceled": {"", "client_canceled", "", "", ""},
	}
	for name, w := range want {
		row := rows[ids[name]]
		got := [5]string{row["status"], row["error_code"], row["prompt_tokens"], row["completion_tokens"], row["total_tokens"]}
		if got != w {
			t.Errorf("%s: got %q, want %q", name, got, w)
		}
	}

	chat := rows[ids["chat"]]
	if chat["client_request_id"] != "cr-1" || chat["run_id"] != `run,with "comma"` || chat["endpoint"] != "chat" ||
		chat["model_alias"] != "shared" || chat["upstream_model"] != "fake-chat"+fakeupstream.ModelVersionSuffix ||
		chat["time"] == "" || chat["duration_ms"] == "" {
		t.Errorf("chat row = %v", chat)
	}
	if rows[ids["embeddings"]]["endpoint"] != "embeddings" {
		t.Errorf("embeddings row = %v", rows[ids["embeddings"]])
	}
}

func TestExportUsageEmptyInput(t *testing.T) {
	var out bytes.Buffer
	invalid, err := ExportUsage(strings.NewReader(""), &out)
	if err != nil || invalid != 0 || out.String() != strings.Join(usageColumns, ",")+"\n" {
		t.Fatalf("invalid = %d, err = %v, out = %q", invalid, err, out.String())
	}
}

// 以公式字符开头的值前面加一个单引号，去掉它就是原值；规则作用于每个单元格，普通的行逐字节不变。
func TestExportUsageFormulaCells(t *testing.T) {
	const plain = `{"time":"2026-10-11T00:00:00Z","msg":"call","request_id":"qf-1","client_request_id":"cr-1","run_id":"run-1",` +
		`"endpoint":"chat","model_alias":"shared","upstream_model":"fake-chat","status":502,"error_code":"upstream_error",` +
		`"usage":{"prompt_tokens":3,"completion_tokens":10,"total_tokens":13},"duration_ms":7,"replay_of":"qf-0"}`
	var out bytes.Buffer
	if _, err := ExportUsage(strings.NewReader(plain), &out); err != nil {
		t.Fatal(err)
	}
	if want := strings.Join(usageColumns, ",") + "\n" +
		"2026-10-11T00:00:00Z,qf-1,cr-1,run-1,chat,shared,fake-chat,502,upstream_error,3,10,13,7,qf-0\n"; out.String() != want {
		t.Fatalf("plain row:\n%s\nwant\n%s", out.String(), want)
	}

	external := []string{"client_request_id", "run_id", "model_alias", "upstream_model"}
	var input strings.Builder
	var values []string
	for _, prefix := range []string{"=", "+", "-", "@", "\t", "\r", "'"} {
		for _, col := range external {
			v := prefix + "1+1"
			values = append(values, v)
			line, _ := json.Marshal(map[string]any{"msg": "call", "request_id": "qf-" + col, col: v})
			input.Write(append(line, '\n'))
		}
	}
	out.Reset()
	if invalid, err := ExportUsage(strings.NewReader(input.String()), &out); err != nil || invalid != 0 {
		t.Fatalf("invalid = %d, err = %v", invalid, err)
	}
	records, err := csv.NewReader(&out).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for i, rec := range records[1:] {
		col := external[i%len(external)]
		cell := rec[slices.Index(usageColumns, col)]
		if want := values[i]; cell != "'"+want || strings.TrimPrefix(cell, "'") != want {
			t.Errorf("%s = %q, want %q with one leading quote", col, cell, want)
		}
	}
}
