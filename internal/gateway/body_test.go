package gateway

import "testing"

func TestParseRequestFindsModelValue(t *testing.T) {
	cases := []struct {
		body, model, value string
		stream             bool
	}{
		{`{"model":"a"}`, "a", `"a"`, false},
		{"{ \"model\" \t:\n  \"a\"  , \"stream\" : false }", "a", `"a"`, false},
		{`{"x":{"model":"inner"},"model":"outer","stream":true}`, "outer", `"outer"`, true},
		{`{"model":"sh\u0061red"}`, "shared", `"sh\u0061red"`, false},
		{`{"stream":null,"model":"中文<&>"}`, "中文<&>", `"中文<&>"`, false},
		{"{\"model\":\"a\"}\n\t ", "a", `"a"`, false},
	}
	for _, tc := range cases {
		p, reason := parseRequest([]byte(tc.body))
		if reason != "" {
			t.Errorf("%s: %s", tc.body, reason)
			continue
		}
		if p.model != tc.model || p.stream != tc.stream || tc.body[p.modelStart:p.modelEnd] != tc.value {
			t.Errorf("%s: model %q stream %v value %q", tc.body, p.model, p.stream, tc.body[p.modelStart:p.modelEnd])
		}
	}
}

// 每种不合格的请求体给出固定的原因（进调用日志的 invalid_reason），不是 encoding/json 的错误原文。
func TestParseRequestRejects(t *testing.T) {
	cases := map[string]invalidReason{
		``: reasonNotObject, `null`: reasonNotObject, `"model"`: reasonNotObject, `[{"model":"a"}]`: reasonNotObject,
		`{"model":"a"`: reasonMalformedJSON, `{"model":"a","t":[1,}`: reasonMalformedJSON, `{"model":"a",}`: reasonMalformedJSON,
		`{"model":"a"}x`: reasonTrailingData, `{"model":"a"}{}`: reasonTrailingData,
		`{}`: reasonModelMissing, `{"model":""}`: reasonModelMissing,
		`{"model":1}`: reasonModelNotString, `{"model":null}`: reasonModelNotString, `{"model":["a"]}`: reasonModelNotString,
		`{"model":"a","model":"a"}`: reasonAmbiguousField, `{"model":"a","stream":true,"stream":false}`: reasonAmbiguousField,
		// 只差大小写或 Unicode 折叠（ſ 折叠成 s）的变体键
		`{"model":"a","MODEL":"b"}`: reasonAmbiguousField, `{"Model":"a"}`: reasonAmbiguousField,
		`{"model":"a","stream":false,"ſtream":true}`: reasonAmbiguousField,
		`{"model":"a","stream":"true"}`:              reasonStreamNotBoolean,
		`{"model":"a","max_tokens":1.5}`:             reasonMaxTokensInvalid, `{"model":"a","max_tokens":-1}`: reasonMaxTokensInvalid,
		`{"model":"a","max_tokens":"8"}`: reasonMaxTokensInvalid, `{"model":"a","max_tokens":2147483648}`: reasonMaxTokensInvalid,
	}
	for body, want := range cases {
		if _, got := parseRequest([]byte(body)); got != want {
			t.Errorf("parseRequest(%q) = %q, want %q", body, got, want)
		}
	}
}

func TestWithModelChangesOnlyTheValue(t *testing.T) {
	body := []byte(`{ "a" : "<&>",  "model" :  "alias" , "b":[1, 2.50] }`)
	p, reason := parseRequest(body)
	if reason != "" {
		t.Fatal(reason)
	}
	// 上游模型名先做 JSON 编码再写进去。
	got := string(p.withModel(body, `up"<x>`))
	want := `{ "a" : "<&>",  "model" :  "up\"\u003cx\u003e" , "b":[1, 2.50] }`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
