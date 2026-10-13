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
	got := string(p.upstreamBody(body, `up"<x>`))
	want := `{ "a" : "<&>",  "model" :  "up\"\u003cx\u003e" , "b":[1, 2.50] }`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// 流式请求的 stream_options：发给上游的一定要 usage，只改 include_usage（或者加上它），其余字节一字不改；
// 客户端自己要了 usage 的原样。
func TestStreamOptionsRewrite(t *testing.T) {
	cases := []struct {
		body, want string
		wantsUsage bool
	}{
		{`{"model":"a","stream":true}`, `{"stream_options":{"include_usage":true},"model":"up","stream":true}`, false},
		{` { "model" : "a" , "stream":true } `, ` {"stream_options":{"include_usage":true}, "model" : "up" , "stream":true } `, false},
		{`{"stream":true,"stream_options":null,"model":"a"}`, `{"stream":true,"stream_options":{"include_usage":true},"model":"up"}`, false},
		{`{"stream":true,"stream_options":{},"model":"a"}`, `{"stream":true,"stream_options":{"include_usage":true},"model":"up"}`, false},
		{`{"stream":true,"stream_options":{ "x" : [1] },"model":"a"}`, `{"stream":true,"stream_options":{"include_usage":true, "x" : [1] },"model":"up"}`, false},
		{`{"model":"a","stream":true,"stream_options":{"x":1,"include_usage" : false}}`, `{"model":"up","stream":true,"stream_options":{"x":1,"include_usage" : true}}`, false},
		{`{"model":"a","stream":true,"stream_options":{"include_usage":null,"y":2}}`, `{"model":"up","stream":true,"stream_options":{"include_usage":true,"y":2}}`, false},
		{`{"model":"a","stream":true,"stream_options":{"include_usage": true}}`, `{"model":"up","stream":true,"stream_options":{"include_usage": true}}`, true},
		// 另一个对象里的 include_usage 不算
		{`{"model":"a","stream":true,"x":{"include_usage":true}}`, `{"stream_options":{"include_usage":true},"model":"up","stream":true,"x":{"include_usage":true}}`, false},
	}
	for _, tc := range cases {
		p, reason := parseRequest([]byte(tc.body))
		if reason == "" {
			reason = p.checkStreamOptions()
		}
		if reason != "" {
			t.Errorf("%s: %s", tc.body, reason)
			continue
		}
		if got := string(p.upstreamBody([]byte(tc.body), "up")); got != tc.want || p.wantsUsage != tc.wantsUsage {
			t.Errorf("%s:\ngot  %s (wants usage %v)\nwant %s (wants usage %v)", tc.body, got, p.wantsUsage, tc.want, tc.wantsUsage)
		}
	}
}

// 流式时 stream_options 和 include_usage 照 QF 读的字段的规则：变体和重复拒绝；类型不对的拒绝。非流式请求不看它们。
func TestStreamOptionsRejects(t *testing.T) {
	cases := map[string]invalidReason{
		`{"model":"a","stream":true,"stream_options":{},"stream_options":{}}`:                             reasonAmbiguousField,
		`{"model":"a","stream":true,"Stream_Options":{}}`:                                                 reasonAmbiguousField,
		`{"model":"a","stream":true,"stream_options":{},"ſtream_options":{}}`:                             reasonAmbiguousField,
		`{"model":"a","stream":true,"stream_options":{"include_usage":true,"Include_Usage":false}}`:       reasonAmbiguousField,
		`{"model":"a","stream":true,"stream_options":{"include_usage":true,"include_usage":true}}`:        reasonAmbiguousField,
		`{"model":"a","stream":true,"stream_options":"yes"}`:                                              reasonStreamOptionsInvalid,
		`{"model":"a","stream":true,"stream_options":[]}`:                                                 reasonStreamOptionsInvalid,
		`{"model":"a","stream":true,"stream_options":{"include_usage":"true"}}`:                           reasonStreamOptionsInvalid,
		`{"model":"a","stream":true,"stream_options":{"include_usage":1}}`:                                reasonStreamOptionsInvalid,
		`{"model":"a","stream":false,"stream_options":"yes","Stream_Options":{"include_usage":1}}`:        "",
		`{"model":"a","stream_options":{"include_usage":1},"stream_options":{"include_usage":1}}`:         "",
		`{"model":"a","stream":null,"stream_options":[],"stream_options":{"include_usage":true,"x":"y"}}`: "",
	}
	for body, want := range cases {
		p, reason := parseRequest([]byte(body))
		if reason != "" {
			t.Errorf("%s: parse %s", body, reason)
			continue
		}
		if p.stream {
			reason = p.checkStreamOptions()
		}
		if reason != want {
			t.Errorf("%s: %q, want %q", body, reason, want)
		}
	}
}
