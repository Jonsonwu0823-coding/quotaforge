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
		p, err := parseRequest([]byte(tc.body))
		if err != nil {
			t.Errorf("%s: %v", tc.body, err)
			continue
		}
		if p.model != tc.model || p.stream != tc.stream || tc.body[p.modelStart:p.modelEnd] != tc.value {
			t.Errorf("%s: model %q stream %v value %q", tc.body, p.model, p.stream, tc.body[p.modelStart:p.modelEnd])
		}
	}
}

func TestParseRequestRejects(t *testing.T) {
	for _, body := range []string{
		``, `null`, `"model"`, `[{"model":"a"}]`, `{"model":"a"`, `{"model":"a"}x`, `{"model":"a"}{}`,
		`{}`, `{"model":""}`, `{"model":1}`, `{"model":null}`, `{"model":["a"]}`,
		`{"model":"a","model":"a"}`, `{"model":"a","stream":true,"stream":false}`, `{"model":"a","stream":"true"}`,
		// 只差大小写或 Unicode 折叠（ſ 折叠成 s）的变体键
		`{"model":"a","MODEL":"b"}`, `{"Model":"a"}`, `{"model":"a","stream":false,"ſtream":true}`, `{"model":"a","t":[1,}`,
	} {
		if _, err := parseRequest([]byte(body)); err == nil {
			t.Errorf("parseRequest(%q) succeeded, want an error", body)
		}
	}
}

func TestWithModelChangesOnlyTheValue(t *testing.T) {
	body := []byte(`{ "a" : "<&>",  "model" :  "alias" , "b":[1, 2.50] }`)
	p, err := parseRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	// 上游模型名先做 JSON 编码再写进去。
	got := string(p.withModel(body, `up"<x>`))
	want := `{ "a" : "<&>",  "model" :  "up\"\u003cx\u003e" , "b":[1, 2.50] }`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
