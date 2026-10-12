package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

const (
	testDSN      = "qf_app:dsn-SECRET@tcp(127.0.0.1:3306)/quotaforge"
	testRedisURL = "redis://:redis-SECRET@127.0.0.1:6379/0"
)

var testEnv = env(map[string]string{"UP_KEY": "upstream-secret", "UP_URL": "https://example.test/v1", "DB_DSN": testDSN, "REDIS_URL": testRedisURL})

// base 是一份合法配置，各个用例在它上面改一处。
func base() map[string]any {
	return map[string]any{
		"upstreams": []any{
			map[string]any{"name": "up", "base_url": "http://127.0.0.1:8081/v1/", "api_key_env": "UP_KEY"},
		},
		"models": []any{
			map[string]any{"alias": "same", "purpose": "chat", "upstream": "up", "upstream_model": "chat-model", "default_max_tokens": 512},
			map[string]any{"alias": "same", "purpose": "embeddings", "upstream": "up", "upstream_model": "embed-model"},
		},
		"database": map[string]any{"dsn_env": "DB_DSN"},
		"redis":    map[string]any{"url_env": "REDIS_URL"},
	}
}

func item(m map[string]any, list string) map[string]any { return m[list].([]any)[0].(map[string]any) }

func parseMap(t *testing.T, m map[string]any) (*Config, error) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return parse(data, testEnv)
}

func TestParseValid(t *testing.T) {
	c, err := parseMap(t, base())
	if err != nil {
		t.Fatal(err)
	}
	if c.Timeout(Chat) != 25*time.Second || c.Timeout(Embeddings) != 25*time.Second {
		t.Fatalf("default timeouts = %v, %v; want 25s, 25s", c.Timeout(Chat), c.Timeout(Embeddings))
	}
	// 回收的上界：最长的上游超时 25 秒 + 3 次账本操作各 2 秒 + 余量 5 秒；优雅退出的上限和它相同。
	if c.ReclaimAfter() != 36*time.Second || c.ShutdownTimeout() != 36*time.Second {
		t.Fatalf("ReclaimAfter = %v, ShutdownTimeout = %v, want 36s", c.ReclaimAfter(), c.ShutdownTimeout())
	}
	// 同一个别名分别配给两种用途，各按自己的配置。
	chat, emb := c.Model(Chat, "same"), c.Model(Embeddings, "same")
	if chat == nil || emb == nil || chat.UpstreamModel != "chat-model" || emb.UpstreamModel != "embed-model" {
		t.Fatalf("models = %+v, %+v", chat, emb)
	}
	if got := chat.Upstream.URL(Chat); got != "http://127.0.0.1:8081/v1/chat/completions" {
		t.Fatalf("chat URL = %q", got)
	}
	if got := emb.Upstream.URL(Embeddings); got != "http://127.0.0.1:8081/v1/embeddings" {
		t.Fatalf("embeddings URL = %q", got)
	}
	if chat.Upstream.APIKey != "upstream-secret" {
		t.Fatal("upstream key not read from the environment")
	}
	if chat.DefaultMaxTokens != 512 || emb.DefaultMaxTokens != 0 {
		t.Fatalf("default_max_tokens = %d, %d", chat.DefaultMaxTokens, emb.DefaultMaxTokens)
	}
	if c.DatabaseDSN() != testDSN {
		t.Fatal("database DSN not read from the environment")
	}
	if c.RedisURL() != testRedisURL {
		t.Fatal("Redis URL not read from the environment")
	}
	if chat.Upstream.RPM != 0 || chat.Upstream.TPM != 0 {
		t.Fatalf("upstream limits without rpm and tpm = %d, %d; want 0 (no limit)", chat.Upstream.RPM, chat.Upstream.TPM)
	}
}

func TestParseRedisURLFile(t *testing.T) {
	m := base()
	m["redis"] = map[string]any{"url_file": writeFile(t, testRedisURL+"\n")}
	c, err := parseMap(t, m)
	if err != nil {
		t.Fatal(err)
	}
	if c.RedisURL() != testRedisURL {
		t.Fatal("Redis URL not read from the file")
	}
}

// 上游的限额：0 到 10^12 的整数，0 表示不限。
func TestParseUpstreamRate(t *testing.T) {
	for _, n := range []int64{0, 1, MaxRate} {
		m := base()
		item(m, "upstreams")["rpm"], item(m, "upstreams")["tpm"] = n, n
		c, err := parseMap(t, m)
		if err != nil {
			t.Fatalf("rpm = tpm = %d: %v", n, err)
		}
		if up := c.Model(Chat, "same").Upstream; up.RPM != n || up.TPM != n {
			t.Fatalf("limits = %d, %d; want %d", up.RPM, up.TPM, n)
		}
	}
	for _, field := range []string{"rpm", "tpm"} {
		for _, bad := range []any{-1, MaxRate + 1, 1.5, "10"} {
			m := base()
			item(m, "upstreams")[field] = bad
			if _, err := parseMap(t, m); err == nil {
				t.Errorf("%s = %v was accepted", field, bad)
			}
		}
	}
}

func TestValidRate(t *testing.T) {
	for n, want := range map[int64]bool{-1: false, 0: true, 1: true, MaxRate: true, MaxRate + 1: false} {
		if ValidRate(n) != want {
			t.Errorf("ValidRate(%d) = %v", n, !want)
		}
	}
}

func TestParseDatabaseDSNFile(t *testing.T) {
	m := base()
	m["database"] = map[string]any{"dsn_file": writeFile(t, testDSN+"\n")}
	c, err := parseMap(t, m)
	if err != nil {
		t.Fatal(err)
	}
	if c.DatabaseDSN() != testDSN {
		t.Fatal("database DSN not read from the file")
	}
}

func TestParseTimeouts(t *testing.T) {
	m := base()
	m["timeouts"] = map[string]any{"chat_ms": 100, "embeddings_ms": 40000}
	c, err := parseMap(t, m)
	if err != nil {
		t.Fatal(err)
	}
	if c.Timeout(Chat) != 100*time.Millisecond || c.Timeout(Embeddings) != 40*time.Second {
		t.Fatalf("timeouts = %v, %v", c.Timeout(Chat), c.Timeout(Embeddings))
	}
	if c.ReclaimAfter() != 51*time.Second || c.ShutdownTimeout() != 51*time.Second {
		t.Fatalf("ReclaimAfter = %v, ShutdownTimeout = %v, want 51s", c.ReclaimAfter(), c.ShutdownTimeout())
	}
}

// 关闭上限要容得下一个请求最坏的情况：4 次账本操作（查 Key、预留、发送意图、收尾）、2 次限流操作（准入、修正），
// 再加较大的上游超时。默认配置测一次；上游超时没有上限，再用 24 小时测一次（两边都随它变，差值和它无关）。
// 把 RateLimitTimeout 改成 2 秒、或者少算一次账本操作，都过不了。
func TestShutdownCoversTheWorstRequest(t *testing.T) {
	long := base()
	long["timeouts"] = map[string]any{"chat_ms": 1000, "embeddings_ms": 24 * 3600 * 1000}
	for name, m := range map[string]map[string]any{"default": base(), "24h upstream timeout": long} {
		c, err := parseMap(t, m)
		if err != nil {
			t.Fatal(err)
		}
		worst := 4*LedgerTimeout + 2*RateLimitTimeout + max(c.Timeout(Chat), c.Timeout(Embeddings))
		if worst > c.ShutdownTimeout() {
			t.Errorf("%s: the worst request takes %v, longer than the shutdown cap %v", name, worst, c.ShutdownTimeout())
		}
	}
}

func TestParseBaseURLFromEnv(t *testing.T) {
	m := base()
	u := item(m, "upstreams")
	delete(u, "base_url")
	u["base_url_env"] = "UP_URL"
	c, err := parseMap(t, m)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Model(Chat, "same").Upstream.URL(Chat); got != "https://example.test/v1/chat/completions" {
		t.Fatalf("chat URL = %q", got)
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(m map[string]any)
		want   string
	}{
		{"unknown field", func(m map[string]any) { m["timeout"] = 5 }, "unknown field"},
		{"negative timeout", func(m map[string]any) { m["timeouts"] = map[string]any{"chat_ms": -1} }, "timeouts.chat_ms"},
		{"no upstreams", func(m map[string]any) { delete(m, "upstreams") }, "upstreams"},
		{"upstream without name", func(m map[string]any) { delete(item(m, "upstreams"), "name") }, "name is required"},
		{"duplicate upstream", func(m map[string]any) {
			m["upstreams"] = append(m["upstreams"].([]any), item(m, "upstreams"))
		}, "duplicate name"},
		{"no base url", func(m map[string]any) { delete(item(m, "upstreams"), "base_url") }, "one of base_url and base_url_env"},
		{"both base urls", func(m map[string]any) { item(m, "upstreams")["base_url_env"] = "UP_URL" }, "only one of"},
		{"base url env unset", func(m map[string]any) {
			u := item(m, "upstreams")
			delete(u, "base_url")
			u["base_url_env"] = "MISSING_URL"
		}, "MISSING_URL is not set"},
		{"base url scheme", func(m map[string]any) { item(m, "upstreams")["base_url"] = "ftp://host/v1" }, "base URL"},
		{"base url without host", func(m map[string]any) { item(m, "upstreams")["base_url"] = "http:///v1" }, "base URL"},
		{"base url with query", func(m map[string]any) { item(m, "upstreams")["base_url"] = "http://host/v1?x=1" }, "base URL"},
		{"no api key source", func(m map[string]any) { delete(item(m, "upstreams"), "api_key_env") }, "one of api_key_env and api_key_file is required"},
		{"both api key sources", func(m map[string]any) { item(m, "upstreams")["api_key_file"] = "key.txt" }, "only one of api_key_env and api_key_file"},
		{"api key env unset", func(m map[string]any) { item(m, "upstreams")["api_key_env"] = "MISSING_KEY" }, "MISSING_KEY is not set"},
		{"no models", func(m map[string]any) { m["models"] = []any{} }, "models"},
		{"model without alias", func(m map[string]any) { delete(item(m, "models"), "alias") }, "alias is required"},
		{"bad purpose", func(m map[string]any) { item(m, "models")["purpose"] = "rerank" }, "purpose"},
		{"unknown upstream", func(m map[string]any) { item(m, "models")["upstream"] = "nope" }, `upstream "nope" is not defined`},
		{"no upstream model", func(m map[string]any) { delete(item(m, "models"), "upstream_model") }, "upstream_model"},
		{"duplicate alias in purpose", func(m map[string]any) {
			m["models"] = append(m["models"].([]any), item(m, "models"))
		}, `alias "same" is already defined for chat`},
		{"keys are no longer in the config", func(m map[string]any) { m["keys"] = []any{} }, "unknown field"},
		{"chat model without default_max_tokens", func(m map[string]any) { delete(item(m, "models"), "default_max_tokens") }, "default_max_tokens must be a positive integer"},
		{"zero default_max_tokens", func(m map[string]any) { item(m, "models")["default_max_tokens"] = 0 }, "default_max_tokens must be a positive integer"},
		{"embeddings model with default_max_tokens", func(m map[string]any) {
			m["models"].([]any)[1].(map[string]any)["default_max_tokens"] = 5
		}, "default_max_tokens is only for chat models"},
		{"alias too long", func(m map[string]any) { item(m, "models")["alias"] = strings.Repeat("a", 256) }, "at most 255"},
		{"no database", func(m map[string]any) { delete(m, "database") }, "database: one of dsn_env and dsn_file is required"},
		{"both dsn sources", func(m map[string]any) { m["database"] = map[string]any{"dsn_env": "DB_DSN", "dsn_file": "x"} }, "database: set only one of dsn_env and dsn_file"},
		{"dsn env unset", func(m map[string]any) { m["database"] = map[string]any{"dsn_env": "MISSING_DSN"} }, "MISSING_DSN is not set"},
		{"dsn file missing", func(m map[string]any) { m["database"] = map[string]any{"dsn_file": "/nonexistent/dsn"} }, "read dsn_file"},
		{"no redis", func(m map[string]any) { delete(m, "redis") }, "redis: one of url_env and url_file is required"},
		{"both redis sources", func(m map[string]any) { m["redis"] = map[string]any{"url_env": "REDIS_URL", "url_file": "x"} }, "redis: set only one of url_env and url_file"},
		{"redis env unset", func(m map[string]any) { m["redis"] = map[string]any{"url_env": "MISSING_URL"} }, "MISSING_URL is not set"},
		{"redis file missing", func(m map[string]any) { m["redis"] = map[string]any{"url_file": "/nonexistent/url"} }, "read url_file"},
		{"negative upstream rpm", func(m map[string]any) { item(m, "upstreams")["rpm"] = -1 }, "rpm and tpm must be integers from 0"},
		{"upstream tpm too large", func(m map[string]any) { item(m, "upstreams")["tpm"] = MaxRate + 1 }, "rpm and tpm must be integers from 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := base()
			tc.mutate(m)
			_, err := parseMap(t, m)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// 地址里的凭据不能出现在错误信息里，无论地址写在配置里还是来自环境变量。
func TestBaseURLErrorHidesCredentials(t *testing.T) {
	m := base()
	item(m, "upstreams")["base_url"] = "http://user:hunter2@host/v1"
	_, err := parseMap(t, m)
	if err == nil || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "host/v1") {
		t.Fatalf("err = %v", err)
	}

	m = base()
	u := item(m, "upstreams")
	delete(u, "base_url")
	u["base_url_env"] = "SECRET_URL"
	_, err = parse(mustJSON(t, m), env(map[string]string{"UP_KEY": "k", "SECRET_URL": "https://user:hunter2@host/v1?token=abc"}))
	if err == nil || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "token=abc") {
		t.Fatalf("err = %v", err)
	}
}

// fileKeyConfig 返回一份上游 Key 来自文件 path 的配置。
func fileKeyConfig(path string) map[string]any {
	m := base()
	u := item(m, "upstreams")
	delete(u, "api_key_env")
	u["api_key_file"] = path
	return m
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upstream_api_key")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseAPIKeyFile(t *testing.T) {
	for _, content := range []string{"file-secret", "file-secret\n", "file-secret\r\n", " \tfile-secret \n\n"} {
		c, err := parseMap(t, fileKeyConfig(writeFile(t, content)))
		if err != nil {
			t.Fatalf("%q: %v", content, err)
		}
		if got := c.Model(Chat, "same").Upstream.APIKey; got != "file-secret" {
			t.Fatalf("%q: key = %q, want file-secret", content, got)
		}
	}
}

// 文件为空、只有空白、不存在都报错；错误信息带路径，方便排查。
func TestParseAPIKeyFileErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	cases := []struct{ name, path, want string }{
		{"empty", writeFile(t, ""), "is empty"},
		{"only whitespace", writeFile(t, " \r\n\t\n"), "is empty"},
		{"missing", missing, "read api_key_file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseMap(t, fileKeyConfig(tc.path))
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("err = %v, want it to mention %q and the path", err, tc.want)
			}
		})
	}
}

// 文件里有 Key 或连接串、但配置别处有错时，错误信息里也不能有它们。
func TestAPIKeyFileErrorHidesKey(t *testing.T) {
	m := fileKeyConfig(writeFile(t, "file-secret-XYZ\n"))
	item(m, "models")["upstream"] = "nope"
	_, err := parseMap(t, m)
	if err == nil || strings.Contains(err.Error(), "file-secret-XYZ") {
		t.Fatalf("err = %v", err)
	}
	m = base()
	m["database"] = map[string]any{"dsn_file": writeFile(t, testDSN)}
	m["timeouts"] = map[string]any{"chat_ms": -1}
	if _, err := parseMap(t, m); err == nil || strings.Contains(err.Error(), "dsn-SECRET") {
		t.Fatalf("err = %v", err)
	}
	// Redis 的地址（可能带密码）也一样：读到了它、配置却在后面出错，或者它本身后面的一项出错。
	m = base()
	m["redis"] = map[string]any{"url_file": writeFile(t, testRedisURL)}
	item(m, "upstreams")["rpm"] = -1
	if _, err := parseMap(t, m); err == nil || strings.Contains(err.Error(), "redis-SECRET") || strings.Contains(err.Error(), "127.0.0.1:6379") {
		t.Fatalf("err = %v", err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// 提交的示例配置能加载，qs-fake-upstream-v1 两种用途都有。
func TestExampleConfig(t *testing.T) {
	c, err := Load(filepath.Join("..", "..", "config.example.json"),
		env(map[string]string{"QF_FAKE_UPSTREAM_KEY": "test-only-upstream-key", "QUOTAFORGE_MYSQL_DSN": testDSN, "QUOTAFORGE_REDIS_URL": testRedisURL}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Model(Chat, "qs-fake-upstream-v1") == nil || c.Model(Embeddings, "qs-fake-upstream-v1") == nil {
		t.Fatal("qs-fake-upstream-v1 must be configured for both chat and embeddings")
	}
}

// deploy/ 下的三份配置（换上 secret 文件的路径后）能加载；联调配置符合 QS 的接法，真实配置的别名是百炼的。
func TestDeployConfigs(t *testing.T) {
	deploy := filepath.Join("..", "..", "deploy")
	keyFile := filepath.ToSlash(writeFile(t, "real-secret\n"))
	dsnFile := filepath.ToSlash(writeFile(t, testDSN+"\n"))
	cases := []struct {
		file             string
		env              map[string]string
		chat, embeddings string
		upstreamModel    string // 空：和别名相同
		baseURL, apiKey  string
		rpm, tpm         int64 // 上游的限额：冒烟和联调远高于用量，真实的是保守的示例值
	}{
		{"config.compose.json", map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL},
			"qs-fake-upstream-v1", "qs-fake-upstream-v1", "fake-chat", "http://qf-fake-upstream:8081/v1", "k", 6000, 100_000_000},
		{"config.qs-fake.json", map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL},
			"qs-fake-upstream-v1", "qs-fake-upstream-v1", "", "http://fake-upstream:8000/v1", "k", 6000, 100_000_000},
		{"config.real.json", map[string]string{"QF_UPSTREAM_BASE_URL": "https://bailian.test/v1", "QF_REDIS_URL": testRedisURL},
			"qwen-plus", "text-embedding-v4", "", "https://bailian.test/v1", "real-secret", 600, 2_000_000},
	}
	for _, tc := range cases {
		data, err := os.ReadFile(filepath.Join(deploy, tc.file))
		if err != nil {
			t.Fatal(err)
		}
		s := strings.ReplaceAll(string(data), "/run/secrets/upstream_api_key", keyFile)
		s = strings.ReplaceAll(s, "/run/secrets/app_dsn", dsnFile)
		c, err := parse([]byte(s), env(tc.env))
		if err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		want := tc.upstreamModel
		if want == "" {
			want = tc.chat
		}
		chat, emb := c.Model(Chat, tc.chat), c.Model(Embeddings, tc.embeddings)
		if chat == nil || emb == nil || chat.UpstreamModel != want {
			t.Fatalf("%s: models = %+v, %+v", tc.file, chat, emb)
		}
		if chat.Upstream.URL(Chat) != tc.baseURL+"/chat/completions" || chat.Upstream.APIKey != tc.apiKey || c.DatabaseDSN() != testDSN {
			t.Fatalf("%s: chat URL = %q, or the upstream key or DSN is not from its source", tc.file, chat.Upstream.URL(Chat))
		}
		if !strings.Contains(string(data), `"dsn_file": "/run/secrets/app_dsn"`) {
			t.Fatalf("%s: the DSN must come from the app_dsn secret", tc.file)
		}
		if c.RedisURL() != testRedisURL {
			t.Fatalf("%s: the Redis URL must come from QF_REDIS_URL (set in compose.yaml)", tc.file)
		}
		if up := chat.Upstream; up.RPM != tc.rpm || up.TPM != tc.tpm {
			t.Fatalf("%s: upstream limits = %d, %d; want %d, %d", tc.file, up.RPM, up.TPM, tc.rpm, tc.tpm)
		}
	}
}
