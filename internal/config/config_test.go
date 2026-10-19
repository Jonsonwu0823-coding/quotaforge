package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

const (
	testDSN      = "qf_app:dsn-SECRET@tcp(127.0.0.1:3306)/quotaforge"
	testRedisURL = "redis://:redis-SECRET@127.0.0.1:6379/0"
	testCacheURL = "redis://127.0.0.1:6380/0" // 缓存用另一个 Redis
)

var testEnv = env(map[string]string{"UP_KEY": "upstream-secret", "UP_URL": "https://example.test/v1", "DB_DSN": testDSN, "REDIS_URL": testRedisURL,
	"CACHE_URL": testCacheURL, "SAME_REDIS_OTHER_DB": "redis://127.0.0.1:6379/1", "SAME_REDIS_DEFAULT_PORT": "redis://127.0.0.1/2",
	"NAMED_REDIS": "redis://qf-redis:6379/0", "NAMED_REDIS_OTHER_CASE": "redis://QF-Redis:6379/1"})

// base 是一份合法配置，各个用例在它上面改一处。
func base() map[string]any {
	return map[string]any{
		"upstreams": []any{
			map[string]any{"name": "up", "base_url": "http://127.0.0.1:8081/v1/", "api_key_env": "UP_KEY"},
		},
		"models": []any{
			map[string]any{"alias": "same", "purpose": "chat", "targets": targets("up", "chat-model", 1), "default_max_tokens": 512},
			map[string]any{"alias": "same", "purpose": "embeddings", "targets": targets("up", "embed-model", 1)},
		},
		"database": map[string]any{"dsn_env": "DB_DSN"},
		"redis":    map[string]any{"url_env": "REDIS_URL"},
	}
}

func item(m map[string]any, list string) map[string]any { return m[list].([]any)[0].(map[string]any) }

// targets 按（上游，上游模型名，权重）三个一组写出 targets 列表。
func targets(spec ...any) []any {
	var out []any
	for i := 0; i < len(spec); i += 3 {
		out = append(out, map[string]any{"upstream": spec[i], "upstream_model": spec[i+1], "weight": spec[i+2]})
	}
	return out
}

// target 是第一个模型的第一个目标。
func target(m map[string]any) map[string]any {
	return item(m, "models")["targets"].([]any)[0].(map[string]any)
}

// only 是只有一个目标的模型的那个目标。
func only(t *testing.T, m *Model) Target {
	t.Helper()
	if m == nil || len(m.Targets) != 1 {
		t.Fatalf("model = %+v, want one target", m)
	}
	return m.Targets[0]
}

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
	chatModel, embModel := c.Model(Chat, "same"), c.Model(Embeddings, "same")
	chat, emb := only(t, chatModel), only(t, embModel)
	if chat.UpstreamModel != "chat-model" || emb.UpstreamModel != "embed-model" || chat.Weight != 1 {
		t.Fatalf("targets = %+v, %+v", chat, emb)
	}
	if chat.Upstream != emb.Upstream || len(c.Upstreams()) != 1 || c.Upstreams()[0] != chat.Upstream {
		t.Fatal("both targets must point to the one configured upstream")
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
	if chatModel.DefaultMaxTokens != 512 || embModel.DefaultMaxTokens != 0 {
		t.Fatalf("default_max_tokens = %d, %d", chatModel.DefaultMaxTokens, embModel.DefaultMaxTokens)
	}
	if b := c.Breaker(); b != (Breaker{Failures: 5, Cooldown: 30 * time.Second}) {
		t.Fatalf("default circuit breaker = %+v, want 5 failures, 30s", b)
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
		if up := only(t, c.Model(Chat, "same")).Upstream; up.RPM != n || up.TPM != n {
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

// 一个别名可以有多个目标：按配置的顺序保留；权重 0 是备用；同一个上游可以配不同的上游模型名，两个别名可以共用上游。
func TestParseTargets(t *testing.T) {
	m := base()
	m["upstreams"] = append(m["upstreams"].([]any), map[string]any{"name": "up-b", "base_url": "http://127.0.0.1:8082/v1", "api_key_env": "UP_KEY"})
	item(m, "models")["targets"] = targets("up-b", "b-model", 9, "up", "chat-model", 1, "up", "other-model", 0)
	c, err := parseMap(t, m)
	if err != nil {
		t.Fatal(err)
	}
	got := c.Model(Chat, "same").Targets
	ups := c.Upstreams()
	if len(ups) != 2 || ups[0].Name != "up" || ups[1].Name != "up-b" {
		t.Fatalf("upstreams = %+v", ups)
	}
	want := []Target{{ups[1], "b-model", 9}, {ups[0], "chat-model", 1}, {ups[0], "other-model", 0}}
	if len(got) != len(want) {
		t.Fatalf("targets = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	if only(t, c.Model(Embeddings, "same")).Upstream != ups[0] {
		t.Fatal("the embeddings alias shares the upstream object")
	}
}

func TestParseCircuitBreaker(t *testing.T) {
	cases := []struct {
		cb   map[string]any
		want Breaker
	}{
		{map[string]any{}, Breaker{5, 30 * time.Second}},
		{map[string]any{"failure_threshold": 0, "cooldown_ms": 0}, Breaker{5, 30 * time.Second}},
		{map[string]any{"failure_threshold": 3}, Breaker{3, 30 * time.Second}},
		{map[string]any{"cooldown_ms": 5000}, Breaker{5, 5 * time.Second}},
		{map[string]any{"failure_threshold": 1000000, "cooldown_ms": 1}, Breaker{1000000, time.Millisecond}},
	}
	for _, tc := range cases {
		m := base()
		m["circuit_breaker"] = tc.cb
		c, err := parseMap(t, m)
		if err != nil {
			t.Fatalf("%v: %v", tc.cb, err)
		}
		if c.Breaker() != tc.want {
			t.Fatalf("%v: breaker = %+v, want %+v", tc.cb, c.Breaker(), tc.want)
		}
	}
}

// 嵌入缓存：没写这一段就不开；写了要有地址，过期时长默认 24 小时。
func TestParseEmbeddingCache(t *testing.T) {
	c, err := parseMap(t, base())
	if err != nil || c.EmbeddingCache() != nil {
		t.Fatalf("without embedding_cache: %+v, %v; want off", c.EmbeddingCache(), err)
	}
	file := filepath.ToSlash(writeFile(t, testCacheURL+"\n"))
	ok := []struct {
		section map[string]any
		want    EmbeddingCache
	}{
		{map[string]any{"url_env": "CACHE_URL"}, EmbeddingCache{testCacheURL, 24 * time.Hour}},
		{map[string]any{"url_env": "CACHE_URL", "ttl_ms": 0}, EmbeddingCache{testCacheURL, 24 * time.Hour}},
		{map[string]any{"url_file": file, "ttl_ms": 5000}, EmbeddingCache{testCacheURL, 5 * time.Second}},
	}
	for _, tc := range ok {
		m := base()
		m["embedding_cache"] = tc.section
		c, err := parseMap(t, m)
		if err != nil || c.EmbeddingCache() == nil || *c.EmbeddingCache() != tc.want {
			t.Fatalf("%v: got %+v, %v", tc.section, c.EmbeddingCache(), err)
		}
	}
	bad := []map[string]any{
		{},
		{"ttl_ms": 1000},
		{"url_env": "CACHE_URL", "url_file": file},
		{"url_env": "NOT_SET"},
		{"url_env": "CACHE_URL", "ttl_ms": -1},
		{"url_env": "CACHE_URL", "max_body": 1},
		{"url_env": "REDIS_URL"},
		{"url_env": "SAME_REDIS_OTHER_DB"},
		{"url_env": "SAME_REDIS_DEFAULT_PORT"},
	}
	for _, section := range bad {
		m := base()
		m["embedding_cache"] = section
		_, err := parseMap(t, m)
		if err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%v: err = %v, want an error without the URL", section, err)
		}
	}
}

// 用量事件：没写这一段就不开；写了要有至少一个“主机:端口”的 broker。
func TestParseUsageEvents(t *testing.T) {
	c, err := parseMap(t, base())
	if err != nil || c.UsageEvents() != nil {
		t.Fatalf("without usage_events: %+v, %v; want off", c.UsageEvents(), err)
	}
	m := base()
	m["usage_events"] = map[string]any{"brokers": []any{"qf-kafka:9092", "10.0.0.2:19092"}}
	c, err = parseMap(t, m)
	if err != nil || c.UsageEvents() == nil || !slices.Equal(c.UsageEvents().Brokers, []string{"qf-kafka:9092", "10.0.0.2:19092"}) {
		t.Fatalf("usage_events = %+v, %v", c.UsageEvents(), err)
	}
	for _, section := range []map[string]any{
		{},
		{"brokers": []any{}},
		{"brokers": []any{"qf-kafka"}},
		{"brokers": []any{":9092"}},
		{"brokers": []any{"qf-kafka:0"}},
		{"brokers": []any{"qf-kafka:65536"}},
		{"brokers": []any{"qf-kafka:http"}},
		{"brokers": []any{"qf-kafka:9092"}, "topic": "x"},
	} {
		m := base()
		m["usage_events"] = section
		if _, err := parseMap(t, m); err == nil {
			t.Errorf("%v: accepted", section)
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

// 关闭上限要容得下一个请求读完请求体之后最坏的情况：4 次账本操作（查 Key、预留、发送意图、收尾）、2 次限流操作（准入、修正）、
// 2 次缓存操作（开了缓存的嵌入：读、写），再加较大的上游超时。默认配置测一次；上游超时没有上限，再用 24 小时测一次
// （两边都随它变，差值和它无关）。把 RateLimitTimeout 改成 2 秒、或者少算一次账本操作，都过不了。
// 不算在里面的两段，都不影响账本：
//   - 读请求体：在 handler 里、预留之前，最多是服务器的 ReadTimeout（30 秒）。排空开始时还在上传的请求可能被截断，那时什么都还没预留。
//   - 写回：每次写最多 5 秒（网关的 writeTimeout），除了流式的事件（它们有写的上限管着，算在流里），写回都在收尾之后，被截断的写回
//     不影响账本，带幂等键的重试拿到重放。
func TestShutdownCoversTheWorstRequest(t *testing.T) {
	long := base()
	long["timeouts"] = map[string]any{"chat_ms": 1000, "embeddings_ms": 24 * 3600 * 1000}
	for name, m := range map[string]map[string]any{"default": base(), "24h upstream timeout": long} {
		c, err := parseMap(t, m)
		if err != nil {
			t.Fatal(err)
		}
		worst := 4*LedgerTimeout + 2*RateLimitTimeout + 2*CacheTimeout + max(c.Timeout(Chat), c.Timeout(Embeddings))
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
	if got := only(t, c.Model(Chat, "same")).Upstream.URL(Chat); got != "https://example.test/v1/chat/completions" {
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
		{"unknown upstream", func(m map[string]any) { target(m)["upstream"] = "nope" }, `models[0]: targets[0]: upstream "nope" is not defined`},
		{"no upstream model", func(m map[string]any) { delete(target(m), "upstream_model") }, "targets[0]: upstream_model is required"},
		{"upstream model too long", func(m map[string]any) { target(m)["upstream_model"] = strings.Repeat("m", 256) }, "at most 255"},
		{"no targets", func(m map[string]any) { delete(item(m, "models"), "targets") }, "models[0]: targets: at least one is required"},
		{"empty targets", func(m map[string]any) { item(m, "models")["targets"] = []any{} }, "targets: at least one is required"},
		// 旧的写法（模型上直接写上游）不再认：严格解码直接报错，不会悄悄换了意思。
		{"old upstream field", func(m map[string]any) { item(m, "models")["upstream"] = "up" }, `unknown field "upstream"`},
		{"old upstream_model field", func(m map[string]any) { item(m, "models")["upstream_model"] = "x" }, `unknown field "upstream_model"`},
		{"negative weight", func(m map[string]any) { target(m)["weight"] = -1 }, "weight must be an integer from 0 (standby) to 1000"},
		{"weight too large", func(m map[string]any) { target(m)["weight"] = 1001 }, "weight must be an integer from 0"},
		{"fractional weight", func(m map[string]any) { target(m)["weight"] = 1.5 }, "weight"},
		{"only standby targets", func(m map[string]any) {
			item(m, "models")["targets"] = targets("up", "a", 0, "up", "b", 0)
		}, "at least one weight must be greater than 0"},
		{"no weight", func(m map[string]any) { delete(target(m), "weight") }, "at least one weight must be greater than 0"},
		{"duplicate target", func(m map[string]any) {
			item(m, "models")["targets"] = targets("up", "chat-model", 1, "up", "chat-model", 0)
		}, `targets[1]: upstream "up" with upstream_model "chat-model" is already a target`},
		{"negative failure threshold", func(m map[string]any) { m["circuit_breaker"] = map[string]any{"failure_threshold": -1} }, "circuit_breaker"},
		{"negative cooldown", func(m map[string]any) { m["circuit_breaker"] = map[string]any{"cooldown_ms": -1} }, "circuit_breaker"},
		{"unknown circuit breaker field", func(m map[string]any) { m["circuit_breaker"] = map[string]any{"failures": 3} }, "unknown field"},
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
		{"cache on the rate limiter's redis", func(m map[string]any) { m["embedding_cache"] = map[string]any{"url_env": "SAME_REDIS_OTHER_DB"} },
			"embedding_cache: must not use the same Redis server (host:port) as redis"},
		{"cache on the rate limiter's redis, host name in another case", func(m map[string]any) {
			m["redis"] = map[string]any{"url_env": "NAMED_REDIS"}
			m["embedding_cache"] = map[string]any{"url_env": "NAMED_REDIS_OTHER_CASE"}
		}, "embedding_cache: must not use the same Redis server (host:port) as redis"},
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
		if got := only(t, c.Model(Chat, "same")).Upstream.APIKey; got != "file-secret" {
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
	target(m)["upstream"] = "nope"
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
// 给 QS 用的两份开着嵌入缓存（地址来自 QF_REDIS_CACHE_URL，过期用默认的 24 小时），冒烟的不开。
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
		cache            bool
	}{
		{"config.compose.json", map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL},
			"qs-fake-upstream-v1", "qs-fake-upstream-v1", "fake-chat", "http://qf-fake-upstream:8081/v1", "k", 6000, 100_000_000, false},
		{"config.qs-fake.json", map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL, "QF_REDIS_CACHE_URL": testCacheURL},
			"qs-fake-upstream-v1", "qs-fake-upstream-v1", "", "http://fake-upstream:8000/v1", "k", 6000, 100_000_000, true},
		{"config.real.json", map[string]string{"QF_UPSTREAM_BASE_URL": "https://bailian.test/v1", "QF_REDIS_URL": testRedisURL, "QF_REDIS_CACHE_URL": testCacheURL},
			"qwen-plus", "text-embedding-v4", "", "https://bailian.test/v1", "real-secret", 600, 2_000_000, true},
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
		// 自带的配置都是一个上游、每个别名一个目标
		chat, emb := only(t, c.Model(Chat, tc.chat)), only(t, c.Model(Embeddings, tc.embeddings))
		if chat.UpstreamModel != want || len(c.Upstreams()) != 1 || emb.Upstream != chat.Upstream {
			t.Fatalf("%s: targets = %+v, %+v", tc.file, chat, emb)
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
		if got, want := c.EmbeddingCache(), (&EmbeddingCache{URL: testCacheURL, TTL: 24 * time.Hour}); (got != nil) != tc.cache || tc.cache && *got != *want {
			t.Fatalf("%s: embedding cache = %+v, want it on (%v) as %+v", tc.file, got, tc.cache, want)
		}
		// 给 QS 用的部署不打开用量事件：除了结清时一条什么都不写的语句，行为和以前完全一样
		if c.UsageEvents() != nil {
			t.Fatalf("%s: usage events must stay off", tc.file)
		}
	}
}

func TestLoadTelemetry(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	ok := []struct {
		env  map[string]string
		want Telemetry
	}{
		{nil, Telemetry{SampleRatio: 1}},
		{map[string]string{"QUOTAFORGE_METRICS_ADDR": "0.0.0.0:9464"}, Telemetry{MetricsAddr: "0.0.0.0:9464", SampleRatio: 1}},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://qf-jaeger:4318/"},
			Telemetry{OTLPEndpoint: "http://qf-jaeger:4318", SampleRatio: 1, Instance: host}},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "https://c.test/otlp", "QUOTAFORGE_TRACE_SAMPLE_RATIO": "0.25", "QUOTAFORGE_INSTANCE": "qf-b"},
			Telemetry{OTLPEndpoint: "https://c.test/otlp", SampleRatio: 0.25, Instance: "qf-b"}},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://c:4318", "QUOTAFORGE_TRACE_SAMPLE_RATIO": "0"},
			Telemetry{OTLPEndpoint: "http://c:4318", SampleRatio: 0, Instance: host}},
	}
	for _, tc := range ok {
		got, err := LoadTelemetry(env(tc.env))
		if err != nil || got != tc.want {
			t.Errorf("%v: got %+v, %v; want %+v", tc.env, got, err, tc.want)
		}
	}
	// 报错说明是哪个变量，不带它的值（地址里可能有凭据）。
	bad := []struct {
		env      map[string]string
		variable string
	}{
		{map[string]string{"QUOTAFORGE_METRICS_ADDR": "9464-SECRET"}, "QUOTAFORGE_METRICS_ADDR"},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://u:xxx@c:4318"}, "QUOTAFORGE_OTLP_ENDPOINT"},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "c:4318-SECRET"}, "QUOTAFORGE_OTLP_ENDPOINT"},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://c:4318?k=SECRET"}, "QUOTAFORGE_OTLP_ENDPOINT"},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://c:4318", "QUOTAFORGE_TRACE_SAMPLE_RATIO": "1.5"}, "QUOTAFORGE_TRACE_SAMPLE_RATIO"},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://c:4318", "QUOTAFORGE_TRACE_SAMPLE_RATIO": "NaN"}, "QUOTAFORGE_TRACE_SAMPLE_RATIO"},
		{map[string]string{"QUOTAFORGE_OTLP_ENDPOINT": "http://c:4318", "QUOTAFORGE_TRACE_SAMPLE_RATIO": "-0.1"}, "QUOTAFORGE_TRACE_SAMPLE_RATIO"},
		{map[string]string{"QUOTAFORGE_TRACE_SAMPLE_RATIO": "0.5"}, "QUOTAFORGE_OTLP_ENDPOINT"},
		{map[string]string{"QUOTAFORGE_INSTANCE": "qf-b"}, "QUOTAFORGE_OTLP_ENDPOINT"},
	}
	for _, tc := range bad {
		_, err := LoadTelemetry(env(tc.env))
		if err == nil || !strings.Contains(err.Error(), tc.variable) {
			t.Errorf("%v: err = %v, want one naming %s", tc.env, err, tc.variable)
			continue
		}
		for _, v := range tc.env {
			if strings.Contains(err.Error(), v) {
				t.Errorf("%v: err = %v, shows the value", tc.env, err)
			}
		}
	}
}

// 换上游的演示配置：两个上游；split 按 9:1 分流，primary-b 是主备（fake-a 是权重 0 的备用）；熔断 5 次、冷却 5 秒。
func TestFailoverDemoConfig(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "config.failover.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := strings.ReplaceAll(string(data), "/run/secrets/app_dsn", filepath.ToSlash(writeFile(t, testDSN+"\n")))
	c, err := parse([]byte(s), env(map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL}))
	if err != nil {
		t.Fatal(err)
	}
	ups := c.Upstreams()
	if len(ups) != 2 || ups[0].URL(Chat) != "http://qf-fake-upstream:8081/v1/chat/completions" ||
		ups[1].URL(Chat) != "http://qf-fake-upstream-b:8081/v1/chat/completions" {
		t.Fatalf("upstreams = %+v", ups)
	}
	want := map[string][]Target{
		"split":     {{ups[0], "fake-chat", 9}, {ups[1], "fake-chat", 1}},
		"primary-b": {{ups[1], "fake-chat", 1}, {ups[0], "fake-chat", 0}},
	}
	for alias, w := range want {
		if got := c.Model(Chat, alias).Targets; !slices.Equal(got, w) {
			t.Fatalf("%s: targets = %+v", alias, got)
		}
	}
	// 拨号失败在 Docker Desktop 上要等名字查不到（约 4 秒），chat 的超时要留出换上游之后的时间；回收的上界不变
	if c.Breaker() != (Breaker{5, 5 * time.Second}) || c.Timeout(Chat) != 10*time.Second || c.ReclaimAfter() != 36*time.Second {
		t.Fatalf("breaker %+v, chat timeout %v, reclaim after %v", c.Breaker(), c.Timeout(Chat), c.ReclaimAfter())
	}
}

// 嵌入缓存的演示配置：两个上游、各一个嵌入别名；缓存的地址取自 QF_REDIS_CACHE_URL，条目 5 秒过期。
func TestCacheDemoConfig(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "config.cache.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := strings.ReplaceAll(string(data), "/run/secrets/app_dsn", filepath.ToSlash(writeFile(t, testDSN+"\n")))
	const cacheURL = "redis://qf-redis-cache:6379/0"
	c, err := parse([]byte(s), env(map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL, "QF_REDIS_CACHE_URL": cacheURL}))
	if err != nil {
		t.Fatal(err)
	}
	if ec := c.EmbeddingCache(); ec == nil || *ec != (EmbeddingCache{cacheURL, 5 * time.Second}) {
		t.Fatalf("embedding cache = %+v", ec)
	}
	a, b := only(t, c.Model(Embeddings, "cache-emb")), only(t, c.Model(Embeddings, "cache-emb-b"))
	if a.Upstream.Name != "fake-a" || b.Upstream.Name != "fake-b" || a.UpstreamModel != "fake-embed" || b.UpstreamModel != "fake-embed" ||
		b.Upstream.URL(Embeddings) != "http://qf-fake-upstream-b:8081/v1/embeddings" {
		t.Fatalf("targets = %+v, %+v", a, b)
	}
}

// 用量事件的演示配置：打开用量事件（broker 是 qf-kafka）和嵌入缓存；四个别名分别得到结算、上游 401（取消）、
// 没有 usage（未知）和嵌入。
func TestUsageDemoConfig(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "config.usage.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := strings.ReplaceAll(string(data), "/run/secrets/app_dsn", filepath.ToSlash(writeFile(t, testDSN+"\n")))
	c, err := parse([]byte(s), env(map[string]string{"QF_FAKE_UPSTREAM_KEY": "k", "QF_REDIS_URL": testRedisURL,
		"QF_REDIS_CACHE_URL": "redis://qf-redis-cache:6379/0"}))
	if err != nil {
		t.Fatal(err)
	}
	if ue := c.UsageEvents(); ue == nil || !slices.Equal(ue.Brokers, []string{"qf-kafka:9092"}) || c.EmbeddingCache() == nil {
		t.Fatalf("usage events %+v, embedding cache %+v", ue, c.EmbeddingCache())
	}
	for alias, want := range map[string]string{"usage-chat": "fake-chat", "usage-rejected": "fake-401", "usage-unknown": "fake-no-usage"} {
		if got := only(t, c.Model(Chat, alias)).UpstreamModel; got != want {
			t.Errorf("%s -> %s, want %s", alias, got, want)
		}
	}
	if got := only(t, c.Model(Embeddings, "usage-emb")).UpstreamModel; got != "fake-embed" {
		t.Errorf("usage-emb -> %s", got)
	}
}
