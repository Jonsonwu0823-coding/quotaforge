// Package config 读取并校验 QuotaForge 的 JSON 配置。Load 返回的 Config 启动后只读，可以并发使用。
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// Purpose 是模型的用途，也就是它能用在哪个端点上。
type Purpose string

const (
	Chat       Purpose = "chat"
	Embeddings Purpose = "embeddings"
)

// LedgerTimeout 是每次账本操作（查 Key、预留、发送意图、收尾、回收的一个事务）的上限，等连接池也算在内。
const LedgerTimeout = 2 * time.Second

// RateLimitTimeout 是每次限流操作（准入、修正）的上限，等连接池也算在内。超了准入就是 503。
const RateLimitTimeout = 500 * time.Millisecond

// MaxRate 是限流限额（每分钟的请求数或 token 数）的上限。0 表示不限。
const MaxRate = 1_000_000_000_000

// ValidRate 报告 n 能否作为限流限额：0 到 MaxRate。配置文件和 qfctl 都按这一条校验。
func ValidRate(n int64) bool { return n >= 0 && n <= MaxRate }

const (
	defaultChatTimeout       = 25 * time.Second
	defaultEmbeddingsTimeout = 25 * time.Second
	// 回收上界的余量：CPU、GC 停顿、解析上游正文、写回响应。
	reclaimMargin = 5 * time.Second
	// 名字会写进账本的 VARCHAR(255) 列。
	maxNameLen = 255
)

type Upstream struct {
	Name   string
	APIKey string
	// RPM、TPM 是这个上游的限流限额（每分钟），保护的是 QF 用的那把上游 Key 的总量。0 表示不限。
	RPM, TPM int64
	urls     map[Purpose]string
}

// URL 是这个上游在某个用途下的完整地址，例如 …/v1/chat/completions。
func (u *Upstream) URL(p Purpose) string { return u.urls[p] }

type Model struct {
	Alias         string
	Purpose       Purpose
	Upstream      *Upstream
	UpstreamModel string
	// DefaultMaxTokens 是请求没写 max_tokens 时估算输出用的上限；只有 chat 模型有，embeddings 为 0。
	DefaultMaxTokens int64
}

type Config struct {
	timeouts map[Purpose]time.Duration
	models   map[modelKey]*Model
	dsn      string
	redisURL string
}

type modelKey struct {
	purpose Purpose
	alias   string
}

// Model 按端点用途和别名查模型，没有就返回 nil。
func (c *Config) Model(p Purpose, alias string) *Model { return c.models[modelKey{p, alias}] }

// DatabaseDSN 是账本数据库的连接串（go-sql-driver 的格式）。里面有密码，不能写进日志和报错。
func (c *Config) DatabaseDSN() string { return c.dsn }

// RedisURL 是限流用的 Redis 地址（redis://[:密码@]主机:端口/库号）。可能有密码，不能写进日志和报错。
func (c *Config) RedisURL() string { return c.redisURL }

// Timeout 是某个用途下调上游的超时。
func (c *Config) Timeout(p Purpose) time.Duration { return c.timeouts[p] }

// ReclaimAfter 是回收的上界：从预留（created_at）算起，超过它还在 RESERVED、IN_FLIGHT 的调用由回收处理。
// 一次活着的调用停在这两个状态的最长时间是：预留事务剩下的部分、发送意图、上游超时（从拨号管到读完正文）、收尾，
// 三次账本操作各最多 LedgerTimeout；再加余量。不另设配置项，由超时算出来。
func (c *Config) ReclaimAfter() time.Duration {
	return max(c.timeouts[Chat], c.timeouts[Embeddings]) + 3*LedgerTimeout + reclaimMargin
}

// ShutdownTimeout 是优雅退出的上限，和回收的上界相同：进行中的调用都能完整收尾。真被截断的调用留给回收。
// 部署里的 stop_grace_period 要比它长。
func (c *Config) ShutdownTimeout() time.Duration { return c.ReclaimAfter() }

type fileConfig struct {
	Timeouts struct {
		ChatMS       int64 `json:"chat_ms"`
		EmbeddingsMS int64 `json:"embeddings_ms"`
	} `json:"timeouts"`
	Upstreams []fileUpstream `json:"upstreams"`
	Models    []fileModel    `json:"models"`
	Database  struct {
		DSNEnv  string `json:"dsn_env"`
		DSNFile string `json:"dsn_file"`
	} `json:"database"`
	Redis struct {
		URLEnv  string `json:"url_env"`
		URLFile string `json:"url_file"`
	} `json:"redis"`
}

type fileUpstream struct {
	Name       string `json:"name"`
	BaseURL    string `json:"base_url"`
	BaseURLEnv string `json:"base_url_env"`
	APIKeyEnv  string `json:"api_key_env"`
	APIKeyFile string `json:"api_key_file"`
	RPM        int64  `json:"rpm"`
	TPM        int64  `json:"tpm"`
}

type fileModel struct {
	Alias            string  `json:"alias"`
	Purpose          Purpose `json:"purpose"`
	Upstream         string  `json:"upstream"`
	UpstreamModel    string  `json:"upstream_model"`
	DefaultMaxTokens int64   `json:"default_max_tokens"`
}

// Load 读取 path 处的配置。上游的 Key、数据库连接串、Redis 地址和用 base_url_env 指定的地址从 getenv 或文件取。
// 错误信息只说哪一项错了，不带 Key、连接串、地址或环境变量的值。
func Load(path string, getenv func(string) string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parse(data, getenv)
}

func parse(data []byte, getenv func(string) string) (*Config, error) {
	var f fileConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // 字段名拼错时报错，而不是悄悄用默认值
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if dec.More() {
		return nil, errors.New("parse config: trailing data after the JSON object")
	}

	c := &Config{}
	var err error
	if c.timeouts, err = timeouts(f.Timeouts.ChatMS, f.Timeouts.EmbeddingsMS); err != nil {
		return nil, err
	}
	upstreams, err := parseUpstreams(f.Upstreams, getenv)
	if err != nil {
		return nil, err
	}
	if c.models, err = parseModels(f.Models, upstreams); err != nil {
		return nil, err
	}
	if c.dsn, err = Secret("dsn_env", "dsn_file", f.Database.DSNEnv, f.Database.DSNFile, getenv); err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}
	if c.redisURL, err = Secret("url_env", "url_file", f.Redis.URLEnv, f.Redis.URLFile, getenv); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return c, nil
}

func parseUpstreams(list []fileUpstream, getenv func(string) string) (map[string]*Upstream, error) {
	if len(list) == 0 {
		return nil, errors.New("upstreams: at least one is required")
	}
	upstreams := map[string]*Upstream{}
	for i, u := range list {
		if u.Name == "" || len(u.Name) > maxNameLen {
			return nil, fmt.Errorf("upstreams[%d]: name is required, at most %d bytes", i, maxNameLen)
		}
		where := fmt.Sprintf("upstream %q", u.Name)
		if upstreams[u.Name] != nil {
			return nil, fmt.Errorf("%s: duplicate name", where)
		}
		base, err := baseURL(u.BaseURL, u.BaseURLEnv, getenv)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		key, err := Secret("api_key_env", "api_key_file", u.APIKeyEnv, u.APIKeyFile, getenv)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		if !ValidRate(u.RPM) || !ValidRate(u.TPM) {
			return nil, fmt.Errorf("%s: rpm and tpm must be integers from 0 (no limit) to %d", where, int64(MaxRate))
		}
		upstreams[u.Name] = &Upstream{Name: u.Name, APIKey: key, RPM: u.RPM, TPM: u.TPM, urls: map[Purpose]string{
			Chat:       base + "/chat/completions",
			Embeddings: base + "/embeddings",
		}}
	}
	return upstreams, nil
}

func parseModels(list []fileModel, upstreams map[string]*Upstream) (map[modelKey]*Model, error) {
	if len(list) == 0 {
		return nil, errors.New("models: at least one is required")
	}
	models := map[modelKey]*Model{}
	for i, m := range list {
		where := fmt.Sprintf("models[%d]", i)
		switch {
		case m.Alias == "" || len(m.Alias) > maxNameLen:
			return nil, fmt.Errorf("%s: alias is required, at most %d bytes", where, maxNameLen)
		case m.Purpose != Chat && m.Purpose != Embeddings:
			return nil, fmt.Errorf("%s: purpose must be %q or %q", where, Chat, Embeddings)
		case upstreams[m.Upstream] == nil:
			return nil, fmt.Errorf("%s: upstream %q is not defined", where, m.Upstream)
		case m.UpstreamModel == "" || len(m.UpstreamModel) > maxNameLen:
			return nil, fmt.Errorf("%s: upstream_model is required, at most %d bytes", where, maxNameLen)
		case m.Purpose == Chat && m.DefaultMaxTokens <= 0:
			return nil, fmt.Errorf("%s: default_max_tokens must be a positive integer for chat models", where)
		case m.Purpose == Embeddings && m.DefaultMaxTokens != 0:
			return nil, fmt.Errorf("%s: default_max_tokens is only for chat models", where)
		}
		k := modelKey{m.Purpose, m.Alias}
		if models[k] != nil {
			return nil, fmt.Errorf("%s: alias %q is already defined for %s", where, m.Alias, m.Purpose)
		}
		models[k] = &Model{Alias: m.Alias, Purpose: m.Purpose, Upstream: upstreams[m.Upstream],
			UpstreamModel: m.UpstreamModel, DefaultMaxTokens: m.DefaultMaxTokens}
	}
	return models, nil
}

func timeouts(chatMS, embeddingsMS int64) (map[Purpose]time.Duration, error) {
	t := map[Purpose]time.Duration{Chat: defaultChatTimeout, Embeddings: defaultEmbeddingsTimeout}
	for p, ms := range map[Purpose]int64{Chat: chatMS, Embeddings: embeddingsMS} {
		switch {
		case ms < 0:
			return nil, fmt.Errorf("timeouts.%s_ms must not be negative", p)
		case ms > 0:
			t[p] = time.Duration(ms) * time.Millisecond
		}
	}
	return t, nil
}

// baseURL 取上游的 base URL：base_url 和 base_url_env 正好写一个。返回值去掉了末尾的 /。
func baseURL(literal, envName string, getenv func(string) string) (string, error) {
	switch {
	case literal != "" && envName != "":
		return "", errors.New("set only one of base_url and base_url_env")
	case literal == "" && envName == "":
		return "", errors.New("one of base_url and base_url_env is required")
	case envName != "":
		literal = getenv(envName)
		if literal == "" {
			return "", fmt.Errorf("environment variable %s is not set", envName)
		}
	}
	u, err := url.Parse(literal)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		// 不回显地址：里面可能有凭据。
		return "", errors.New("base URL must be http(s)://host[:port][/path] without credentials, query or fragment")
	}
	return strings.TrimRight(literal, "/"), nil
}

// Secret 取一个机密值（上游 Key、数据库连接串、Redis 地址）：envName（环境变量名）和 file（文件路径）正好给一个。
// envField、fileField 是这两项在配置里的名字，只用于报错。文件用于容器的 secrets，
// 这样机密不会出现在 docker inspect 的环境变量里。错误信息可以带路径，不带值和文件内容。
func Secret(envField, fileField, envName, file string, getenv func(string) string) (string, error) {
	switch {
	case envName != "" && file != "":
		return "", fmt.Errorf("set only one of %s and %s", envField, fileField)
	case envName == "" && file == "":
		return "", fmt.Errorf("one of %s and %s is required", envField, fileField)
	case envName != "":
		key := getenv(envName)
		if key == "" {
			return "", fmt.Errorf("environment variable %s is not set", envName)
		}
		return key, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read %s %s: %w", fileField, file, err)
	}
	// 写文件时常带上末尾换行；Key 和连接串本身不含首尾空白。
	v := strings.TrimSpace(string(data))
	if v == "" {
		return "", fmt.Errorf("%s %s is empty", fileField, file)
	}
	return v, nil
}
