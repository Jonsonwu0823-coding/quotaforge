package telemetry

import (
	"database/sql"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// OverheadSLOSeconds 是延迟 SLO 的门槛：QF 自身耗时不超过它的请求算好。它必须正好是 qf_overhead_seconds 的一个桶边界，
// SLI 直接按这个桶算，不靠插值（deploy/prometheus/rules.yml 里写的是同一个数）。
const OverheadSLOSeconds = 0.25

// Metrics 是 QF 的全部指标，名字、标签和桶只在这里定义。标签的值只取固定集合或配置里的名字（接口、HTTP 状态、错误码、
// 上游名、结局），不用账户、Key、run、调用号，也不用客户端给的原始字符串。
// 用自己的注册表，不用全局默认的：库不会悄悄挂上别的指标。
type Metrics struct {
	reg *prometheus.Registry

	Requests         *prometheus.CounterVec   // endpoint, status, code
	RequestDuration  *prometheus.HistogramVec // endpoint
	Overhead         *prometheus.HistogramVec // endpoint
	UpstreamRequests *prometheus.CounterVec   // upstream, endpoint, result
	UpstreamDuration *prometheus.HistogramVec // upstream, endpoint
	Calls            *prometheus.CounterVec   // endpoint, outcome
	UsageTokens      *prometheus.CounterVec   // endpoint, upstream, type
	Replays          *prometheus.CounterVec   // endpoint
	RateLimited      *prometheus.CounterVec   // layer, metric
	DependencyErrors *prometheus.CounterVec   // dependency, operation
	Streams          *prometheus.CounterVec   // result, client
	StreamFirstEvent *prometheus.HistogramVec // endpoint

	CircuitState      *prometheus.GaugeVec   // upstream
	Failovers         *prometheus.CounterVec // endpoint, upstream
	CircuitRejections *prometheus.CounterVec // endpoint

	CacheLookups *prometheus.CounterVec // result
	CacheWrites  *prometheus.CounterVec // result
	CacheWaiting prometheus.Gauge

	Reclaimed             *prometheus.CounterVec // result
	Purged                prometheus.Counter
	BackgroundErrors      *prometheus.CounterVec // task
	BackgroundLastSuccess prometheus.Gauge
	BackgroundInterval    prometheus.Gauge
	UnknownCalls          prometheus.Gauge
	UnknownOldestAge      prometheus.Gauge

	OutboxUnsent    prometheus.Gauge
	OutboxOldestAge prometheus.Gauge
	EventsForwarded prometheus.Counter
	EventsConsumed  *prometheus.CounterVec // result
	EventDelay      prometheus.Observer

	TraceErrors prometheus.Counter
}

// 总耗时和上游耗时的桶：chat 的中位数几秒，上游超时默认 25 秒。
var slowBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 20, 25, 30}

// QF 自身耗时的桶：没有竞争时约 12 毫秒，OverheadSLOSeconds 是其中一个边界。
var overheadBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, OverheadSLOSeconds, .5, 1, 2.5}

// 用量事件延迟的桶：转发每秒一轮，消息队列或消费者停过的话可以是几分钟到几小时。
var eventBuckets = []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

// NewMetrics 建好全部指标。dbStats 是账本连接池的统计（ledger.Store.Stats），每次抓取时读。
func NewMetrics(dbStats func() sql.DBStats) *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}
	counter, histogram, gauge := m.counter, m.histogram, m.gauge
	m.Requests = counter("qf_requests_total", "Requests by endpoint, HTTP status written (0: the client was gone) and error code.",
		"endpoint", "status", "code")
	m.RequestDuration = histogram("qf_request_duration_seconds", "Total time of a request.", slowBuckets, "endpoint")
	m.Overhead = histogram("qf_overhead_seconds", "Time of a request spent in QF itself: the total minus the wait for the upstream.",
		overheadBuckets, "endpoint")
	m.UpstreamRequests = counter("qf_upstream_requests_total", "Upstream attempts by result: ok or the upstream error code; a call that moved to another upstream counts once per attempt.",
		"upstream", "endpoint", "result")
	m.UpstreamDuration = histogram("qf_upstream_duration_seconds", "Time from sending an upstream request to reading its whole body.",
		slowBuckets, "upstream", "endpoint")
	m.Calls = counter("qf_calls_total", "Calls in the ledger by the gateway's outcome: settled, cancelled, usage_unknown, or cached (an embedding cache hit, nothing reserved).",
		"endpoint", "outcome")
	m.UsageTokens = counter("qf_usage_tokens_total", "Tokens of settled calls; calls with unknown usage are counted in qf_calls_total, not here.",
		"endpoint", "upstream", "type")
	m.Replays = counter("qf_idempotent_replays_total", "Requests answered with the stored response of an earlier call.", "endpoint")
	m.RateLimited = counter("qf_rate_limited_total", "Requests denied by a rate limit bucket.", "layer", "metric")
	m.DependencyErrors = counter("qf_dependency_errors_total", "Failed ledger or rate limiter operations; requests fail with them. "+
		"Embedding cache errors are not counted here: the request goes on to the upstream.", "dependency", "operation")
	m.Streams = counter("qf_streams_total", "Streams the upstream started, by how they ended (ok or the upstream error code) "+
		"and the client (connected, disconnected, write_timeout).", "result", "client")
	m.StreamFirstEvent = histogram("qf_stream_first_event_seconds", "Time from the start of a streaming request to the first upstream event.",
		slowBuckets, "endpoint")

	m.CircuitState = m.gaugeVec("qf_upstream_circuit_state", "Circuit breaker state of an upstream in this instance: 0 closed, 1 half-open (a probe is in flight), 2 open.",
		"upstream")
	m.Failovers = counter("qf_upstream_failovers_total", "Calls moved to another target after the upstream could not be connected (the request was not sent to it).",
		"endpoint", "upstream")
	m.CircuitRejections = counter("qf_upstream_circuit_rejections_total", "Requests failed fast because the circuits of all their targets were open.",
		"endpoint")

	m.CacheLookups = counter("qf_embedding_cache_lookups_total", "Embedding requests by how the cache answered: hit, coalesced (shared the "+
		"result of an identical request in flight), miss, bypass (an Idempotency-Key, or a body that cannot be canonicalized) or error "+
		"(the cache failed; the request went on to the upstream).", "result")
	m.CacheWrites = counter("qf_embedding_cache_writes_total", "Settled embedding responses written to the cache: ok or error.", "result")
	m.CacheWaiting = gauge("qf_embedding_cache_waiting", "Embedding requests in this instance waiting for an identical request in flight.")

	m.Reclaimed = counter("qf_background_reclaimed_total", "Calls reclaimed by the background loop: released or marked_unknown.", "result")
	m.Purged = counter("qf_background_purged_total", "Expired idempotency records deleted by the background loop.").WithLabelValues()
	m.BackgroundErrors = counter("qf_background_errors_total", "Background tasks that failed: reclaim, purge, unknown_scan, "+
		"outbox_purge, usage_forward (QF), or usage_consume (the usage event consumer).", "task")
	m.BackgroundLastSuccess = gauge("qf_background_last_success_timestamp_seconds", "When the last background round without errors finished.")
	m.BackgroundInterval = gauge("qf_background_interval_seconds", "Interval between background rounds.")
	m.UnknownCalls = gauge("qf_unknown_usage_calls", "Calls with unknown usage that still hold their reservation.")
	m.UnknownOldestAge = gauge("qf_unknown_usage_oldest_age_seconds", "Age of the oldest call with unknown usage; 0 when there is none.")

	m.OutboxUnsent = gauge("qf_usage_outbox_unsent", "Usage events in the outbox not yet sent to the message queue (all instances share the outbox).")
	m.OutboxOldestAge = gauge("qf_usage_outbox_oldest_age_seconds", "Age of the oldest usage event not yet sent; 0 when there is none.")
	m.EventsForwarded = counter("qf_usage_events_forwarded_total", "Usage events sent to the message queue and marked sent by this instance.").WithLabelValues()
	m.EventsConsumed = counter("qf_usage_events_consumed_total", "Usage events taken from the message queue by the consumer: applied to the "+
		"daily totals, duplicate (the call was counted before), or malformed (skipped).", "result")
	m.EventDelay = histogram("qf_usage_event_delay_seconds", "Time from resolving a call to adding its event to the daily totals.",
		eventBuckets).WithLabelValues()

	m.TraceErrors = counter("qf_trace_export_errors_total", "Errors reported by the tracing SDK, mostly failed span exports.").WithLabelValues()

	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.registerDBStats(dbStats)
	return m
}

// counter、histogram、gaugeVec、gauge 建一个指标并注册。
func (m *Metrics) counter(name, help string, labels ...string) *prometheus.CounterVec {
	v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	m.reg.MustRegister(v)
	return v
}

func (m *Metrics) histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	m.reg.MustRegister(v)
	return v
}

func (m *Metrics) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	m.reg.MustRegister(v)
	return v
}

func (m *Metrics) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	m.reg.MustRegister(g)
	return g
}

// registerDBStats 导出账本连接池的统计。ledger.Store 不暴露 *sql.DB，所以不用 collectors.NewDBStatsCollector，
// 每个数一个函数，抓取时读 Stats()。
func (m *Metrics) registerDBStats(stats func() sql.DBStats) {
	gauge := func(name, help string, f func(sql.DBStats) float64) {
		m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return f(stats()) }))
	}
	counter := func(name, help string, f func(sql.DBStats) float64) {
		m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 { return f(stats()) }))
	}
	gauge("qf_db_open_connections", "Open ledger connections.", func(s sql.DBStats) float64 { return float64(s.OpenConnections) })
	gauge("qf_db_in_use_connections", "Ledger connections in use.", func(s sql.DBStats) float64 { return float64(s.InUse) })
	gauge("qf_db_idle_connections", "Idle ledger connections.", func(s sql.DBStats) float64 { return float64(s.Idle) })
	gauge("qf_db_max_open_connections", "Maximum open ledger connections.", func(s sql.DBStats) float64 { return float64(s.MaxOpenConnections) })
	counter("qf_db_wait_count_total", "Times a request waited for a ledger connection.", func(s sql.DBStats) float64 { return float64(s.WaitCount) })
	counter("qf_db_wait_duration_seconds_total", "Total time spent waiting for a ledger connection.",
		func(s sql.DBStats) float64 { return s.WaitDuration.Seconds() })
}

// Handler 是 /metrics：Prometheus 的文本格式。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
