package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
)

// Telemetry 是指标和追踪的设置。它们和监听地址（QUOTAFORGE_ADDR）一样是进程和部署层面的，来自环境变量；
// 同一份配置文件可以给两个实例用，各自的实例标识不同。
type Telemetry struct {
	MetricsAddr  string  // 指标的监听地址；空：不开
	OTLPEndpoint string  // OTLP/HTTP 的地址，span 发到它的 /v1/traces；空：不追踪
	SampleRatio  float64 // 没有上游的 traceparent 时的采样比例
	Instance     string  // span 的资源属性 service.instance.id
}

// LoadTelemetry 读 QUOTAFORGE_METRICS_ADDR、QUOTAFORGE_OTLP_ENDPOINT、QUOTAFORGE_TRACE_SAMPLE_RATIO、QUOTAFORGE_INSTANCE。
// 不合法时报错说明是哪个变量，不带它的值。比例和实例只对追踪有用：没设地址却设了它们，多半是漏了地址，也报错。
func LoadTelemetry(getenv func(string) string) (Telemetry, error) {
	t := Telemetry{MetricsAddr: getenv("QUOTAFORGE_METRICS_ADDR"), SampleRatio: 1, Instance: getenv("QUOTAFORGE_INSTANCE")}
	if t.MetricsAddr != "" {
		if _, _, err := net.SplitHostPort(t.MetricsAddr); err != nil {
			return t, errors.New("QUOTAFORGE_METRICS_ADDR must be host:port")
		}
	}
	ratio := getenv("QUOTAFORGE_TRACE_SAMPLE_RATIO")
	endpoint := getenv("QUOTAFORGE_OTLP_ENDPOINT")
	if endpoint == "" {
		if ratio != "" || t.Instance != "" {
			return t, errors.New("QUOTAFORGE_TRACE_SAMPLE_RATIO and QUOTAFORGE_INSTANCE need QUOTAFORGE_OTLP_ENDPOINT")
		}
		return t, nil
	}
	var err error
	if t.OTLPEndpoint, err = plainURL("QUOTAFORGE_OTLP_ENDPOINT", endpoint); err != nil {
		return t, err
	}
	if ratio != "" {
		t.SampleRatio, err = strconv.ParseFloat(ratio, 64)
		if err != nil || math.IsNaN(t.SampleRatio) || t.SampleRatio < 0 || t.SampleRatio > 1 {
			return t, errors.New("QUOTAFORGE_TRACE_SAMPLE_RATIO must be a number from 0 to 1")
		}
	}
	if t.Instance == "" {
		if t.Instance, err = os.Hostname(); err != nil {
			return t, fmt.Errorf("QUOTAFORGE_INSTANCE is not set and the host name is unavailable: %w", err)
		}
	}
	return t, nil
}
