package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 监听在所有地址时要改连 127.0.0.1（Windows 上连 0.0.0.0 会失败）。这里只测换算，
// 不真的监听 0.0.0.0：那会在 Windows 上弹防火墙提示。
func TestProbeAddr(t *testing.T) {
	cases := map[string]string{
		"0.0.0.0:8080":   "127.0.0.1:8080",
		":8080":          "127.0.0.1:8080",
		"[::]:8080":      "127.0.0.1:8080",
		"127.0.0.1:9000": "127.0.0.1:9000",
		"10.1.2.3:8080":  "10.1.2.3:8080",
	}
	for in, want := range cases {
		if got, err := probeAddr(in); err != nil || got != want {
			t.Errorf("probeAddr(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := probeAddr("8080"); err == nil {
		t.Error("probeAddr(\"8080\") succeeded, want an error")
	}
}

func TestHealthcheck(t *testing.T) {
	ok := httptest.NewServer(newHandler(http.NotFoundHandler()))
	defer ok.Close()
	if err := healthcheck(ok.Listener.Addr().String()); err != nil {
		t.Fatalf("healthy server: %v", err)
	}

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()
	if err := healthcheck(failing.Listener.Addr().String()); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("500 from /healthz: err = %v", err)
	}

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := dead.Addr().String()
	dead.Close()
	if err := healthcheck(addr); err == nil {
		t.Fatal("nothing listening: healthcheck succeeded")
	}
}

// 容器的 HEALTHCHECK 只看子命令的退出码：健康 0，否则 1。
func TestHealthcheckCommand(t *testing.T) {
	ok := httptest.NewServer(newHandler(http.NotFoundHandler()))
	defer ok.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	dead.Close()

	cases := []struct {
		name, addr string
		want       int
	}{
		{"healthy", ok.Listener.Addr().String(), 0},
		{"healthz returns 500", failing.Listener.Addr().String(), 1},
		{"nothing listening", deadAddr, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("QUOTAFORGE_ADDR", tc.addr)
			if code := command("healthcheck"); code != tc.want {
				t.Fatalf("exit code = %d, want %d", code, tc.want)
			}
		})
	}
}
