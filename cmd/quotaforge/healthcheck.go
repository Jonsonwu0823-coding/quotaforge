package main

import (
	"fmt"
	"net"
	"net/http"
	"time"
)

// healthcheck 请求本进程监听地址上的 /healthz，给容器的 HEALTHCHECK 用：运行镜像里没有 shell 和 curl。
func healthcheck(listenAddr string) error {
	addr, err := probeAddr(listenAddr)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("/healthz returned %d", resp.StatusCode)
	}
	return nil
}

// probeAddr 把监听地址换成能连的地址：监听在所有地址（0.0.0.0、::、空主机）时改连 127.0.0.1。
// 回环地址不会被 HTTP_PROXY 送去代理。
func probeAddr(listenAddr string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("listen address: %w", err)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), nil
}
