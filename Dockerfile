# 构建上下文是项目根目录。五个目标：quotaforge（生产，默认）、usage-consumer（用量事件的消费者）、qfctl（管理工具，迁移和发 Key）、
# fakeupstream（只用于测试）、qfbench（压测和齐发工具，只用于测试，见 compose.two.yaml）。
# 构建镜像的版本和 go.mod 的 toolchain 一致，按 digest 固定（多架构 index）。
# 依赖在 vendor/ 里，有 vendor/ 时 go build 默认 -mod=vendor，构建时不联网下载模块。
FROM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/quotaforge ./cmd/qfctl ./cmd/fakeupstream ./cmd/qfbench

FROM scratch AS fakeupstream
COPY --from=build /out/fakeupstream /fakeupstream
USER 65532:65532
ENTRYPOINT ["/fakeupstream"]
CMD ["-addr", "0.0.0.0:8081"]

# 压测工具：从 Compose 网络里面发请求，不经过宿主机的端口转发。Key 从 stdin 读，原始数据写 stdout。
FROM scratch AS qfbench
COPY --from=build /out/qfbench /qfbench
USER 65532:65532
ENTRYPOINT ["/qfbench"]

# 管理工具单独一个镜像，服务镜像里不带它。直连数据库，不需要 CA 证书。
FROM scratch AS qfctl
COPY --from=build /out/qfctl /qfctl
USER 65532:65532
ENTRYPOINT ["/qfctl"]
CMD ["migrate"]

# 用量事件的消费者单独一个目标：同一个程序，但它不开 API，镜像里不能带连 API 的健康检查。`up --wait` 对没有健康检查的
# 容器只等它在运行；在 Compose 里用 `healthcheck: disable: true` 去掉镜像的健康检查，容器配置里是 NONE，旧版 Compose 会报错。
FROM scratch AS usage-consumer
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/quotaforge /quotaforge
USER 65532:65532
ENTRYPOINT ["/quotaforge", "usage-consumer"]

# 放在最后：不带 --target 构建时得到的是生产镜像。
# scratch 里没有 shell 和 /etc/passwd：用数字 UID，健康检查用 exec 形式调用自己的子命令。
FROM scratch AS quotaforge
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/quotaforge /quotaforge
USER 65532:65532
# 容器里要监听所有地址，端口映射才连得上；健康检查按同一个变量去连 127.0.0.1。
ENV QUOTAFORGE_ADDR=0.0.0.0:8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --start-interval=1s --retries=3 \
  CMD ["/quotaforge", "healthcheck"]
ENTRYPOINT ["/quotaforge"]
