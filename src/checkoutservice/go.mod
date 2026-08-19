module github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice

go 1.25.8

toolchain go1.26.4

require (
	github.com/GoogleCloudPlatform/microservices-demo/protos v0.0.0
	github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go v0.0.0
	github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go v0.0.0
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/nats-io/nats.go v1.52.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/sirupsen/logrus v1.9.4
	google.golang.org/protobuf v1.36.11
)

replace github.com/GoogleCloudPlatform/microservices-demo/protos => ../../protos

replace github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go => ../shared/stateless/go

replace github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go => ../shared/telemetry/go

require (
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.81.1 // indirect
)
