module github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice

go 1.25.8

toolchain go1.26.4

require (
	github.com/GoogleCloudPlatform/microservices-demo/protos v0.0.0
	github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go v0.0.0
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/nats-io/nats.go v1.52.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/sirupsen/logrus v1.9.4
	google.golang.org/protobuf v1.36.11
)

replace github.com/GoogleCloudPlatform/microservices-demo/protos => ../../protos

replace github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go => ../shared/stateless/go

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)
