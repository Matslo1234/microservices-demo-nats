module github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice

go 1.25.8

require (
	github.com/GoogleCloudPlatform/microservices-demo/hipstershop v0.0.0
	github.com/GoogleCloudPlatform/microservices-demo/protos v0.0.0
	github.com/nats-io/nats.go v1.52.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace github.com/GoogleCloudPlatform/microservices-demo/hipstershop => ../../protos/hipstershop

replace github.com/GoogleCloudPlatform/microservices-demo/protos => ../../protos
