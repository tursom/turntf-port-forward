module github.com/tursom/turntf-port-forward

go 1.26.1

require (
	github.com/tursom/turntf-go v0.0.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/coder/websocket v1.8.14 // indirect
	golang.org/x/crypto v0.43.0 // indirect
)

replace github.com/tursom/turntf-go => ../../sdk/turntf-go
