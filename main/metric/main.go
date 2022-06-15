package main

import (
	_ "embed"

	_ "github.com/amjadjibon/encoding"
	"github.com/mkawserm/abesh/cmd"

	_ "github.com/amjadjibon/nats/capability/kv"
	_ "github.com/amjadjibon/nats/capability/metric"
	_ "github.com/amjadjibon/nats/capability/nats"
)

//go:embed manifest.yaml
var manifestBytes []byte

func main() {
	cmd.ManifestBytes = manifestBytes
	cmd.Execute()
}
