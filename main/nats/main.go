package main

import (
	_ "embed"

	"github.com/mkawserm/abesh/cmd"

	_ "github.com/amjadjibon/nats/capability/nats"
)

//go:embed manifest.yaml
var manifestBytes []byte

func main() {
	cmd.ManifestBytes = manifestBytes
	cmd.Execute()
}
