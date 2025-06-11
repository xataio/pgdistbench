package client

import (
	"k8s.io/client-go/rest"

	"pgdistbench/pkg/client/systems"
)

type Client struct {
	restConfig *rest.Config
}

func New(restConfig *rest.Config) *Client {
	return &Client{
		restConfig: restConfig,
	}
}

func (b *Client) RestConfig() rest.Config {
	return *b.restConfig
}

func (b *Client) Close() {
}

func (b *Client) Systems() *systems.Systems {
	return systems.New(b.restConfig)
}

func (b *Client) Runners() *Runners {
	return &Runners{
		parent:     b,
		restConfig: b.restConfig,
	}
}

func (b *Client) BenchmarkExec() *Benchmarks {
	return &Benchmarks{
		parent:     b,
		restConfig: b.restConfig,
	}
}
