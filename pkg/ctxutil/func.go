package ctxutil

import (
	"context"
)

type chContext struct {
	context.Context
	done chan struct{}
}

func WithFuncContext(parent context.Context, fn func()) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		go fn()
		return parent, func() {}
	}

	ctx, cancel := context.WithCancel(parent)
	chCtx := &chContext{
		Context: ctx,
		done:    make(chan struct{}),
	}

	go func() {
		defer close(chCtx.done)
		<-ctx.Done()
		fn()
	}()

	return chCtx, cancel

}

func (c *chContext) Done() <-chan struct{} {
	return c.done
}

func (c *chContext) Err() error {
	select {
	case <-c.done:
		return c.Context.Err()
	default:
		return nil
	}
}
