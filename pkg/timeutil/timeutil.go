package timeutil

import (
	"context"
	"iter"
	"time"
)

func Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func IterTick(ctx context.Context, period time.Duration) iter.Seq[time.Time] {
	return func(yield func(time.Time) bool) {
		now := time.Now()
		next := now.Add(period)

		ticker := time.NewTicker(period)
		defer ticker.Stop()

		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				if t.After(next) {
					next = t.Add(period)
					if !yield(t) {
						return
					}
				}
			}
		}
	}
}
