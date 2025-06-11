package prop

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

type BoolValue struct {
	propTrue float64
}

func Bool(propTrue float64) BoolValue {
	return BoolValue{propTrue: propTrue}
}

func (b BoolValue) Next() bool                 { return b.GetWithProp(rand.Float64()) }
func (b BoolValue) Rand(r *rand.Rand) bool     { return b.GetWithProp(r.Float64()) }
func (b BoolValue) GetWithProp(p float64) bool { return p < b.propTrue }

type UniformValue[T any] struct {
	slice []T
}

func Uniform[T any](slice []T) UniformValue[T] {
	return UniformValue[T]{slice: slice}
}

func (u UniformValue[T]) Next() T             { return u.slice[rand.IntN(len(u.slice))] }
func (u UniformValue[T]) Rand(r *rand.Rand) T { return u.slice[r.IntN(len(u.slice))] }
func (u UniformValue[T]) GetWithProp(p float64) T {
	idx := int(p * float64(len(u.slice)))
	if idx >= len(u.slice) {
		idx = len(u.slice) - 1
	}
	return u.slice[idx]
}

type WeigthedValue[T any] struct {
	total  float64
	weight []float64
	values []T
}

func WeightedOf[T any](values []T, weight func(int) float64) WeigthedValue[T] {
	w := make([]float64, len(values))
	total := 0.0
	for i := range values {
		w[i] = weight(i)
		total += w[i]
	}
	return WeigthedValue[T]{weight: w, values: values, total: total}
}

func (wv *WeigthedValue[T]) Add(value T, weight float64) {
	wv.values = append(wv.values, value)
	wv.weight = append(wv.weight, weight)
	wv.total += weight
}

func (wv *WeigthedValue[T]) Rand(r *rand.Rand) T { return wv.GetWithProp(r.Float64()) }
func (wv *WeigthedValue[T]) Next() T             { return wv.GetWithProp(rand.Float64()) }
func (wv *WeigthedValue[T]) GetWithProp(p float64) T {
	weight := p * wv.total
	for i, w := range wv.weight {
		weight -= w
		if weight < 0 {
			return wv.values[i]
		}
	}
	return wv.values[len(wv.values)-1]
}

type NormalValue struct {
	mean float64
	std  float64
	min  float64
	max  float64
}

func Normal(mean, std float64) NormalValue {
	return NormalValue{mean: mean, std: std, min: math.Inf(-1), max: math.Inf(1)}
}

func NormalRange(mean, std, min, max float64) NormalValue {
	return NormalValue{mean: mean, std: std, min: min, max: max}
}

func (n *NormalValue) Next() float64 {
	var u1 float64
	for {
		if u1 = rand.Float64(); u1 > 1e-7 {
			break
		}
	}
	return n.boxMuller(u1, rand.Float64())
}

func (n *NormalValue) Rand(r *rand.Rand) float64 {
	var u1 float64
	for {
		if u1 = r.Float64(); u1 > 1e-7 {
			break
		}
	}
	return n.boxMuller(u1, r.Float64())
}

func (n *NormalValue) boxMuller(u1, u2 float64) float64 {
	r := math.Sqrt(-2 * math.Log(u1))
	theta := 2 * math.Pi * u2
	return math.Max(math.Min(n.mean+n.std*r*math.Sin(theta), n.max), n.min)
}

type UniformJitterDurationValue struct {
	avg    time.Duration
	jitter time.Duration
}

func UniformJitterDuration(avg, jitter time.Duration) UniformJitterDurationValue {
	return UniformJitterDurationValue{avg: avg, jitter: jitter}
}

func (j UniformJitterDurationValue) Next() time.Duration { return j.GetWithProp(rand.Float64()) }
func (j UniformJitterDurationValue) Rand(r *rand.Rand) time.Duration {
	return j.GetWithProp(r.Float64())
}

func (j UniformJitterDurationValue) GetWithProp(p float64) time.Duration {
	v := j.avg + time.Duration(p*j.jitter.Seconds()*float64(time.Second))
	if v < 0 {
		return 0
	}
	return v
}

func (j UniformJitterDurationValue) Wait(ctx context.Context) error {
	timer := time.NewTimer(j.Next())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
