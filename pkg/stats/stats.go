package stats

import (
	"errors"
	"iter"
	"math"
	"slices"
	"sort"
)

type Histogram struct {
	Buckets []float64 `json:"buckets"`
	Counts  []int     `json:"counts"`
}

type DistMetrics struct {
	Sum       float64   `json:"sum"`
	Min       float64   `json:"min"`
	Max       float64   `json:"max"`
	Avg       float64   `json:"avg"`
	Median    float64   `json:"mean"`
	Stddev    float64   `json:"stddev"`
	Histogram Histogram `json:"histogram"`
}

func DistMetricStatsFrom[T any](it []T, fn func(T) float64) (stats DistMetrics) {
	values := make([]float64, len(it))
	for i := range it {
		values[i] = fn(it[i])
	}
	slices.Sort(values)

	if len(values) == 0 {
		return stats
	}

	var sum float64
	for _, v := range values {
		sum += v
	}

	min := values[0]
	max := values[len(values)-1]
	return DistMetrics{
		Sum:       sum,
		Min:       min,
		Max:       max,
		Avg:       SliceAverage(values),
		Median:    values[len(values)/2],
		Stddev:    SliceStddev(values),
		Histogram: SliceHistogram(values, min, max, 10),
	}
}

func identity[T any](v T) T {
	return v
}

func MinMaxOfFunc[T any](it iter.Seq[T], fn func(T) float64) (min float64, max float64) {
	count := 0
	max = math.Inf(-1)
	min = math.Inf(1)
	for item := range it {
		v := fn(item)
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	if count == 0 {
		panic(errors.New("no values to compute min/max"))
	}
	return min, max
}

func SliceAverage(values []float64) float64 {
	return SlicesSum(values) / float64(len(values))
}

func SliceAverageFunc[T any](items []T, fn func(T) float64) float64 {
	return SlicesSumOfFunc(items, fn) / float64(len(items))
}

func SlicesMedianOf[T any](summaries []T, selector func(T) float64) float64 {
	values := make([]float64, len(summaries))
	for i, summary := range summaries {
		values[i] = selector(summary)
	}
	sort.Float64s(values)
	mid := len(values) / 2
	if len(values)%2 == 0 {
		return (values[mid-1] + values[mid]) / 2
	}
	return values[mid]
}

func SliceStddev(items []float64) float64 {
	return SliceStddevFunc(items, identity)
}

func SliceStddevFunc[T any](items []T, fn func(T) float64) float64 {
	if len(items) <= 1 {
		return 0
	}

	avg := SliceAverageFunc(items, fn)
	sum := SlicesSumOfFunc(items, func(item T) float64 {
		v := fn(item) - avg
		return v * v
	})
	return math.Sqrt(sum / float64(len(items)-1))
}

func SliceHistogram(values []float64, min, max float64, n int) Histogram {
	return SliceHistogramFunc(min, max, n, values, identity)
}

func SliceHistogramFunc[T any](min, max float64, n int, items []T, fn func(T) float64) Histogram {
	return HistogramFunc(min, max, n, slices.Values(items), fn)
}

func HistogramFunc[T any](min, max float64, n int, items iter.Seq[T], fn func(T) float64) Histogram {
	if n <= 0 {
		n = 10
	}

	buckets := make([]float64, n+2)
	bucketWidth := (max - min) / float64(n)
	for i := range buckets {
		buckets[i] = min + float64(i)*bucketWidth + bucketWidth/2
	}

	counts := make([]int, n+2)
	for i := range items {
		v := fn(i)
		if v < min {
			buckets[0]++
		} else if v >= max {
			buckets[n-1]++
		} else {
			idx := int((v - min) / bucketWidth)
			if idx >= len(counts) {
				idx = len(counts) - 1
			}
			counts[idx]++
		}
	}
	return Histogram{Buckets: buckets, Counts: counts}
}

func SlicesSum(values []float64) float64 {
	return SumOfFunc(slices.Values(values), identity)
}

func SlicesSumOfFunc[T any](items []T, fn func(T) float64) float64 {
	return SumOfFunc(slices.Values(items), fn)
}

func SlicesGeometricMeanFunc[T any](items []T, fn func(T) float64) float64 {
	return GeometricMeanFunc(slices.Values(items), fn)
}

func GeometricMeanFunc[T any](in iter.Seq[T], fn func(T) float64) float64 {
	count := 0
	// Sum the logarithms of the extracted values
	sumLog := SumOfFunc(in, func(item T) float64 {
		count += 1

		value := fn(item)
		if value <= 0 {
			panic(errors.New("all extracted values must be positive to compute geometric mean"))
		}
		return math.Log(value)
	})

	if count == 0 {
		return 0
	}

	meanLog := sumLog / float64(count)
	return math.Exp(meanLog)
}

func SumOfFunc[T any](in iter.Seq[T], fn func(T) float64) float64 {
	sum := 0.0
	correction := 0.0 // Correction term for reducing floating-point errors

	for item := range in {
		y := fn(item) - correction
		t := sum + y
		correction = (t - sum) - y
		sum = t
	}

	return sum
}

func ExpBuckets(start float64, factor float64, max float64) []float64 {
	var buckets []float64
	current := start
	for current <= max {
		buckets = append(buckets, current)
		current *= factor
	}
	return buckets
}
