package benchdriverapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type WorkerStatus[T any] struct {
	Code StatusCode `json:"code"`
	Task TaskName   `json:"task,omitempty"`
	Last *T         `json:"last,omitempty"`
}

func CollectValues[T any](status []WorkerStatus[Result[T]]) []T {
	results := make([]T, 0, len(status))
	for _, s := range status {
		if s.Last != nil && s.Last.Error == nil {
			results = append(results, s.Last.Value)
		}
	}
	return results
}

type APIWorkerStatus = WorkerStatus[Result[any]]

type StatusCode string

const (
	StatusIdle         StatusCode = "Idle"
	StatusBusy         StatusCode = "Busy"
	StatusDisconnected StatusCode = "Disconnected"
)

type TaskName string

type Result[T any] struct {
	Value T     `json:"value,omitempty"`
	Error error `json:"error,omitempty"`
}

func (r *Result[T]) MarshalJSON() ([]byte, error) {
	if r.Error != nil {
		return json.Marshal(map[string]string{
			"error": r.Error.Error(),
		})
	}
	var zero T
	if reflect.DeepEqual(r.Value, zero) {
		return []byte("{}"), nil
	}

	return json.Marshal(map[string]any{
		"value": r.Value,
	})
}

func (r *Result[T]) UnmarshalJSON(b []byte) error {
	*r = Result[T]{}

	tmp := map[string]json.RawMessage{}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	if v, ok := tmp["error"]; ok {
		var errStr string
		if err := json.Unmarshal(v, &errStr); err != nil {
			return err
		}
		r.Error = errors.New(errStr)
	}

	if v, ok := tmp["value"]; ok {
		if err := json.Unmarshal(v, &r.Value); err != nil {
			return err
		}
	}

	return nil
}

type OpStats struct {
	Prefix    string  `json:"prefix"`
	Operation string  `json:"operation"`
	Count     int64   `json:"count"`
	Sum       float64 `json:"sum_ms"`
	Avg       float64 `json:"avg_ms"`
	Median    float64 `json:"median_ms"`
	Max       float64 `json:"max_ms"`
}

type Sample float64

func (s Sample) MarshalJSON() ([]byte, error) {
	return json.Marshal(float64(s))
}

func (s *Sample) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*s = Sample(value)
		return nil
	case string:
		tmp, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		*s = Sample(tmp)
		return nil
	default:
		return errors.New("invalid sample")
	}
}

type Percentage float64

func (p Percentage) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%.2f%%", p))
}

func (p *Percentage) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*p = Percentage(value)
		return nil
	case string:
		if end := len(value) - 1; value[end] == '%' {
			value = value[:end]
		}

		tmp, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		*p = Percentage(tmp)
		return nil
	default:
		return errors.New("invalid percentage")
	}
}

type JitterDuration struct {
	Duration *Duration `json:"duration"`
	Jitter   *Duration `json:"jitter"`
}

type NormDistValue struct {
	Mean   float64  `json:"mean"`
	StdDev *float64 `json:"stddev"`
}

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

type PartitionType string

const (
	PartitionHash     PartitionType = "hash"
	PatitionRange     PartitionType = "range"
	PatitionListHash  PartitionType = "list_hash"
	PatitionListRange PartitionType = "list_range"
)

type IsolationLevel string

const (
	IsolationLevelDefault        IsolationLevel = "default"
	IsoaltionLevelReadCommitted  IsolationLevel = "read_committed"
	IsoaltionLevelReadUncommited IsolationLevel = "read_uncommitted"
	IsoaltionLevelRepeatableRead IsolationLevel = "repeatable_read"
	IsoaltionLevelSnapshot       IsolationLevel = "snapshot"
	IsolationLevelSerializable   IsolationLevel = "serializable"
	IsolationLevelLinearizable   IsolationLevel = "linearizable"
)

func GetOptValue[T any](v *T, def T) T {
	if v == nil {
		return def
	}
	return *v
}
