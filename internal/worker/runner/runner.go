package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
)

type Runner struct {
	Config worker.Config
	ch     chan any
	chRet  chan any
}

func New(cfg worker.Config) *Runner {
	w := &Runner{
		Config: cfg,
		ch:     make(chan any, 1),
		chRet:  make(chan any),
	}
	return w
}

func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup
	var activeTask func(context.Context) (any, error)
	var taskCh chan benchdriverapi.Result[any]
	var cancelTask context.CancelFunc
	var lastName benchdriverapi.TaskName
	var lastResult *benchdriverapi.Result[any]

	defer func() {
		if cancelTask != nil {
			cancelTask()
		}
		wg.Wait()
	}()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case result := <-taskCh:
			lastResult = &result
			cancelTask = nil
			activeTask = nil
			log.Printf("Task %q finished with %v", lastName, result)
			log.Print("Worker is now idle")

		case cmd := <-r.ch:
			switch cmd := cmd.(type) {
			case statusCommand:
				code := benchdriverapi.StatusIdle
				if activeTask != nil {
					code = benchdriverapi.StatusBusy
				}

				r.chRet <- benchdriverapi.WorkerStatus[benchdriverapi.Result[any]]{
					Code: code,
					Task: lastName,
					Last: lastResult,
				}
			case stopCommand:
				if cancelTask != nil {
					cancelTask()
					cancelTask = nil
				}
				r.chRet <- nil
			case healthCommand:
				if activeTask != nil {
					// Assume we are ok if we have an active task. Active tasks will report their own errors
					r.chRet <- healthResponse{StatusCode: benchdriverapi.StatusBusy}
				} else {
					status := benchdriverapi.StatusIdle
					err := func() error {
						db, err := worker.OpenDB(r.Config)
						if err != nil {
							return err
						}
						defer db.Close()

						return db.Ping()
					}()
					if err != nil {
						status = benchdriverapi.StatusDisconnected
					}
					r.chRet <- healthResponse{StatusCode: status, Error: err}
				}

			case worker.Task:
				if activeTask != nil {
					r.chRet <- benchdriverapi.ErrorBusy(errors.New("worker is busy"))
				}

				lastResult = nil
				lastName = cmd.Name
				activeTask = cmd.Task
				taskCh = make(chan benchdriverapi.Result[any])
				r.chRet <- nil

				var taskCtx context.Context
				taskCtx, cancelTask = context.WithCancel(ctx)

				log.Printf("Starting task %q\n", lastName)
				log.Println("Worker is now busy")

				wg.Add(1)
				go func() {
					defer wg.Done()

					v, err := func() (v any, err error) {
						defer recoverError(&err)
						return cmd.Task(taskCtx)
					}()
					if err != nil {
						fmt.Fprintln(os.Stderr, err)
					}

					taskCh <- benchdriverapi.Result[any]{
						Value: v,
						Error: err,
					}
				}()
			}
		}
	}
}

func (w *Runner) Healthcheck(ctx context.Context) (benchdriverapi.StatusCode, error) {
	select {
	case w.ch <- healthCommand{}:
		resp := castNotNil[healthResponse](<-w.chRet)
		return resp.StatusCode, resp.Error
	case <-ctx.Done():
		return benchdriverapi.StatusDisconnected, ctx.Err()
	}
}

func (w *Runner) Status(ctx context.Context) (status benchdriverapi.WorkerStatus[benchdriverapi.Result[any]]) {
	select {
	case w.ch <- statusCommand{}:
		return castNotNil[benchdriverapi.WorkerStatus[benchdriverapi.Result[any]]](<-w.chRet)
	case <-ctx.Done():
		return status
	}
}

func (w *Runner) CancelActive(ctx context.Context) error {
	select {
	case w.ch <- stopCommand{}:
		return castNotNil[error](<-w.chRet)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type BenchmarkWorker[Config any] struct {
	*Runner
	factory worker.TaskFactory[Config]
}

func NewBenchmarkWorker[Config any](r *Runner, f worker.TaskFactory[Config]) *BenchmarkWorker[Config] {
	return &BenchmarkWorker[Config]{Runner: r, factory: f}
}

func (w *BenchmarkWorker[Config]) Prepare(ctx context.Context, cfg Config) error {
	cmd, err := w.factory.Prepare(cfg)
	if err != nil {
		return err
	}
	return w.sendTask(ctx, cmd)
}

func (w *BenchmarkWorker[Config]) Cleanup(ctx context.Context) error {
	cmd, err := w.factory.Cleanup()
	if err != nil {
		return err
	}
	return w.sendTask(ctx, cmd)
}

func (w *BenchmarkWorker[Config]) Run(ctx context.Context, cfg Config) error {
	t, err := w.factory.Run(cfg)
	if err != nil {
		return err
	}
	return w.sendTask(ctx, t)
}

func (w *BenchmarkWorker[Config]) sendTask(ctx context.Context, cmd worker.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case w.ch <- cmd:
		return castNotNil[error](<-w.chRet)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type (
	stopCommand    struct{}
	statusCommand  struct{}
	healthCommand  struct{}
	healthResponse struct {
		StatusCode benchdriverapi.StatusCode
		Error      error
	}
)

func castNotNil[T any](v any) (zero T) {
	if v == nil {
		return zero
	}
	ret, ok := v.(T)
	if !ok {
		panic(fmt.Errorf("unexpected type %T, expected %T", v, zero))
	}
	return ret
}

func recoverError(err *error) {
	if r := recover(); r != nil {
		log.Printf("Runner: Recovered from panic: %v", r)
		debug.PrintStack()

		if *err == nil {
			if e := r.(error); e != nil {
				*err = e
			} else {
				*err = fmt.Errorf("%v", r)
			}
		}
	}
}
