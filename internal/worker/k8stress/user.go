package k8stress

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"time"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/prop"

	"github.com/rs/xid"
)

type stressUser struct {
	name                     string
	minPostgres, maxPostgres int
	opWait                   prop.UniformJitterDurationValue
	postgresConfigs          prop.WeigthedValue[benchdriverapi.K8StressPostgresConfig]
	tester                   *Tester

	rampup     int
	rampupWait prop.UniformJitterDurationValue

	registry *stressInstanceRegistry
}

const (
	LabelStressUser = "bench.maki.tech/stress-user"

	StressTestNamespace = "stress-test"
)

func newStressUser(tester *Tester, name string, cfg benchdriverapi.K8StressUser) *stressUser {
	rampupMax := benchdriverapi.GetOptValue(cfg.Rampup.MaxInstances, 1)
	rampupMin := benchdriverapi.GetOptValue(cfg.Rampup.MinInstances, rampupMax)
	if rampupMin > rampupMax {
		rampupMin = rampupMax
	}

	rampup := rampupMax
	if rampupMax > rampupMin {
		rampup = rand.IntN(rampupMax-rampupMin) + rampupMin
	}

	return &stressUser{
		tester:      tester,
		name:        name,
		minPostgres: benchdriverapi.GetOptValue(cfg.MinPostgres, 1),
		maxPostgres: benchdriverapi.GetOptValue(cfg.MaxPostgres, 1),
		opWait:      createTimer(cfg.UpdatesInterval, prop.UniformJitterDuration(5*time.Minute, 0)),
		postgresConfigs: prop.WeightedOf(cfg.PostgresConfigs, func(i int) float64 {
			return float64(benchdriverapi.GetOptValue(cfg.PostgresConfigs[i].Weight, 10))
		}),
		rampup:     rampup,
		rampupWait: createTimer(cfg.Rampup.CreateInterval, prop.UniformJitterDuration(1*time.Second, 0)),
		registry:   newStressInstanceRegistry(),
	}
}

func (u *stressUser) Run(ctx context.Context) error {
	log.Printf("Starting stress user: %s", u.name)
	defer log.Printf("Stopped stress user: %s", u.name)

	// background worker adding live stats from active runners
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	for ctx.Err() == nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					u.tester.metrics.Run.OpPanics.WithLabelValues(u.name, "run").Inc()
					log.Printf("ERROR running stress user: %s: %v", u.name, r)
					debug.PrintStack()
				}
			}()

			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer u.registry.reset()

			if err := u.run(runCtx); err != nil {
				log.Printf("ERROR running stress user: %s: %v", u.name, err)
			}
		}()
	}

	return nil
}

func (u *stressUser) run(ctx context.Context) (err error) {
	log.Printf("Starting stress user: %s", u.name)
	defer log.Printf("Stopped stress user: %s", u.name)

	defer func() {
		u.registry.CancelAll()
		log.Print("Waiting for instance workers to stop")
		u.registry.Wait()
		log.Print("Instance workers stopped")
	}()

	// ramp up
	for i := 0; i < u.rampup && ctx.Err() == nil; i++ {
		log.Printf("rampup operation: user %s: %d/%d", u.name, i, u.rampup)

		err := u.execOp(ctx, clusterOpCreate)
		if err != nil {
			log.Printf("ERROR rampup operation: user %s: %v", u.name, err)
		}
		if err := u.rampupWait.Wait(ctx); err != nil {
			return err
		}
	}

	for u.opWait.Wait(ctx) == nil {
		op := selectOp(len(u.registry.active), len(u.registry.instances), u.minPostgres, u.maxPostgres)
		if op == clusterOpNoop {
			continue
		}

		log.Printf("User: %s, cluster operation: %v", u.name, op)
		err := u.execOp(ctx, op)
		if err != nil {
			log.Printf("ERROR cluster operation: user %s: %v", u.name, err)
		}
	}
	return nil
}

func (u *stressUser) execOp(ctx context.Context, op clusterOp) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
			u.tester.metrics.Run.OpPanics.WithLabelValues(u.name, op.String()).Inc()

			debug.PrintStack()
		} else if err != nil {
			u.tester.metrics.Run.OpErrors.WithLabelValues(u.name, op.String()).Inc()
		}
	}()

	switch op {
	case clusterOpCreate:
		if err = u.createCluster(ctx); err != nil {
			err = fmt.Errorf("create cluster: %w", err)
		}
		u.tester.metrics.Run.CreateInstanceOps.WithLabelValues(u.name).Inc()
	case clusterOpBranch:
		log.Printf("branching cluster: not yet implemented")
	case clusterOpDelete:
		var ok bool
		ok, err = u.deleteCluster(ctx)
		if ok {
			u.tester.metrics.Run.DeleteInstanceOps.WithLabelValues(u.name).Inc()
		}
		if err != nil {
			err = fmt.Errorf("delete cluster: %w", err)
		}
	}

	return err
}

func (u *stressUser) createCluster(ctx context.Context) error {
	config := u.postgresConfigs.Next()

	instCtx, instCancel := context.WithCancel(ctx)
	instanceName := u.name + "-" + xid.New().String()
	instanceName = strings.ReplaceAll(instanceName, " ", "-")
	si := &stressInstance{
		name:       instanceName,
		cancel:     instCancel,
		owner:      u,
		config:     &config,
		registry:   u.registry,
		createTime: time.Now(),
	}

	u.registry.mu.Lock()
	defer u.registry.mu.Unlock()
	u.registry.instances[instanceName] = si
	u.registry.starting[instanceName] = struct{}{}
	si.Start(instCtx)
	return nil
}

func (u *stressUser) deleteCluster(ctx context.Context) (bool, error) {
	u.registry.mu.Lock()
	defer u.registry.mu.Unlock()

	if len(u.registry.active) == 0 {
		return false, nil
	}

	dropCandidates := make([]*stressInstance, 0, len(u.registry.active))
	for name := range u.registry.active {
		inst := u.registry.instances[name]
		if inst.mustDelete() {
			dropCandidates = []*stressInstance{u.registry.instances[name]}
			break
		}
		if inst.canDelete() {
			dropCandidates = append(dropCandidates, inst)
		}
	}

	if len(dropCandidates) == 0 {
		return false, nil
	}

	var inst *stressInstance
	if len(dropCandidates) == 1 {
		inst = dropCandidates[0]
	} else {
		inst = prop.Uniform(dropCandidates).Next()
	}

	inst.cancel()
	delete(u.registry.active, inst.name)
	delete(u.registry.instances, inst.name)

	return true, nil
}

func createTimer(dist *benchdriverapi.JitterDuration, def prop.UniformJitterDurationValue) prop.UniformJitterDurationValue {
	if dist == nil {
		return def
	}
	var jitter time.Duration
	if j := dist.Jitter; j != nil {
		jitter = j.Duration
	}
	return prop.UniformJitterDuration(dist.Duration.Duration, jitter)
}

func selectOp(active, total, min, max int) clusterOp {
	log.Println("selectOp:", active, total, min, max)

	switch active {
	case 0:
		return clusterOpCreate
	case max:
		return clusterOpDelete
	default:
		// The deletion probability is higher the more instances we have in the cluster
		if prop.Bool(float64(active-min) / float64(max-min)).Next() {
			return clusterOpDelete
		}

		// TODO: branching
		return clusterOpCreate
	}
}
