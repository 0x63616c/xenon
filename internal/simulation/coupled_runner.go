package simulation

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"runtime"

	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
)

//go:embed coupled_runner.go
var coupledRunnerSource []byte

const CoupledKind = "coupled-production-steps-v1"

type coupledWorkload struct {
	Version            int               `json:"version"`
	ProductionRevision string            `json:"production_revision"`
	Toolchain          string            `json:"toolchain"`
	RequiredPartition  ids.PartitionID   `json:"required_partition"`
	RequiredOwner      ids.IncarnationID `json:"required_owner"`
}
type coupledTopology struct {
	Initial              cluster.Control `json:"initial"`
	ExpectedLayoutDigest [32]byte        `json:"expected_layout_digest"`
	Actors               []CoupledActor  `json:"actors"`
}

func strictJSON(raw []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing scenario bytes")
	}
	return nil
}

// ExpandCoupledScenario converts a code-authored scenario into the generic search envelope.
func ExpandCoupledScenario(c CoupledScenario) (Scenario, error) {
	workload, err := json.Marshal(coupledWorkload{c.Version, c.ProductionRevision, c.Toolchain, c.RequiredPartition, c.RequiredOwner})
	if err != nil {
		return Scenario{}, err
	}
	topology, err := json.Marshal(coupledTopology{c.Initial, c.ExpectedLayoutDigest, c.Actors})
	if err != nil {
		return Scenario{}, err
	}
	faults, err := json.Marshal(c.Steps)
	return Scenario{1, CoupledKind, workload, topology, faults}, err
}

// ExpandCoupled preserves every selected event, actor, tick and effect identity.
// It performs no generation or rescheduling. The source fixture's historical
// revision is retained separately from Runner's actual executing provenance.
func ExpandCoupled(raw []byte) (Scenario, error) {
	var c CoupledScenario
	if err := strictJSON(raw, &c); err != nil {
		return Scenario{}, err
	}
	return ExpandCoupledScenario(c)
}
func decodeCoupled(s Scenario) (CoupledScenario, error) {
	var w coupledWorkload
	var t coupledTopology
	var steps []CoupledInput
	if s.Version != 1 || s.Kind != CoupledKind {
		return CoupledScenario{}, errors.New("unsupported scenario kind")
	}
	if err := strictJSON(s.Workload, &w); err != nil {
		return CoupledScenario{}, err
	}
	if err := strictJSON(s.Topology, &t); err != nil {
		return CoupledScenario{}, err
	}
	if err := strictJSON(s.Faults, &steps); err != nil {
		return CoupledScenario{}, err
	}
	return CoupledScenario{w.Version, w.ProductionRevision, w.Toolchain, t.Initial, t.ExpectedLayoutDigest, t.Actors, w.RequiredPartition, w.RequiredOwner, steps}, nil
}

// CoupledDriver drives the real production Steps with modeled registry/native
// effects. RunCoupled already asserts the final healthy owner, progress and empty
// pending queues; Settle confirms that check completed. No external resources.
type CoupledDriver struct{ settled bool }

func (d *CoupledDriver) Validate(s Scenario, l WorkloadLimits) error {
	c, err := decodeCoupled(s)
	if err != nil {
		return err
	}
	if c.Version != 1 || len(c.Steps) == 0 || len(c.Steps) > 256 || len(c.Steps) > l.MaxOperations || len(c.Actors) != 5 || c.Toolchain != runtime.Version() {
		return errors.New("unsupported coupled schedule bounds or toolchain")
	}
	if err := validateCoupledFaults(c.Steps); err != nil {
		return err
	}
	for _, feature := range l.Features {
		if feature != CoupledKind {
			return errors.New("unsupported coupled feature")
		}
	}
	return nil
}
func (d *CoupledDriver) Run(ctx context.Context, s Scenario, emit func(json.RawMessage) error) error {
	d.settled = false
	c, err := decodeCoupled(s)
	if err != nil {
		return err
	}
	_, err = runCoupled(ctx, c, "", func(entry CoupledTrace) error {
		raw, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		return emit(raw)
	})
	if err == nil {
		d.settled = true
	}
	return err
}
func (d *CoupledDriver) Settle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !d.settled {
		return errors.New("coupled recovery was not verified")
	}
	return nil
}
func (d *CoupledDriver) Cleanup(context.Context) error { return nil }

// CoupledCorpus is a finite saved schedule corpus, not random workflow synthesis.
// Both RNG identities are recorded by Search but this generator consumes neither.
// EOF prevents a continuous search from pretending repeated fixed input is novel.
type CoupledCorpus struct {
	scenarios []Scenario
	info      GeneratorInfo
}

func NewCoupledCorpus(inputs ...[]byte) (*CoupledCorpus, error) {
	if len(inputs) == 0 {
		return nil, errors.New("empty corpus")
	}
	g := &CoupledCorpus{}
	for _, raw := range inputs {
		s, err := ExpandCoupled(raw)
		if err != nil {
			return nil, err
		}
		g.scenarios = append(g.scenarios, s)
	}
	g.info = GeneratorInfo{"coupled-corpus-v1", hash(coupledRunnerSource), []string{CoupledKind, "finite-corpus; workload/fault RNG unused"}}
	return g, nil
}
func (g *CoupledCorpus) Info() GeneratorInfo {
	out := g.info
	out.Capabilities = append([]string(nil), out.Capabilities...)
	return out
}
func (g *CoupledCorpus) Next(ctx context.Context, r GenerateRequest) (Scenario, error) {
	if err := ctx.Err(); err != nil {
		return Scenario{}, err
	}
	if r.Index >= uint64(len(g.scenarios)) {
		return Scenario{}, io.EOF
	}
	s := g.scenarios[r.Index]
	s.Workload = bytes.Clone(s.Workload)
	s.Topology = bytes.Clone(s.Topology)
	s.Faults = bytes.Clone(s.Faults)
	return s, nil
}
