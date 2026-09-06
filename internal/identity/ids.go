// Package identity defines new Xenon-owned identities. It does not parse or
// rewrite legacy references or Temporal/user IDs; those retain their own codecs.
package identity

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
)

type ClusterID string
type NodeID string
type IncarnationID string
type PartitionID string
type OperationID string
type TransitionID string

// Source is independent of placement and fault-schedule randomness. Call once
// per logical identity and retain the result across retries.
type Source interface {
	NewID(prefix string) (string, error)
}

// Generator samples exactly 128 entropy bits. A nil Entropy uses crypto/rand.
// Injected readers must be synchronized by their owner if shared concurrently.
type Generator struct{ Entropy io.Reader }

var ErrInvalid = errors.New("invalid Xenon identity")

// Alphabet fixes the numeric order of the canonical base62 encoding.
const Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func validPrefix(prefix string) bool {
	switch prefix {
	case "clu", "nod", "inc", "prt", "op", "trn":
		return true
	}
	return false
}

func (g Generator) NewID(prefix string) (string, error) {
	if !validPrefix(prefix) {
		return "", fmt.Errorf("%w: prefix %q", ErrInvalid, prefix)
	}
	r := g.Entropy
	if r == nil {
		r = rand.Reader
	}
	var raw [16]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return "", fmt.Errorf("identity entropy: %w", err)
	}
	n := new(big.Int).SetBytes(raw[:])
	base := big.NewInt(62)
	rem := new(big.Int)
	var encoded [22]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		n.QuoRem(n, base, rem)
		encoded[i] = Alphabet[rem.Int64()]
	}
	return prefix + "_" + string(encoded[:]), nil
}

func validate(value, prefix string) error {
	if len(value) != len(prefix)+23 || !strings.HasPrefix(value, prefix+"_") {
		return ErrInvalid
	}
	n := new(big.Int)
	base := big.NewInt(62)
	for _, ch := range []byte(value[len(prefix)+1:]) {
		digit := strings.IndexByte(Alphabet, ch)
		if digit < 0 {
			return ErrInvalid
		}
		n.Mul(n, base)
		n.Add(n, big.NewInt(int64(digit)))
	}
	if n.BitLen() > 128 {
		return ErrInvalid
	}
	return nil
}

func (id ClusterID) Validate() error     { return validate(string(id), "clu") }
func (id NodeID) Validate() error        { return validate(string(id), "nod") }
func (id IncarnationID) Validate() error { return validate(string(id), "inc") }
func (id PartitionID) Validate() error   { return validate(string(id), "prt") }
func (id OperationID) Validate() error   { return validate(string(id), "op") }
func (id TransitionID) Validate() error  { return validate(string(id), "trn") }

// Validate even injected sources: an entropy fixture cannot bypass ingress rules.
func generate(s Source, prefix string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("%w: nil source", ErrInvalid)
	}
	value, err := s.NewID(prefix)
	if err != nil {
		return "", err
	}
	if err = validate(value, prefix); err != nil {
		return "", err
	}
	return value, nil
}
func NewClusterID(s Source) (ClusterID, error) { v, e := generate(s, "clu"); return ClusterID(v), e }
func NewNodeID(s Source) (NodeID, error)       { v, e := generate(s, "nod"); return NodeID(v), e }
func NewIncarnationID(s Source) (IncarnationID, error) {
	v, e := generate(s, "inc")
	return IncarnationID(v), e
}
func NewPartitionID(s Source) (PartitionID, error) {
	v, e := generate(s, "prt")
	return PartitionID(v), e
}
func NewOperationID(s Source) (OperationID, error) {
	v, e := generate(s, "op")
	return OperationID(v), e
}
func NewTransitionID(s Source) (TransitionID, error) {
	v, e := generate(s, "trn")
	return TransitionID(v), e
}
