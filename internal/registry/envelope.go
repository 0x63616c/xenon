package registry

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/0x63616c/xenon/internal/identity"
)

// The payload is opaque canonical application bytes: registry never normalizes
// JSON or other application encodings. Callers must retain exactly these bytes
// on retry. An empty expected version denotes Create; otherwise Replace.
// Expected stores opaque version bytes as base64 in JSON, without UTF-8 coercion.
// The envelope's version is independent of legacy directory/topology formats.
type Envelope struct {
	Format     int                   `json:"format"`
	Key        Key                   `json:"key"`
	Expected   []byte                `json:"expected"`
	Transition identity.TransitionID `json:"transition"`
	Digest     string                `json:"digest"`
	Body       []byte                `json:"body"`
}

// ValidateKey rejects path aliases shared by S3 and filesystem adapters. An
// adapter must additionally enforce its namespace and contain filesystem paths.
func ValidateKey(key Key) error {
	s := string(key)
	if s == "" || len(s) > 1024 || !utf8.ValidString(s) || strings.Contains(s, "\\") {
		return &Invalid{key, "noncanonical key"}
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return &Invalid{key, "control character in key"}
		}
	}
	for _, part := range strings.Split(s, "/") {
		if part == "" || part == "." || part == ".." {
			return &Invalid{key, "noncanonical key segment"}
		}
	}
	return nil
}

func hashField(h hash.Hash, b []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(b)))
	h.Write(length[:])
	h.Write(b)
}
func digest(key Key, expected Version, body []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte("xenon.registry.write.v1\x00"))
	hashField(h, []byte(key))
	hashField(h, []byte(expected))
	hashField(h, body)
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}
func NewWrite(key Key, expected Version, transition identity.TransitionID, body []byte) (Write, error) {
	if len(body) == 0 {
		body = nil
	}
	w := Write{Transition: transition, Body: bytes.Clone(body), Digest: digest(key, expected, body)}
	if err := ValidateWrite(key, expected, w); err != nil {
		return Write{}, err
	}
	return w, nil
}

// ValidateCreate and ValidateReplace enforce the method-specific condition before
// any backend dispatch. ValidateWrite alone cannot know which method was called.
func ValidateCreate(key Key, w Write) error { return ValidateWrite(key, "", w) }
func ValidateReplace(key Key, expected Version, w Write) error {
	if expected == "" {
		return &Invalid{key, "Replace requires an observed version"}
	}
	return ValidateWrite(key, expected, w)
}

func ValidateWrite(key Key, expected Version, w Write) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	if err := w.Transition.Validate(); err != nil {
		return &Invalid{key, "invalid transition identity"}
	}
	if w.Digest != digest(key, expected, w.Body) {
		return &Invalid{key, "digest does not bind key, condition and body"}
	}
	return nil
}
func Encode(key Key, expected Version, w Write) ([]byte, error) {
	if err := ValidateWrite(key, expected, w); err != nil {
		return nil, err
	}
	body := w.Body
	if len(body) == 0 {
		body = nil
	}
	return json.Marshal(Envelope{1, key, []byte(expected), w.Transition, hex.EncodeToString(w.Digest[:]), body})
}

// Decode accepts only the exact canonical envelope emitted by Encode. This also
// rejects duplicate/unknown fields, alternate numbers, trailing values, and JSON
// coercions. Backend code must bound object reads before invoking Decode.
func Decode(key Key, r Record) (Envelope, error) {
	fail := func(err error) (Envelope, error) { return Envelope{}, &Corrupt{key, err} }
	if err := ValidateKey(key); err != nil {
		return Envelope{}, err
	}
	if r.Version == "" {
		return fail(errors.New("empty record version"))
	}
	var e Envelope
	if err := json.Unmarshal(r.Body, &e); err != nil {
		return fail(err)
	}
	if e.Format != 1 || e.Key != key {
		return fail(errors.New("wrong format or key"))
	}
	w, err := NewWrite(key, Version(e.Expected), e.Transition, e.Body)
	if err != nil {
		return fail(err)
	}
	canonical, err := Encode(key, Version(e.Expected), w)
	if err != nil {
		return fail(err)
	}
	if !bytes.Equal(canonical, r.Body) {
		return fail(errors.New("noncanonical envelope or digest mismatch"))
	}
	return e, nil
}

type Resolution uint8

const (
	Unresolved Resolution = iota
	Published
	RetrySameWrite
)

// Reconcile uses a fresh authoritative Read result after an ambiguous mutation.
// RetrySameWrite permits only the SAME identity/body/condition; it does not prove
// historical nonpublication or authorize reporting Conflict. A later transition
// cannot establish historical success/failure. Preserve UnknownOutcome across
// subsequent failed attempts; retain application receipts if history is needed.
func Reconcile(key Key, expected Version, w Write, observed Record, readErr error) (Resolution, error) {
	if err := ValidateWrite(key, expected, w); err != nil {
		return Unresolved, err
	}
	if readErr != nil {
		var missing *NotFound
		if errors.As(readErr, &missing) && missing.Key == key && expected == "" {
			return RetrySameWrite, nil
		}
		return Unresolved, &UnknownOutcome{key, w.Transition, readErr}
	}
	e, err := Decode(key, observed)
	if err != nil {
		return Unresolved, &UnknownOutcome{key, w.Transition, err}
	}
	if e.Transition == w.Transition {
		if e.Digest != hex.EncodeToString(w.Digest[:]) {
			return Unresolved, &Invalid{key, "transition reused with a different digest"}
		}
		return Published, nil
	}
	if expected != "" && observed.Version == expected {
		return RetrySameWrite, nil
	}
	return Unresolved, &UnknownOutcome{key, w.Transition, fmt.Errorf("observed a different transition")}
}
