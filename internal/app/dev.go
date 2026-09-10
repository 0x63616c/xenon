package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// devContainer is an explicitly declared local fixture process. No host process
// identity is ever accepted as a cleanup handle.
type devContainer struct {
	Entrypoint    string   `json:"entrypoint,omitempty"`
	Config        *Config  `json:"config,omitempty"`
	Role          string   `json:"role"`
	Image         string   `json:"image"`
	Args          []string `json:"args"`
	Environment   []string `json:"environment,omitempty"`
	Ports         []string `json:"ports,omitempty"`
	NetworkHolder bool     `json:"network_holder,omitempty"`
	Objects       bool     `json:"objects,omitempty"`
}
type devPlan struct {
	Schema     int            `json:"schema"`
	Containers []devContainer `json:"containers"`
}
type devResource struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}
type devState struct {
	Schema         int           `json:"schema"`
	Run            string        `json:"run"`
	Hash           string        `json:"fixture_sha256"`
	Fixture        devPlan       `json:"fixture"`
	Resources      []devResource `json:"resources"`
	VolumeCreated  string        `json:"volume_created,omitempty"`
	Ephemeral      bool          `json:"ephemeral_authorized"`
	Initialized    bool          `json:"initialized,omitempty"`
	Initialization string        `json:"initialization,omitempty"`
}
type DevResult struct {
	Schema          int      `json:"schema"`
	Status          string   `json:"status"`
	Run             string   `json:"run,omitempty"`
	CleanupVerified bool     `json:"cleanup_verified"`
	Pending         []string `json:"pending,omitempty"`
}
type devDocker func(context.Context, ...string) ([]byte, error)

type devOutput struct{ bytes.Buffer }

func (b *devOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 16<<20 {
		return 0, errors.New("Docker output exceeds 16 MiB")
	}
	return b.Buffer.Write(p)
}

func dockerCommand(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	command.WaitDelay = 2 * time.Second
	out, diagnostics := devOutput{}, devOutput{}
	command.Stdout = &out
	command.Stderr = &diagnostics
	if len(args) > 0 && args[0] == "logs" {
		command.Stderr = &out
	}
	err := command.Run()
	if err != nil {
		return nil, fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(diagnostics.String()))
	}
	return out.Bytes(), nil
}

var devRole = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
var devImage = regexp.MustCompile(`^(sha256:[0-9a-f]{64}|[^\s]+@sha256:[0-9a-f]{64})$`)
var devID = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (f devPlan) validate() error {
	if f.Schema != 1 || len(f.Containers) < 1 || len(f.Containers) > 16 {
		return errors.New("fixture schema 1 and 1..16 declared containers required")
	}
	roles := map[string]bool{}
	for i, c := range f.Containers {
		if !devRole.MatchString(c.Role) || roles[c.Role] || !devImage.MatchString(c.Image) || len(c.Args) == 0 {
			return errors.New("unique valid roles, immutable images and explicit commands required")
		}
		roles[c.Role] = true
		if c.NetworkHolder != (i == 0) || c.Objects != (i == 0) {
			return errors.New("first container must exclusively own network namespace and object volume")
		}
		if i > 0 && len(c.Ports) > 0 {
			return errors.New("only network holder publishes ports")
		}
		for _, port := range c.Ports {
			if !regexp.MustCompile(`^127\.0\.0\.1:[0-9]{4,5}:[0-9]{1,5}$`).MatchString(port) {
				return errors.New("explicit loopback port mappings required")
			}
		}
		for _, e := range c.Environment {
			if !strings.Contains(e, "=") || strings.ContainsRune(e, 0) {
				return errors.New("invalid environment entry")
			}
		}
	}
	return nil
}
func readDevJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return errors.New("dev document exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing dev document")
	}
	return nil
}
func saveDevState(dir string, state devState) error {
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".state-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(append(raw, '\n'))
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(dir, "state.json")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func devLabels(state devState, role string) map[string]string {
	return map[string]string{"io.xenon.dev.run": state.Run, "io.xenon.dev.fixture": state.Hash, "io.xenon.dev.role": role}
}
func appendDevLabels(args []string, labels map[string]string) []string {
	for _, key := range []string{"io.xenon.dev.run", "io.xenon.dev.fixture", "io.xenon.dev.role"} {
		args = append(args, "--label", key+"="+labels[key])
	}
	return args
}
func devName(state devState, role string) string { return "xenon-dev-" + state.Run + "-" + role }
func fixtureHash(f devPlan) string {
	raw, _ := json.Marshal(f)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func runDevPlan(ctx context.Context, action string, fixture devPlan, dir string, ephemeral bool, docker devDocker, hooks devHooks) (result DevResult, err error) {
	result = DevResult{Schema: 1, Status: "invalid"}
	if ctx == nil || ctx.Err() != nil {
		return result, errors.New("active bounded context required")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("dev requires one absolute deadline")
	}
	if dir == "" || (action != "up" && action != "down") || ephemeral && action != "down" {
		return result, errors.New("explicit state, up/down and teardown-only ephemeral required")
	}
	if action == "up" {
		if err = fixture.validate(); err != nil {
			return result, err
		}
	}
	if action == "up" {
		if err = os.MkdirAll(dir, 0700); err != nil {
			return result, err
		}
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return result, errors.New("dev state is in use")
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	var state devState
	err = readDevJSON(filepath.Join(dir, "state.json"), &state)
	if errors.Is(err, os.ErrNotExist) && action == "up" {
		id := make([]byte, 16)
		if _, err = rand.Read(id); err != nil {
			return result, err
		}
		state = devState{Schema: 1, Run: hex.EncodeToString(id), Hash: fixtureHash(fixture), Fixture: fixture}
		err = saveDevState(dir, state)
	}
	if err != nil {
		return result, err
	}
	if state.Schema != 1 || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(state.Run) || state.Fixture.validate() != nil || state.Hash != fixtureHash(state.Fixture) {
		return result, errors.New("invalid recorded fixture identity")
	}
	if action == "up" && state.Hash != fixtureHash(fixture) {
		return result, errors.New("fixture differs from preserved state")
	}
	result.Run = state.Run
	result.Status = "failed"
	defer func() {
		if err != nil {
			result.Pending = nil
			for _, resource := range state.Resources {
				result.Pending = append(result.Pending, resource.ID)
			}
		}
	}()
	if ephemeral {
		state.Ephemeral = true
		if err = saveDevState(dir, state); err != nil {
			return result, err
		}
	}
	// Reconcile create-before-record crashes using exact random run identity. Every
	// selected object is then verified independently before adoption or deletion.
	raw, err := docker(ctx, "ps", "-aq", "--no-trunc", "--filter", "label=io.xenon.dev.run="+state.Run)
	if err != nil {
		return result, err
	}
	observed := map[string]string{}
	for _, id := range strings.Fields(string(raw)) {
		if !devID.MatchString(id) {
			return result, errors.New("invalid container identity")
		}
		raw, err = docker(ctx, "inspect", id)
		if err != nil {
			return result, err
		}
		var items []struct {
			ID     string `json:"Id"`
			Name   string
			Config struct {
				Image  string
				Labels map[string]string
			}
		}
		if json.Unmarshal(raw, &items) != nil || len(items) != 1 || items[0].ID != id {
			return result, errors.New("invalid container inspection")
		}
		item := items[0]
		role := item.Config.Labels["io.xenon.dev.role"]
		var spec *devContainer
		for i := range state.Fixture.Containers {
			if state.Fixture.Containers[i].Role == role {
				spec = &state.Fixture.Containers[i]
			}
		}
		if spec == nil || item.Name != "/"+devName(state, role) || item.Config.Image != spec.Image {
			return result, errors.New("container ownership conflict")
		}
		for key, value := range devLabels(state, role) {
			if item.Config.Labels[key] != value {
				return result, errors.New("container label conflict")
			}
		}
		if observed[role] != "" {
			return result, errors.New("duplicate role")
		}
		observed[role] = id
	}
	for _, r := range state.Resources {
		if id := observed[r.Role]; id != "" && id != r.ID {
			return result, errors.New("recorded container was replaced")
		}
	}
	state.Resources = nil
	for _, spec := range state.Fixture.Containers {
		if id := observed[spec.Role]; id != "" {
			state.Resources = append(state.Resources, devResource{id, spec.Role})
		}
	}
	if err = saveDevState(dir, state); err != nil {
		return result, err
	}
	if action == "down" {
		for i := len(state.Resources) - 1; i >= 0; i-- {
			r := state.Resources[i]
			result.Pending = append(result.Pending, r.ID)
			if _, err = docker(ctx, "stop", "--time", "10", r.ID); err != nil {
				return result, err
			}
			raw, err = docker(ctx, "logs", "--timestamps", r.ID)
			if err != nil {
				return result, err
			}
			log, openErr := os.OpenFile(filepath.Join(dir, r.Role+"-"+r.ID+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			if openErr != nil {
				return result, openErr
			}
			_, err = log.Write(raw)
			if err == nil {
				err = log.Sync()
			}
			err = errors.Join(err, log.Close())
			if err != nil {
				return result, err
			}
			if _, err = docker(ctx, "rm", r.ID); err != nil {
				return result, err
			}
			result.Pending = nil
			state.Resources = state.Resources[:i]
			if err = saveDevState(dir, state); err != nil {
				return result, err
			}
		}
		raw, err = docker(ctx, "ps", "-aq", "--no-trunc", "--filter", "label=io.xenon.dev.run="+state.Run)
		if err != nil {
			return result, err
		}
		if strings.TrimSpace(string(raw)) != "" {
			return result, errors.New("owned containers remain")
		}
		state.Resources = nil
		if state.Ephemeral {
			if err = devVolume(ctx, &state, docker, true); err != nil {
				return result, err
			}
		}
		if err = saveDevState(dir, state); err != nil {
			return result, err
		}
		result.Status = "stopped"
		result.CleanupVerified = true
		return result, nil
	}
	if state.Ephemeral {
		return result, errors.New("ephemeral fixture retired; use a new state directory")
	}
	if err = devVolume(ctx, &state, docker, false); err != nil {
		return result, err
	}
	if err = saveDevState(dir, state); err != nil {
		return result, err
	}
	holder := ""
	for _, spec := range fixture.Containers {
		var configPath string
		if spec.Config != nil {
			config := *spec.Config
			if state.Initialized {
				config.Bootstrap = false
				if config.ServiceStorage != nil {
					settings := config.ServiceStorage.Clone()
					settings.FreshNamespace = false
					config.ServiceStorage = &settings
				}
			}
			rawConfig, marshalErr := json.Marshal(config)
			if marshalErr != nil {
				return result, marshalErr
			}
			configPath, err = filepath.Abs(filepath.Join(dir, spec.Role+".json"))
			if err != nil {
				return result, err
			}
			if err = os.WriteFile(configPath, rawConfig, 0600); err != nil {
				return result, err
			}
			// Container runtime uses the image's unprivileged UID.
			if err = os.Chmod(configPath, 0644); err != nil {
				return result, err
			}
		}
		id := observed[spec.Role]
		if id == "" {
			args := appendDevLabels([]string{"create", "--name", devName(state, spec.Role), "--log-driver", "json-file", "--log-opt", "max-size=10m", "--log-opt", "max-file=1"}, devLabels(state, spec.Role))
			if spec.NetworkHolder {
				for _, port := range spec.Ports {
					args = append(args, "--publish", port)
				}
			} else {
				args = append(args, "--network", "container:"+holder)
			}
			if spec.Objects {
				args = append(args, "--mount", "type=volume,source="+devName(state, "objects")+",target=/data")
			}
			for _, e := range spec.Environment {
				args = append(args, "--env", e)
			}
			if spec.Config != nil {
				args = append(args, "--mount", "type=bind,source="+configPath+",target=/etc/xenon/dev.json,readonly")
			}
			if spec.Entrypoint != "" {
				args = append(args, "--entrypoint", spec.Entrypoint)
			}
			args = append(args, spec.Image)
			args = append(args, spec.Args...)
			raw, err = docker(ctx, args...)
			if err != nil {
				return result, err
			}
			id = strings.TrimSpace(string(raw))
			if !devID.MatchString(id) {
				return result, errors.New("invalid created container identity")
			}
			state.Resources = append(state.Resources, devResource{id, spec.Role})
			if err = saveDevState(dir, state); err != nil {
				return result, err
			}
		}
		if spec.NetworkHolder {
			holder = id
		}
		if _, err = docker(ctx, "start", id); err != nil {
			return result, err
		}
		if hooks.Started != nil {
			if err = hooks.Started(ctx, spec.Role, id, state); err != nil {
				return result, err
			}
		}
	}
	result.Status = "started"
	if hooks.Ready != nil {
		if err = hooks.Ready(ctx); err != nil {
			result.Status = "failed"
			return result, err
		}
		if hooks.Identity != nil {
			state.Initialization, err = hooks.Identity(ctx)
			if err != nil {
				return result, err
			}
		}
		state.Initialized = true
		if err = saveDevState(dir, state); err != nil {
			return result, err
		}
		result.Status = "ready"
	}
	return result, nil
}
func devVolume(ctx context.Context, state *devState, docker devDocker, remove bool) error {
	name := devName(*state, "objects")
	raw, err := docker(ctx, "volume", "ls", "-q", "--filter", "name=^"+name+"$")
	if err != nil {
		return err
	}
	exists := strings.TrimSpace(string(raw)) == name
	if !exists {
		if remove {
			state.VolumeCreated = "" // persisted ephemeral intent permits recovery after remove-before-save
			return nil
		}
		if state.VolumeCreated != "" {
			return errors.New("preserved volume missing; refusing empty replacement")
		}
		args := appendDevLabels([]string{"volume", "create"}, devLabels(*state, "objects"))
		if _, err = docker(ctx, append(args, name)...); err != nil {
			return err
		}
	}
	raw, err = docker(ctx, "volume", "inspect", name)
	if err != nil {
		return err
	}
	var items []struct {
		Name, CreatedAt string
		Labels          map[string]string
	}
	if json.Unmarshal(raw, &items) != nil || len(items) != 1 || items[0].Name != name || items[0].CreatedAt == "" {
		return errors.New("invalid volume inspection")
	}
	item := items[0]
	for key, value := range devLabels(*state, "objects") {
		if item.Labels[key] != value {
			return errors.New("volume ownership conflict")
		}
	}
	if state.VolumeCreated != "" && state.VolumeCreated != item.CreatedAt {
		return errors.New("preserved volume replaced")
	}
	state.VolumeCreated = item.CreatedAt
	if remove {
		if _, err = docker(ctx, "volume", "rm", name); err != nil {
			return err
		}
		state.VolumeCreated = ""
	}
	return nil
}

type devHooks struct {
	Started  func(context.Context, string, string, devState) error
	Identity func(context.Context) (string, error)
	Ready    func(context.Context) error
}
