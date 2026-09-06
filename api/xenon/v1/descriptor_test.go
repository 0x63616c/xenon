package xenonv1

import (
	"crypto/sha256"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"sort"
	"strings"
	"testing"
)

// Golden is the pre-migration descriptor set at 3a83534, sorted by file name,
// with only the generated-language GoPackage option removed. Field/service names,
// tags, types, defaults, dependencies and all remaining options stay protected.
func TestCanonicalLayoutPreservesWireDescriptors(t *testing.T) {
	set := &descriptorpb.FileDescriptorSet{}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if !strings.HasPrefix(file.Path(), "xenon/v1/") {
			return true
		}
		d := protodesc.ToFileDescriptorProto(file)
		if d.GetOptions().GetGoPackage() != "github.com/0x63616c/xenon/api/xenon/v1;xenonv1" {
			t.Errorf("noncanonical package %s", d.GetName())
		}
		d.Options.GoPackage = nil
		set.File = append(set.File, d)
		return true
	})
	if len(set.File) != 13 {
		t.Fatalf("descriptor count %d", len(set.File))
	}
	sort.Slice(set.File, func(i, j int) bool { return set.File[i].GetName() < set.File[j].GetName() })
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != "b25f1bb1ec15a3d522fbc12994e352ec5b13a05912ae48e2eb3f296cd37ba09a" {
		t.Fatalf("wire descriptor changed: %s", got)
	}
}
