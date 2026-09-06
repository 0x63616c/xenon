package ownership

import (
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"testing"
)

// A generated persistence service added on another branch must not silently
// disappear in managed mode. Integration must wire every service before passing.
func TestManagedServiceCoverage(t *testing.T) {
	manager := &Manager{}
	server, router := manager.Server()
	defer server.Stop()
	defer router.Close()
	registered := server.GetServiceInfo()
	count := 0
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if file.Package() != "xenon.v1" {
			return true
		}
		services := file.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			info, ok := registered[string(service.FullName())]
			if !ok {
				t.Errorf("managed mode omits %s", service.FullName())
				continue
			}
			for j := 0; j < service.Methods().Len(); j++ {
				method := service.Methods().Get(j)
				full := "/" + string(service.FullName()) + "/" + string(method.Name())
				if entry, ok := methods[full]; !ok || entry.execute == nil {
					t.Errorf("missing local dispatch %s", full)
				}
				response := reply(full)
				if response == nil || response.ProtoReflect().Descriptor() != method.Output() {
					t.Errorf("missing/wrong reply constructor for %s", full)
				}
				found := false
				for _, actual := range info.Methods {
					if actual.Name == string(method.Name()) {
						found = true
					}
				}
				if !found {
					t.Errorf("missing registration %s", full)
				}
				count++
			}
		}
		return true
	})
	if count == 0 {
		t.Fatal("no persistence services inspected")
	}
}
