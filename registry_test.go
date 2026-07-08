package prorate_test

import (
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.cadenya.com/prorate"
	testv1 "go.cadenya.com/prorate/internal/testdata/gen/test/v1"
)

// testFiles returns a protoregistry.Files containing the testdata file and
// its dependencies, exercising the explicit-descriptor path rather than
// GlobalFiles.
func testFiles(t *testing.T) *protoregistry.Files {
	t.Helper()
	fds := &descriptorpb.FileDescriptorSet{}
	seen := map[string]bool{}
	// Build the set from GlobalFiles by walking imports of the testdata file.
	var walk func(name string)
	walk = func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		fd, err := protoregistry.GlobalFiles.FindFileByPath(name)
		if err != nil {
			t.Fatalf("finding %s: %v", name, err)
		}
		for i := 0; i < fd.Imports().Len(); i++ {
			walk(fd.Imports().Get(i).Path())
		}
		fds.File = append(fds.File, protodesc.ToFileDescriptorProto(fd))
	}
	walk("test/v1/test.proto")
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("building files: %v", err)
	}
	return files
}

func wantPolicies() map[string]prorate.Policy {
	return map[string]prorate.Policy{
		"/test.v1.AnnotatedService/Intensive": {Tier: "intensive"},
		"/test.v1.AnnotatedService/Inherit":   {Tier: "standard"},
		"/test.v1.AnnotatedService/Health":    {Exempt: true},
		"/test.v1.AnnotatedService/Watch":     {Tier: "intensive"},
		"/test.v1.PlainService/Do":            {},
	}
}

func TestFromFilesPrecedence(t *testing.T) {
	reg, err := prorate.FromFiles(testFiles(t), []string{
		"test.v1.AnnotatedService",
		"test.v1.PlainService",
	})
	if err != nil {
		t.Fatal(err)
	}
	for method, want := range wantPolicies() {
		got, ok := reg.Policy(method)
		if !ok {
			t.Errorf("Policy(%q) missing", method)
			continue
		}
		if got != want {
			t.Errorf("Policy(%q) = %+v, want %+v", method, got, want)
		}
	}
	if reg.Len() != 5 {
		t.Errorf("Len() = %d, want 5", reg.Len())
	}
}

func TestFromFilesAllServicesWhenNamesNil(t *testing.T) {
	reg, err := prorate.FromFiles(testFiles(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	// nil names → every service in the set, including TypoService.
	if _, ok := reg.Policy("/test.v1.TypoService/Bad"); !ok {
		t.Error("TypoService not included with nil serviceNames")
	}
	if _, ok := reg.Policy("/test.v1.AnnotatedService/Intensive"); !ok {
		t.Error("AnnotatedService not included with nil serviceNames")
	}
}

func TestFromFilesUnknownService(t *testing.T) {
	_, err := prorate.FromFiles(testFiles(t), []string{"test.v1.NoSuchService"})
	if err == nil || !strings.Contains(err.Error(), "NoSuchService") {
		t.Fatalf("want descriptive error for unknown service, got %v", err)
	}
}

func TestFromFileDescriptorSet(t *testing.T) {
	fds := &descriptorpb.FileDescriptorSet{}
	testFiles(t).RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		fds.File = append(fds.File, protodesc.ToFileDescriptorProto(fd))
		return true
	})
	reg, err := prorate.FromFileDescriptorSet(fds, []string{"test.v1.AnnotatedService"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Policy("/test.v1.AnnotatedService/Intensive"); got.Tier != "intensive" {
		t.Errorf("Policy tier = %q, want intensive", got.Tier)
	}
}

func TestFromServer(t *testing.T) {
	srv := grpc.NewServer()
	testv1.RegisterAnnotatedServiceServer(srv, testv1.UnimplementedAnnotatedServiceServer{})
	testv1.RegisterPlainServiceServer(srv, testv1.UnimplementedPlainServiceServer{})
	reg, err := prorate.FromServer(srv)
	if err != nil {
		t.Fatal(err)
	}
	for method, want := range wantPolicies() {
		got, ok := reg.Policy(method)
		if !ok || got != want {
			t.Errorf("Policy(%q) = (%+v, %v), want (%+v, true)", method, got, ok, want)
		}
	}
	if _, ok := reg.Policy("/test.v1.TypoService/Bad"); ok {
		t.Error("TypoService present but was never registered on the server")
	}
}

func TestPoliciesIterator(t *testing.T) {
	reg, err := prorate.FromFiles(testFiles(t), []string{"test.v1.AnnotatedService"})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range reg.Policies() {
		count++
	}
	if count != 4 {
		t.Errorf("iterated %d policies, want 4", count)
	}
}
