package prorate

import (
	"fmt"
	"iter"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	proratev1 "go.cadenya.com/prorate/gen/prorate/v1"
)

// Policy is the rate limit policy resolved for a single RPC method from
// its proto annotations.
type Policy struct {
	// Tier is the named tier resolved from the method annotation or the
	// service default_tier. Empty means neither was set; the interceptor
	// applies its configured global default tier.
	Tier string
	// Exempt means the method is never rate limited.
	Exempt bool
}

// Registry maps full gRPC method names ("/pkg.Service/Method") to their
// resolved rate limit policies. Build one at startup with FromServer or
// FromFiles.
type Registry struct {
	policies map[string]Policy
}

// ServiceInfoProvider is the subset of *grpc.Server needed to enumerate
// registered services. It is identical to (and satisfied by anything
// satisfying) reflection.ServiceInfoProvider.
type ServiceInfoProvider interface {
	GetServiceInfo() map[string]grpc.ServiceInfo
}

// registryOptions collects RegistryOption state.
type registryOptions struct {
	resolver protodesc.Resolver
}

// RegistryOption configures FromServer. FromFiles and
// FromFileDescriptorSet take explicit descriptors and have no options.
type RegistryOption func(*registryOptions)

// WithResolver overrides the descriptor source used by FromServer.
// Defaults to protoregistry.GlobalFiles.
func WithResolver(r protodesc.Resolver) RegistryOption {
	return func(o *registryOptions) { o.resolver = r }
}

// FromServer builds a registry covering every service registered on srv
// (typically a *grpc.Server), resolving descriptors from
// protoregistry.GlobalFiles unless overridden with WithResolver. Register
// all services before calling this.
func FromServer(srv ServiceInfoProvider, opts ...RegistryOption) (*Registry, error) {
	o := registryOptions{resolver: protoregistry.GlobalFiles}
	for _, opt := range opts {
		opt(&o)
	}
	var names []string
	for name := range srv.GetServiceInfo() {
		names = append(names, name)
	}
	slices.Sort(names)
	return fromResolver(o.resolver, names)
}

// FromFiles builds a registry from explicit file descriptors. If
// serviceNames is nil, every service found in files is included;
// otherwise only the named services (fully qualified, e.g.
// "pkg.v1.MyService") are included and a missing name is an error.
//
// This is the entry point for offline tooling (e.g. docs generation from
// a compiled proto descriptor set) and for tests.
func FromFiles(files *protoregistry.Files, serviceNames []string) (*Registry, error) {
	if files == nil {
		return nil, fmt.Errorf("prorate: FromFiles requires a non-nil *protoregistry.Files")
	}
	if serviceNames == nil {
		files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
			svcs := fd.Services()
			for i := 0; i < svcs.Len(); i++ {
				serviceNames = append(serviceNames, string(svcs.Get(i).FullName()))
			}
			return true
		})
		slices.Sort(serviceNames)
	}
	return fromResolver(files, serviceNames)
}

// FromFileDescriptorSet builds a registry from a compiled
// descriptorpb.FileDescriptorSet (e.g. an Envoy proto_descriptor.pb).
// serviceNames semantics match FromFiles.
func FromFileDescriptorSet(fds *descriptorpb.FileDescriptorSet, serviceNames []string) (*Registry, error) {
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, fmt.Errorf("prorate: building files from descriptor set: %w", err)
	}
	return FromFiles(files, serviceNames)
}

// fromResolver resolves each named service against the resolver and
// extracts its policies.
func fromResolver(resolver protodesc.Resolver, serviceNames []string) (*Registry, error) {
	r := &Registry{policies: make(map[string]Policy)}
	for _, name := range serviceNames {
		desc, err := resolver.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			return nil, fmt.Errorf(
				"prorate: no descriptor found for registered service %q — "+
					"ensure its generated code is imported so the descriptor is registered, "+
					"or build the registry with FromFiles: %w", name, err)
		}
		sd, ok := desc.(protoreflect.ServiceDescriptor)
		if !ok {
			return nil, fmt.Errorf("prorate: descriptor for %q is a %T, not a service", name, desc)
		}
		r.addService(sd)
	}
	return r, nil
}

// addService resolves the service-level default and per-method policies.
func (r *Registry) addService(sd protoreflect.ServiceDescriptor) {
	var defaultTier string
	if sp, ok := proto.GetExtension(sd.Options(), proratev1.E_ServicePolicy).(*proratev1.ServicePolicy); ok && sp != nil {
		defaultTier = sp.GetDefaultTier()
	}
	methods := sd.Methods()
	for i := 0; i < methods.Len(); i++ {
		md := methods.Get(i)
		p := Policy{Tier: defaultTier}
		if mp, ok := proto.GetExtension(md.Options(), proratev1.E_MethodPolicy).(*proratev1.MethodPolicy); ok && mp != nil {
			if mp.GetExempt() {
				p = Policy{Exempt: true}
			} else if mp.GetTier() != "" {
				p.Tier = mp.GetTier()
			}
		}
		fullMethod := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
		r.policies[fullMethod] = p
	}
}

// Policy returns the resolved policy for a full method name of the form
// "/pkg.Service/Method".
func (r *Registry) Policy(fullMethod string) (Policy, bool) {
	p, ok := r.policies[fullMethod]
	return p, ok
}

// Policies iterates all registry entries in unspecified order — for docs
// and tooling.
func (r *Registry) Policies() iter.Seq2[string, Policy] {
	return func(yield func(string, Policy) bool) {
		for m, p := range r.policies {
			if !yield(m, p) {
				return
			}
		}
	}
}

// Len returns the number of methods in the registry.
func (r *Registry) Len() int {
	return len(r.policies)
}
