package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"go.temporal.io/api/proxy"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	// Register the gzip codec so the server can decompress incoming gzip
	// requests. The Temporal SDKs (e.g. Python >= 1.29) gzip-compress gRPC
	// requests by default; without this the server returns
	// "Decompressor is not installed for grpc-encoding \"gzip\"".
	_ "google.golang.org/grpc/encoding/gzip"
)

const (
	authorizationHeader = "authorization"
	namespaceHeader     = "temporal-namespace"
	defaultListen       = "127.0.0.1:7233"
)

type config struct {
	listenAddress     string
	temporalAddress   string
	temporalNamespace string
	temporalAPIKey    string
	namespaceAliases  map[string]struct{}
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	interceptor := cloudAuthInterceptor(
		cfg.temporalNamespace,
		cfg.temporalAPIKey,
		cfg.namespaceAliases,
	)
	upstream, err := grpc.NewClient(
		cfg.temporalAddress,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithUnaryInterceptor(interceptor),
	)
	if err != nil {
		log.Fatalf("failed to create Temporal Cloud client: %v", err)
	}
	defer upstream.Close()

	handler, err := proxy.NewWorkflowServiceProxyServer(proxy.WorkflowServiceProxyOptions{
		Client:                  workflowservice.NewWorkflowServiceClient(upstream),
		DisableHeaderForwarding: false,
	})
	if err != nil {
		log.Fatalf("failed to create Temporal proxy handler: %v", err)
	}

	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, handler)

	listener, err := net.Listen("tcp", cfg.listenAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cfg.listenAddress, err)
	}

	log.Printf("Temporal Cloud proxy listening on %s and forwarding to %s namespace %s", cfg.listenAddress, cfg.temporalAddress, cfg.temporalNamespace)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("proxy stopped: %v", err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddress:     envOrDefault("TEMPORAL_PROXY_LISTEN_ADDRESS", defaultListen),
		temporalAddress:   strings.TrimSpace(os.Getenv("TEMPORAL_ADDRESS")),
		temporalNamespace: strings.TrimSpace(os.Getenv("TEMPORAL_NAMESPACE")),
		temporalAPIKey:    strings.TrimSpace(os.Getenv("TEMPORAL_API_KEY")),
		namespaceAliases:  parseAliases(envOrDefault("TEMPORAL_PROXY_NAMESPACE_ALIASES", "default")),
	}

	var errs []error
	if cfg.temporalAddress == "" {
		errs = append(errs, errors.New("TEMPORAL_ADDRESS is required"))
	}
	if cfg.temporalNamespace == "" {
		errs = append(errs, errors.New("TEMPORAL_NAMESPACE is required"))
	}
	if cfg.temporalAPIKey == "" {
		errs = append(errs, errors.New("TEMPORAL_API_KEY is required"))
	}
	return cfg, errors.Join(errs...)
}

func envOrDefault(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func parseAliases(value string) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, item := range strings.Split(value, ",") {
		alias := strings.TrimSpace(item)
		if alias != "" {
			aliases[alias] = struct{}{}
		}
	}
	return aliases
}

func cloudAuthInterceptor(namespace string, apiKey string, aliases map[string]struct{}) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req any,
		reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if msg, ok := req.(proto.Message); ok {
			if err := rewriteNamespace(msg, namespace, aliases); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
		}

		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Delete(authorizationHeader)
		md.Delete(namespaceHeader)
		ctx = metadata.NewOutgoingContext(ctx, md)
		ctx = metadata.AppendToOutgoingContext(
			ctx,
			namespaceHeader,
			namespace,
			authorizationHeader,
			"Bearer "+apiKey,
		)

		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			log.Printf("Temporal Cloud request failed method=%s duration=%s error=%v", method, time.Since(start), err)
			return err
		}
		return nil
	}
}

func rewriteNamespace(msg proto.Message, target string, aliases map[string]struct{}) error {
	return rewriteNamespaceMessage(msg.ProtoReflect(), target, aliases, true)
}

// rewriteNamespaceMessage rewrites the `namespace` field on this message and any
// nested messages. Temporal requests carry a namespace not only at the top level
// but embedded in workflow command attributes — StartChildWorkflowExecution,
// SignalExternalWorkflowExecution, and RequestCancelExternalWorkflowExecution all
// have their own `namespace` field inside RespondWorkflowTaskCompletedRequest.
// The proxy aliases (e.g. "default") to the real Cloud namespace, so EVERY
// occurrence must be rewritten; otherwise Temporal Cloud rejects the whole
// RespondWorkflowTaskCompleted with "Namespace <alias> is not found" and the
// workflow task times out forever (e.g. a child-workflow-based agentic repair
// never starts).
//
// topLevel preserves the original behavior of defaulting an empty request
// namespace to the target. Nested empty namespaces are left untouched — an empty
// embedded namespace already means "the enclosing workflow's namespace".
func rewriteNamespaceMessage(
	m protoreflect.Message,
	target string,
	aliases map[string]struct{},
	topLevel bool,
) error {
	if field := m.Descriptor().Fields().ByName("namespace"); field != nil &&
		field.Kind() == protoreflect.StringKind && !field.IsList() && !field.IsMap() {
		current := m.Get(field).String()
		switch {
		case current == target:
			// Already the real namespace; nothing to do.
		case current == "":
			if topLevel {
				m.Set(field, protoreflect.ValueOfString(target))
			}
		default:
			if _, ok := aliases[current]; !ok {
				return fmt.Errorf("request namespace %q is not allowed by this proxy", current)
			}
			m.Set(field, protoreflect.ValueOfString(target))
		}
	}

	var walkErr error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Name() == "namespace" || fd.Kind() != protoreflect.MessageKind {
			return true
		}
		switch {
		case fd.IsList():
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				if walkErr = rewriteNamespaceMessage(list.Get(i).Message(), target, aliases, false); walkErr != nil {
					return false
				}
			}
		case fd.IsMap():
			if fd.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				if walkErr = rewriteNamespaceMessage(mv.Message(), target, aliases, false); walkErr != nil {
					return false
				}
				return true
			})
		default:
			walkErr = rewriteNamespaceMessage(v.Message(), target, aliases, false)
		}
		return walkErr == nil
	})
	return walkErr
}
