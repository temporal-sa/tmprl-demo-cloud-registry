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
	reflected := msg.ProtoReflect()
	field := reflected.Descriptor().Fields().ByName("namespace")
	if field == nil || field.Kind() != protoreflect.StringKind {
		return nil
	}

	current := reflected.Get(field).String()
	if current == target {
		return nil
	}
	if current == "" {
		reflected.Set(field, protoreflect.ValueOfString(target))
		return nil
	}
	if _, ok := aliases[current]; ok {
		reflected.Set(field, protoreflect.ValueOfString(target))
		return nil
	}
	return fmt.Errorf("request namespace %q is not allowed by this proxy", current)
}
