//go:build bootstrap_integration

package projectbootstrap

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	project "go.temporal.io/cloud-sdk/api/project/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/encoding/protojson"
)

// faultPeer is an actual TLS HTTP/2 peer, not a fake gRPC invoker. It can reject
// HEADERS with REFUSED_STREAM (gRPC's transparent retry path) or discard an
// entire received request before replying (ambiguous server-side execution).
func faultPeer(t *testing.T, mode string) (string, *tls.Config, *atomic.Int32, *atomic.Bool) {
	t.Helper()
	certServer := httptest.NewTLSServer(nil)
	cert := certServer.TLS.Certificates[0]
	pool := x509.NewCertPool()
	pool.AddCert(certServer.Certificate())
	certServer.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("fault listener unavailable")
	}
	tl := tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}})
	var count atomic.Int32
	var authOK atomic.Bool
	var wg sync.WaitGroup
	var sockets sync.Map
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := tl.Accept()
			if err != nil {
				return
			}
			sockets.Store(conn, true)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				defer sockets.Delete(conn)
				_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
					return
				}
				f := http2.NewFramer(conn, conn)
				f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
				if f.WriteSettings() != nil {
					return
				}
				for {
					frame, err := f.ReadFrame()
					if err != nil {
						return
					}
					switch v := frame.(type) {
					case *http2.SettingsFrame:
						if !v.IsAck() {
							_ = f.WriteSettingsAck()
						}
					case *http2.MetaHeadersFrame:
						count.Add(1)
						for _, h := range v.Fields {
							if h.Name == "authorization" && h.Value == "Bearer "+syntheticToken {
								authOK.Store(true)
							}
						}
						if mode == "refused" {
							_ = f.WriteRSTStream(v.StreamID, http2.ErrCodeRefusedStream)
						}
					case *http2.DataFrame:
						if mode == "lost" && v.StreamEnded() {
							return
						}
						if mode == "status" && v.StreamEnded() {
							var header bytes.Buffer
							e := hpack.NewEncoder(&header)
							for _, h := range []hpack.HeaderField{{Name: ":status", Value: "200"}, {Name: "content-type", Value: "application/grpc"}, {Name: "grpc-status", Value: "14"}} {
								_ = e.WriteField(h)
							}
							_ = f.WriteHeaders(http2.HeadersFrameParam{StreamID: v.StreamID, BlockFragment: header.Bytes(), EndHeaders: true, EndStream: true})
						}
					case *http2.PingFrame:
						if !v.IsAck() {
							_ = f.WritePing(true, v.Data)
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = tl.Close()
		sockets.Range(func(k, _ any) bool { _ = k.(net.Conn).Close(); return true })
		wg.Wait()
	})
	return listener.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS12}, &count, &authOK
}

func TestOneAuthenticatedCreateOnRealHTTP2Faults(t *testing.T) {
	for _, mode := range []string{"refused", "lost", "status"} {
		for _, zeroBuffer := range []bool{false, true} {
			t.Run(mode+map[bool]string{true: "-zero-buffer", false: "-transparent-buffer"}[zeroBuffer], func(t *testing.T) {
				host, tc, count, authOK := faultPeer(t, mode)
				c, budget, err := sdkClient(newSession(syntheticToken, time.Now().Add(time.Hour)), host, tc, zeroBuffer)
				if err != nil {
					t.Fatal("test transport unavailable")
				}
				defer c.Close()
				budget.armed.Store(true)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_, err = c.CloudService().CreateProject(ctx, &cloudservice.CreateProjectRequest{Spec: &project.ProjectSpec{DisplayName: "offline"}, AsyncOperationId: "f9172794-74b1-5926-a5ab-527954981531"})
				if err == nil || count.Load() != 1 || !authOK.Load() || !budget.used.Load() {
					t.Fatal("expected one authenticated request and ambiguous failure")
				}
				if mode == "refused" && !zeroBuffer && budget.denied.Load() == 0 {
					t.Fatal("REFUSED_STREAM must exercise blocked transparent auth attempt")
				}
				// A second explicit call on the same bootstrap client is also denied.
				_, _ = c.CloudService().CreateProject(ctx, &cloudservice.CreateProjectRequest{})
				if count.Load() != 1 || budget.denied.Load() == 0 {
					t.Fatal("authenticated mutation attempt budget reused")
				}
			})
		}
	}
}

func TestNamespaceOneAuthenticatedCreateOnHTTP2Faults(t *testing.T) {
	binding := fakeParent(t)
	drafts, _, err := fixtureplan.NamespaceCreateReview(binding)
	if err != nil {
		t.Fatal("synthetic namespace intent invalid")
	}
	req := &cloudservice.CreateNamespaceRequest{}
	if protojson.Unmarshal(drafts[0].RequestJSON, req) != nil {
		t.Fatal("synthetic namespace request invalid")
	}
	for _, mode := range []string{"refused", "lost", "status"} {
		for _, zeroBuffer := range []bool{false, true} {
			t.Run(mode+map[bool]string{true: "-zero-buffer", false: "-transparent-buffer"}[zeroBuffer], func(t *testing.T) {
				host, tc, count, authOK := faultPeer(t, mode)
				c, budget, err := namespaceSDKClient(newSession(syntheticToken, time.Now().Add(time.Hour)), req, binding, host, tc, zeroBuffer)
				if err != nil {
					t.Fatal("synthetic namespace transport invalid")
				}
				defer c.Close()
				budget.armed.Store(true)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_, err = c.CloudService().CreateNamespace(ctx, req)
				if err == nil || count.Load() != 1 || !authOK.Load() || !budget.used.Load() {
					t.Fatal("namespace fault must retain exactly one authenticated request")
				}
				if mode == "refused" && !zeroBuffer && budget.denied.Load() == 0 {
					t.Fatal("namespace REFUSED_STREAM did not block transparent reauth")
				}
				_, _ = c.CloudService().CreateNamespace(ctx, req)
				if count.Load() != 1 || budget.denied.Load() == 0 {
					t.Fatal("namespace mutation authentication budget replayed")
				}
			})
		}
	}
}
