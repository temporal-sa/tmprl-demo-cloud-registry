// A static proof executed only inside the no-network unit-test container.
package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"time"
)

func main() {
	if os.Geteuid() == 0 {
		panic("unit sandbox must run without root identity")
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[0] == "CapEff:" || fields[0] == "CapPrm:" || fields[0] == "CapBnd:") && strings.Trim(fields[1], "0") != "" {
			panic("unit sandbox retains Linux capabilities")
		}
	}
	interfaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, item := range interfaces {
		if item.Flags&net.FlagLoopback == 0 {
			addresses, _ := item.Addrs()
			// Loaded kernel tunnel modules can expose unconfigured links in
			// every namespace. A DOWN, addressless link cannot carry traffic;
			// non-root identity and the empty capability bounding set above
			// prevent tests from configuring it. Check function, not names.
			if item.Flags&net.FlagUp == 0 && len(addresses) == 0 {
				continue
			}
			panic(fmt.Sprintf("non-loopback interface in no-network sandbox: %s flags=%s addresses=%v", item.Name, item.Flags, addresses))
		}
	}
	for _, address := range []string{"192.0.2.1:443", "[2001:db8::1]:443", net.JoinHostPort(os.Getenv("OPERATOR_UNIT_HOST_GATEWAY"), "9")} {
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = connection.Close()
			panic("unexpected external/host reachability")
		}
		if !errors.Is(err, syscall.ENETUNREACH) && !errors.Is(err, syscall.EHOSTUNREACH) && !errors.Is(err, syscall.EAFNOSUPPORT) {
			panic(err)
		}
	}
	for _, path := range []string{"/var/run/docker.sock", "/root/.aws/credentials", "/root/.kube/config"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			panic("host credential/control file present")
		}
	}
	routes, err := os.ReadFile("/proc/net/route")
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(routes), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 1 && fields[1] == "00000000" {
			panic("default network route present")
		}
	}
	fmt.Println("Verified only loopback is active, kernel host/external denial, and absent host credential/socket files.")
}
