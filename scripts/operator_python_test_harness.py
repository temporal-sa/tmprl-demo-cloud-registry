"""Trusted in-container proof and stdlib-only helper test selection.

Not a host entry point. The runner stages exactly this file plus the reviewed
build helper and tests in an immutable image. Linux amd64/arm64 only. Native
process execution/creation is denied by a filter installed before test imports;
network isolation is Docker network=none, independently probed before imports.
"""
import collections
import ctypes
import errno
import fcntl
import os
import pathlib
import platform
import socket
import struct
import sys
import unittest


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def prove_container_boundary():
    require(sys.platform == "linux" and os.geteuid() != 0, "nonroot Linux container required")
    status = dict(line.split(":", 1) for line in pathlib.Path("/proc/self/status").read_text().splitlines() if ":" in line)
    for key in ("CapEff", "CapPrm", "CapBnd"):
        require(int(status[key].strip(), 16) == 0, "Linux capability boundary missing")
    require(status["NoNewPrivs"].strip() == "1", "no-new-privileges boundary missing")
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        for _, name in socket.if_nameindex():
            flags = int(pathlib.Path("/sys/class/net", name, "flags").read_text(), 16)
            if flags & 8:  # IFF_LOOPBACK
                continue
            require(not flags & 1, "active non-loopback interface present")
            try:
                fcntl.ioctl(probe, 0x8915, struct.pack("256s", name.encode()))  # SIOCGIFADDR
            except OSError as error:
                require(error.errno in (errno.EADDRNOTAVAIL, errno.ENODEV), "interface address probe failed")
            else:
                raise RuntimeError("addressed non-loopback interface present")
    for line in pathlib.Path("/proc/net/if_inet6").read_text().splitlines():
        require(line.split()[-1] == "lo", "non-loopback IPv6 address present")
    for line in pathlib.Path("/proc/net/route").read_text().splitlines()[1:]:
        require(line.split()[1] != "00000000", "default route present")
    gateway = os.environ["OPERATOR_PYTHON_HOST_GATEWAY"]
    for family, address in ((socket.AF_INET, (gateway, 9)), (socket.AF_INET, ("192.0.2.1", 443)), (socket.AF_INET6, ("2001:db8::1", 443, 0, 0))):
        try:
            with socket.socket(family) as connection:
                connection.settimeout(1)
                connection.connect(address)
        except OSError as error:
            require(error.errno in (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EAFNOSUPPORT), "host/external denial was not a kernel routing denial")
        else:
            raise RuntimeError("host/external network reachable")
    pathlib.Path("/tmp/home").mkdir(mode=0o700)
    for prefix in ("AWS_", "KUBE", "TEMPORAL_", "GOOGLE_", "AZURE_", "DOCKER_"):
        require(not any(key.startswith(prefix) for key in os.environ), "ambient provider environment present")
    for filename in ("/var/run/docker.sock", "/run/containerd/containerd.sock", "/connection.json", "/tmp/home/.aws/credentials", "/tmp/home/.kube/config", "/root/.aws/credentials", "/root/.docker/config.json", "/root/.kube/config", "/root/.config/temporalio/temporal.yaml"):
        try:
            descriptor = os.open(filename, os.O_RDONLY)
        except (FileNotFoundError, PermissionError):
            pass
        else:
            os.close(descriptor)
            raise RuntimeError("unexpected readable credential/control fixture")
    try:
        descriptor = os.open("/operator-tests/immutable.txt", os.O_WRONLY | os.O_TRUNC)
    except OSError as error:
        require(error.errno in (errno.EROFS, errno.EACCES), "immutable fixture write denied unexpectedly")
    else:
        os.close(descriptor)
        raise RuntimeError("immutable source fixture was writable")
    with open("/tmp/write-proof", "xb") as target:
        target.write(b"owned temporary writes work")
    pathlib.Path("/tmp/write-proof").unlink()


def install_process_boundary():
    # Native syscall ABI tables: Linux arch/x86/entry/syscalls/syscall_64.tbl
    # and include/uapi/asm-generic/unistd.h (aarch64). Unknown ABIs fail closed.
    architectures = {
        "x86_64": (0xC000003E, (56, 57, 58, 59, 322, 435), 59, 435),
        "aarch64": (0xC00000B7, (220, 221, 281, 435), 221, 435),
    }
    require(platform.machine() in architectures, "unsupported native seccomp ABI")
    arch, denied, execve, clone3 = architectures[platform.machine()]

    class Filter(ctypes.Structure):
        _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte), ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint32)]

    class Program(ctypes.Structure):
        _fields_ = [("length", ctypes.c_ushort), ("filters", ctypes.POINTER(Filter))]

    # Check audit architecture, reject x32/alternate syscall-number space, then
    # return EPERM for fork/clone/exec APIs. All other calls retain Docker's
    # default filter and the separate network/filesystem/container boundaries.
    instructions = [(0x20, 0, 0, 4), (0x15, 1, 0, arch), (0x06, 0, 0, 0x80000000),
                    (0x20, 0, 0, 0), (0x35, 0, 1, 0x40000000), (0x06, 0, 0, 0x00050000 | errno.EPERM)]
    for number in denied:
        instructions.extend([(0x15, 0, 1, number), (0x06, 0, 0, 0x00050000 | errno.EPERM)])
    instructions.append((0x06, 0, 0, 0x7FFF0000))
    filters = (Filter * len(instructions))(*(Filter(*item) for item in instructions))
    program = Program(len(filters), filters)
    libc = ctypes.CDLL(None, use_errno=True)
    require(libc.prctl(38, 1, 0, 0, 0) == 0, "cannot enforce no-new-privileges")
    require(libc.prctl(22, 2, ctypes.byref(program), 0, 0) == 0, "cannot install native process seccomp boundary")
    # These synthetic invalid requests cannot execute real code even if the
    # filter were broken. EPERM (not ENOENT/EFAULT/ENOSYS) proves native denial.
    for number, arguments in ((execve, (ctypes.c_char_p(b"/synthetic-absent-executable"), ctypes.c_void_p(), ctypes.c_void_p())), (clone3, (ctypes.c_void_p(), ctypes.c_size_t(0)))):
        ctypes.set_errno(0)
        returned = libc.syscall(ctypes.c_long(number), *arguments)
        require(returned == -1 and ctypes.get_errno() == errno.EPERM, "native process denial probe failed")


def main():
    # Fixed synthetic negative controls exercise full-run rejection. They never
    # accept source paths, code strings or authored command input.
    require(sys.argv[1:] in ([], ["--negative=skipped"], ["--negative=caught-network"]), "unknown harness mode")
    prove_container_boundary()
    install_process_boundary()
    print("Verified nonroot/capability/read-only boundaries, kernel host/external network denial, inaccessible/absent credentials, and native process denial.", flush=True)
    current = [None]
    attempts = collections.Counter()
    blocked = {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn", "os.fork", "os.forkpty", "socket.__new__", "socket.connect", "socket.bind", "socket.getaddrinfo", "socket.gethostbyname", "socket.gethostbyaddr"}

    def audit(event, args):
        if event in blocked:
            attempts[(current[0], event)] += 1
            raise AssertionError("real process/network call forbidden in isolated helper tests")

    sys.addaudithook(audit)

    class Result(unittest.TextTestResult):
        def startTest(self, test):
            current[0] = test.id()
            super().startTest(test)

        def stopTest(self, test):
            super().stopTest(test)
            current[0] = None

    suite = unittest.defaultTestLoader.discover("/operator-tests/buildspec", pattern="runner_test.py")
    if sys.argv[1:] == ["--negative=skipped"]:
        @unittest.skip("synthetic skip rejection proof")
        def synthetic_skip():
            pass
        suite.addTest(unittest.FunctionTestCase(synthetic_skip))
    elif sys.argv[1:] == ["--negative=caught-network"]:
        def synthetic_caught_attempt():
            try:
                socket.socket()
            except AssertionError:
                pass
        suite.addTest(unittest.FunctionTestCase(synthetic_caught_attempt))
    result = unittest.TextTestRunner(verbosity=2, resultclass=Result).run(suite)
    expected_test = "runner_test.HelperContracts.test_guard_rejects_real_process_and_socket_before_execution"
    require(attempts == collections.Counter({(expected_test, "subprocess.Popen"): 1, (expected_test, "socket.__new__"): 1}), "unexpected caught process/network attempt (or missing exact guard self-test)")
    require(result.testsRun > 0 and not result.skipped and result.wasSuccessful(), "stdlib helper contracts failed, skipped or empty")
    print(f"PASS buildspec stdlib helper: {result.testsRun} tests, zero skips, no authored command execution.", flush=True)


if __name__ == "__main__":
    main()
