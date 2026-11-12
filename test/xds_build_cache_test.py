#!/usr/bin/env python3
# JuiceFS, Copyright 2026 Juicedata, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run with python3 test/xds_build_cache_test.py; no xDS service is needed.

All builds and source edits happen in temporary directories. Set
XDS_TEST_MAKEFILE to an older Makefile to verify that the regression fails.
The real cgo test additionally requires Go and a native C compiler.
"""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
NATIVE_SOURCES = (
    "xDS/src/client/xds_client.c",
    "xDS/src/client/xds_control.c",
    "xDS/src/client/xds_shm_queue.c",
    "xDS/src/client/xds_cpu_buffer_mapping.c",
    "xDS/src/client/xds_namespace.c",
    "xDS/kernels/user/xds_p2p_user.c",
)
NATIVE_HEADERS = (
    "xDS/include/xds/client.h",
    "xDS/src/client/private.h",
    "xDS/kernels/include/p2p.h",
    "xDS/kernels/user/include/p2p.h",
)
HASH_FLAG = re.compile(r"-DJFS_XDS_NATIVE_HASH_[0-9a-f]{64}=1")

# Only the final build uses the real compiler in the cgo test. The other Go
# commands are recorded so the fixture needs neither JuiceFS dependencies nor
# SPDK. sha256sum is forwarded unchanged, including stdin for the outer hash.
TOOL_STUB = r'''
import json, os, subprocess, sys
from pathlib import Path

tool, args = Path(sys.argv[0]).name, sys.argv[1:]
with open(os.environ["XDS_TEST_LOG"], "a") as log:
    log.write(json.dumps({"tool": tool, "args": args,
                          "cflags": os.environ.get("CGO_CFLAGS"),
                          "service": os.environ.get("JFS_XDS_TEST_SERVICE")}) + "\n")
if tool == "sha256sum":
    sys.exit(subprocess.call([os.environ["XDS_TEST_SHA256SUM"], *args]))
if tool == "go" and args[0] == "env":
    print({"GOOS": "linux", "CGO_ENABLED": "1",
           "CGO_CFLAGS": os.environ.get("CGO_CFLAGS", os.environ["XDS_TEST_GOENV_CFLAGS"])}[args[1]])
if tool == "go" and args[0] == "build" and os.environ.get("XDS_TEST_REAL_GO"):
    output = args[args.index("-o") + 1]
    sys.exit(subprocess.call([os.environ["XDS_TEST_REAL_GO"], "build", "-buildvcs=false", "-o", output, "."]))
'''


class BuildFixture:
    def __init__(self, root):
        self.root = root
        self.log = root / "commands.jsonl"
        template = Path(os.environ.get("XDS_TEST_MAKEFILE", REPO_ROOT / "Makefile"))
        shutil.copyfile(template, root / "Makefile")
        self.env = os.environ.copy()
        # Keep local Go/make settings from turning this isolated probe into a
        # cross build, importing a parent workspace, or reusing a global cache.
        for key in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL", "MAKEOVERRIDES", "GOFLAGS",
                    "GOOS", "GOARCH", "GOEXPERIMENT", "CGO_CFLAGS", "CGO_CPPFLAGS",
                    "CGO_CXXFLAGS", "CGO_FFLAGS", "CGO_LDFLAGS"):
            self.env.pop(key, None)
        self.env.update({
            "XDS_TEST_LOG": str(self.log),
            "XDS_TEST_SHA256SUM": shutil.which("sha256sum"),
            "XDS_TEST_GOENV_CFLAGS": "-O0 -DPERSISTED_FLAG=1",
            "GOCACHE": str(root / "cache"),
            "GOPATH": str(root / "gopath"),
            "GOENV": "off", "GOWORK": "off", "GOPROXY": "off",
            "GOTOOLCHAIN": "local", "CGO_ENABLED": "1",
            "JFS_XDS_TEST_SERVICE": "must-not-connect",
        })
        for tool in ("go", "make", "sha256sum"):
            script = self.write("bin/" + tool, "#!" + sys.executable + "\n" + TOOL_STUB)
            script.chmod(0o755)
        self.env["PATH"] = str(root / "bin") + os.pathsep + self.env["PATH"]
        for name in (*NATIVE_SOURCES, *NATIVE_HEADERS):
            self.write(name, "/* initial input */\n")
        for name in ("xDS/Makefile", "xDS/spdk4xds/configure",
                     "xDS/spdk4xds/dpdk/meson.build"):
            self.write(name, "# fixture\n")
        # These files satisfy the ordinary juicefs target's prerequisite globs.
        self.write("cmd/main.go", "package cmd\n")
        self.write("pkg/xds/client.go", "package xds\n")
        self.write("go.mod", "module xdsbuildprobe\n\ngo 1.20\n")
        self.write("main.go", "package main\nfunc main() {}\n")

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def make(self, target="juicefs.xds", variables=None, environment=None):
        self.log.write_text("")
        env = dict(self.env, **(environment or {}))
        command = [shutil.which("make"), "--no-print-directory", target,
                   "MAKE=" + str(self.root / "bin/make")]
        command.extend(key + "=" + value for key, value in (variables or {}).items())
        result = subprocess.run(command, cwd=self.root, env=env,
                                capture_output=True, text=True, timeout=240)
        records = [json.loads(line) for line in self.log.read_text().splitlines()]
        return result, records


@unittest.skipUnless(shutil.which("make") and shutil.which("sha256sum"),
                     "make and sha256sum are required")
class XDSBuildCacheTest(unittest.TestCase):
    def fixture(self):
        temporary = tempfile.TemporaryDirectory(prefix="xds-build-cache-", dir="/tmp")
        self.addCleanup(temporary.cleanup)
        return BuildFixture(Path(temporary.name))

    def successful_flags(self, fixture, **kwargs):
        result, records = fixture.make(**kwargs)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = [record for record in records
                    if record["tool"] == "go" and record["args"][0] in ("test", "vet", "build")]
        self.assertEqual([record["args"][0] for record in commands], ["test", "vet", "build"])
        flags = commands[0]["cflags"] or ""
        self.assertEqual(len(HASH_FLAG.findall(flags)), 1, flags)
        self.assertTrue(all(record["cflags"] == flags for record in commands))
        self.assertEqual(commands[0]["service"], "")
        return flags

    def test_external_inputs_change_hash_but_build_products_do_not(self):
        fixture = self.fixture()
        previous = self.successful_flags(fixture)
        for name in (*NATIVE_SOURCES, *NATIVE_HEADERS):
            with self.subTest(input=name):
                path = fixture.root / name
                path.write_text(path.read_text() + "/* changed */\n")
                current = self.successful_flags(fixture)
                self.assertNotEqual(current, previous)
                previous = current
        for name in ("xDS/src/client/xds_client.o", "xDS/kernels/user/libxds_p2p_user.a"):
            with self.subTest(product=name):
                fixture.write(name, "changed build product\n")
                self.assertEqual(self.successful_flags(fixture), previous)

    def test_existing_cflags_survive_all_three_configuration_sources(self):
        fixture = self.fixture()
        cases = (
            ({}, {}, "-O0 -DPERSISTED_FLAG=1"),  # go env -w, not an environment variable.
            ({"CGO_CFLAGS": "-O1 -DENV_FLAG=2"}, {}, "-O1 -DENV_FLAG=2"),
            ({"CGO_CFLAGS": "-O1 -DENV_FLAG=2"},
             {"CGO_CFLAGS": "-O3 -DMAKE_FLAG=3"}, "-O3 -DMAKE_FLAG=3"),
        )
        for environment, variables, expected in cases:
            with self.subTest(expected=expected):
                flags = self.successful_flags(fixture, environment=environment, variables=variables)
                self.assertEqual(HASH_FLAG.sub("", flags).strip(), expected)

    def test_missing_c_source_fails_and_default_build_needs_no_xds(self):
        fixture = self.fixture()
        for name in NATIVE_SOURCES:
            with self.subTest(missing=name):
                path = fixture.root / name
                original = path.read_text()
                path.unlink()
                result, records = fixture.make()
                path.write_text(original)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(name, result.stderr)
                self.assertFalse(any(record["tool"] == "go" and record["args"][0]
                                     in ("test", "vet", "build") for record in records))
        shutil.rmtree(fixture.root / "xDS")
        result, records = fixture.make(target="juicefs")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(any(record["tool"] in ("make", "sha256sum") for record in records))

    @unittest.skipUnless(shutil.which("go") and shutil.which("cc"),
                         "Go and a native C compiler are required")
    def test_real_cgo_cache_rebuilds_after_external_header_and_source_changes(self):
        fixture = self.fixture()
        real_go = shutil.which("go")
        fixture.env.update({"XDS_TEST_REAL_GO": real_go, "CC": shutil.which("cc")})
        fixture.write("main.go", 'package main\n/*\nint cache_value(void);\n*/\n'
                      'import "C"\nimport "fmt"\nfunc main() { fmt.Println(C.cache_value()) }\n')
        fixture.write("native.c", '#include "xDS/src/client/xds_client.c"\n')
        header = fixture.write(NATIVE_HEADERS[0], "#define CACHE_VALUE 1\n")
        source = fixture.write(NATIVE_SOURCES[0],
                               '#include "../../include/xds/client.h"\n'
                               'int cache_value(void) { return CACHE_VALUE; }\n')

        def raw_build():
            result = subprocess.run([real_go, "build", "-buildvcs=false", "-o", "raw-probe", "."],
                                    cwd=fixture.root, env=fixture.env,
                                    capture_output=True, text=True, timeout=240)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return output("raw-probe")

        def output(name):
            return subprocess.check_output([str(fixture.root / name)], text=True).strip()

        self.assertEqual(raw_build(), "1")
        header.write_text("#define CACHE_VALUE 2\n")
        # Reproduce the bug: changing an external header does not invalidate
        # the native object's Go build cache entry by itself.
        self.assertEqual(raw_build(), "1")
        result, _ = fixture.make()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(output("juicefs.xds"), "2")
        header.write_text("#define CACHE_VALUE 3\n")
        result, _ = fixture.make()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(output("juicefs.xds"), "3")
        source.write_text(source.read_text().replace("return CACHE_VALUE;", "return CACHE_VALUE + 1;"))
        result, _ = fixture.make()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(output("juicefs.xds"), "4")


if __name__ == "__main__":
    unittest.main()
