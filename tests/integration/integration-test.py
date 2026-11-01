"""mxl-reader against a real writer on a real domain.

Each test starts pattern-writer and mxl-reader on a fresh domain, has the
writer produce one fault, and checks how the reader counted it in /status.

    python3 integration-test.py <mxl-reader> <mxl-pattern-writer>
"""

import base64
import json
import math
import os
import shutil
import signal
import socket
import subprocess
import struct
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

READER = WRITER = None

VIDEO = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
AUDIO = "b3bb5be7-9fe9-4324-a5bb-4c70e1084449"
GRAIN_RATE = 50

# Small enough for the 64 MiB /dev/shm of a container build: v210 at 48x8.
VIDEO_DEF = {
    "id": VIDEO, "label": "video", "description": "video", "parents": [],
    "tags": {"urn:x-nmos:tag:grouphint/v1.0": ["integration-test:Video"]},
    "format": "urn:x-nmos:format:video", "media_type": "video/v210",
    "grain_rate": {"numerator": GRAIN_RATE, "denominator": 1},
    "frame_width": 48, "frame_height": 8, "interlace_mode": "progressive", "colorspace": "BT709",
    "components": [
        {"name": "Y", "width": 48, "height": 8, "bit_depth": 10},
        {"name": "Cb", "width": 24, "height": 8, "bit_depth": 10},
        {"name": "Cr", "width": 24, "height": 8, "bit_depth": 10},
    ],
}
AUDIO_DEF = {
    "id": AUDIO, "label": "audio", "description": "audio", "parents": [],
    "tags": {"urn:x-nmos:tag:grouphint/v1.0": ["integration-test:Audio"]},
    "format": "urn:x-nmos:format:audio", "media_type": "audio/float32",
    "sample_rate": {"numerator": 48000}, "channel_count": 2, "bit_depth": 32,
}
EXPECT = {
    VIDEO: {"mediaType": "video/v210", "width": 48, "height": 8, "rate": "50/1", "checkLegal": True},
    AUDIO: {"mediaType": "audio/float32", "sampleRate": 48000, "channels": 2},
}
ERRORS = ("too_early", "too_late", "missing", "invalid", "incomplete", "wrong_size", "wrong_format",
          "illegal_values", "permission_error", "other_error")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ReaderTest(unittest.TestCase):
    expect = EXPECT
    writer_first = True
    # The id of the domain, written to its domain_def.json, where set.
    domain_id = None

    def setUp(self):
        shm = "/dev/shm" if os.path.isdir("/dev/shm") else None
        self.domain = tempfile.mkdtemp(prefix="mxl-reader-it-", dir=shm)
        if self.domain_id:
            with open(os.path.join(self.domain, "domain_def.json"), "w") as f:
                json.dump({"id": self.domain_id}, f)
        self.defs = tempfile.mkdtemp(prefix="mxl-reader-it-defs-")
        for name, definition in (("video.json", VIDEO_DEF), ("audio.json", AUDIO_DEF)):
            with open(os.path.join(self.defs, name), "w") as f:
                json.dump(definition, f)
        self.port = free_port()
        self.writer = self.reader = None
        if self.writer_first:
            self.start_writer()
            self.start_reader()
            self.wait_for(lambda: self.flow(VIDEO)["delivering"] and self.flow(AUDIO)["delivering"], "both flows delivering")

    def tearDown(self):
        if self.writer is not None:
            if self.writer.poll() is None:
                self.writer.stdin.write("quit\n")
                self.writer.stdin.flush()
                self.writer.wait(5)
            self.writer.stdin.close()
            self.writer.stdout.close()
        if self.reader is not None:
            self.reader.send_signal(signal.SIGCONT)
            self.reader.terminate()
            self.reader.wait(5)
        shutil.rmtree(self.domain, ignore_errors=True)
        shutil.rmtree(self.defs, ignore_errors=True)

    def start_writer(self, *options):
        self.writer = subprocess.Popen(
            [WRITER, "-d", self.domain, "-v", os.path.join(self.defs, "video.json"), "-a", os.path.join(self.defs, "audio.json"),
             *options],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.assertEqual(self.writer.stdout.readline().strip(), "ready")

    flows = (VIDEO, AUDIO)
    options = ()

    def start_reader(self, user=None, env=None):
        flows = [arg for flow in self.flows for arg in ("-f", flow)]
        self.reader = subprocess.Popen(
            [READER, "-d", self.domain, *flows, "--expect", json.dumps(self.expect),
             "-p", str(self.port), "--report-interval", "0", *self.options],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, user=user, env={**os.environ, **(env or {})})
        self.wait_for(self.serving, "the reader serving /status")

    def command(self, line):
        self.writer.stdin.write(line + "\n")
        self.writer.stdin.flush()
        self.assertEqual(self.writer.stdout.readline().strip(), "done", line)

    def serving(self):
        try:
            self.status()
            return True
        except OSError:
            return False

    def status(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/status", timeout=2) as r:
            return {f["id"]: f for f in json.load(r)["flows"]}

    def flow(self, flow_id):
        return self.status()[flow_id]

    def wait_for(self, condition, what, timeout=10):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if condition():
                return
            time.sleep(0.05)
        self.fail(f"timed out waiting for {what}: {json.dumps(self.status(), indent=1)}")

    # Counters of one flow after minus before, reads and the rest alike.
    @staticmethod
    def delta(before, after):
        d = {k: after["reads"][k] - before["reads"][k] for k in after["reads"]}
        d.update({"open " + k: after["openFailures"][k] - before["openFailures"][k] for k in after["openFailures"]})
        d.update({k: after[k] - before[k] for k in ("skipped", "stalls", "opens", "lost")})
        return d

    # Exactly `low`, or between `low` and `high`, which may be math.inf.
    def assertCount(self, delta, key, low, high=None):
        high = low if high is None else high
        self.assertTrue(low <= delta[key] <= high, f"{key} not in [{low}, {high}]: {delta}")

    def assertOnly(self, delta, allowed, flow):
        unexpected = {k: v for k, v in delta.items() if v and k != "ok" and k not in allowed}
        self.assertEqual(unexpected, {}, f"{flow}: counted beyond {sorted(allowed)}: {delta}")


class Writing(ReaderTest):
    def test_a_writer_at_the_media_clock_reads_clean(self):
        before = self.status()
        time.sleep(2)
        after = self.status()
        for flow in (VIDEO, AUDIO):
            d = self.delta(before[flow], after[flow])
            self.assertOnly(d, set(), flow)
            self.assertTrue(after[flow]["delivering"], flow)
        self.assertCount(self.delta(before[VIDEO], after[VIDEO]), "ok", 80, math.inf)
        self.assertCount(self.delta(before[AUDIO], after[AUDIO]), "ok", 80000, math.inf)

    def test_grains_flagged_invalid_count_once(self):
        before = self.flow(VIDEO)
        self.command("invalid 3")
        self.wait_for(lambda: self.flow(VIDEO)["reads"]["invalid"] - before["reads"]["invalid"] >= 3, "3 invalid reads")
        # Five more turns of the ten-grain ring: each slot that held an invalid
        # grain has been written again, and must read clean.
        time.sleep(1)
        d = self.delta(before, self.flow(VIDEO))
        self.assertCount(d, "invalid", 3)
        self.assertOnly(d, {"invalid"}, VIDEO)

    def test_grains_the_writer_leaves_part_written_are_invalid(self):
        before = self.flow(VIDEO)
        self.command("partial 2")
        time.sleep(0.5)
        d = self.delta(before, self.flow(VIDEO))
        # libmxl takes a grain as committed only once it is whole, so opening
        # the next one jumps over the part-written one and marks it invalid
        # before a reader can take it as incomplete.
        self.assertCount(d, "invalid", 2)
        self.assertOnly(d, {"invalid"}, VIDEO)

    def test_reserved_codes_are_illegal_values(self):
        before = self.flow(VIDEO)
        self.command("illegal 2")
        time.sleep(0.5)
        d = self.delta(before, self.flow(VIDEO))
        self.assertCount(d, "illegal_values", 2)
        self.assertOnly(d, {"illegal_values"}, VIDEO)


    def test_grains_claiming_the_wrong_size_are_wrong_size(self):
        before = self.flow(VIDEO)
        self.command("size 2")
        time.sleep(0.5)
        d = self.delta(before, self.flow(VIDEO))
        self.assertCount(d, "wrong_size", 2)
        self.assertOnly(d, {"wrong_size"}, VIDEO)


class WriterMetrics(ReaderTest):
    def start_writer(self):
        self.metrics_port = free_port()
        super().start_writer("--metrics-port", str(self.metrics_port))

    # The writer's mxl_writer_writes_total of one flow, by result.
    def writes(self, flow):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.metrics_port}/metrics", timeout=2) as r:
            prefix = 'mxl_writer_writes_total{flow="%s",result="' % flow
            return {line[len(prefix):line.index('"', len(prefix))]: float(line.split()[-1])
                    for line in r.read().decode().splitlines() if line.startswith(prefix)}

    def test_the_writer_counts_ok_only_what_reads_ok(self):
        writes, reads = self.writes(VIDEO), self.flow(VIDEO)
        self.command("invalid 5")
        time.sleep(1)
        writes_after, reads_after = self.writes(VIDEO), self.flow(VIDEO)
        d = self.delta(reads, reads_after)
        self.assertCount(d, "invalid", 5)
        # A grain flagged invalid is no ok write. The two snapshots are taken
        # a moment apart, which two grains cover; five miscounted would not be.
        ok = writes_after["ok"] - writes["ok"]
        self.assertTrue(d["ok"] - 2 <= ok <= d["ok"] + 2, f"writer ok {ok}, reader ok {d['ok']}")
        self.assertGreater(self.writes(AUDIO)["ok"], 0)


class Gaps(ReaderTest):
    def test_a_writer_jumping_within_the_ring_leaves_its_gap_invalid(self):
        before = self.flow(VIDEO)
        self.command("pause 0.1")
        time.sleep(1)
        d = self.delta(before, self.flow(VIDEO))
        # libmxl marks the grains a writer jumps over invalid; the pause is
        # five grain periods.
        self.assertCount(d, "invalid", 3, 7)
        self.assertOnly(d, {"invalid", "too_early", "stalls"}, VIDEO)

    def test_a_writer_stopped_longer_than_the_ring_is_too_late_once_back(self):
        before = self.status()
        # Two seconds: delivering turns false a second after the last read.
        self.writer.stdin.write("pause 2\n")
        self.writer.stdin.flush()
        self.wait_for(lambda: not self.flow(VIDEO)["delivering"], "video no longer delivering")
        self.assertEqual(self.writer.stdout.readline().strip(), "done")
        self.wait_for(lambda: self.flow(VIDEO)["delivering"] and self.flow(AUDIO)["delivering"], "both flows delivering again")
        time.sleep(0.5)
        after = self.status()
        video = self.delta(before[VIDEO], after[VIDEO])
        self.assertCount(video, "stalls", 1, math.inf)
        self.assertCount(video, "too_late", 1, math.inf)
        self.assertCount(video, "skipped", 60, math.inf)
        self.assertOnly(video, {"too_early", "too_late", "skipped", "stalls", "invalid"}, VIDEO)
        audio = self.delta(before[AUDIO], after[AUDIO])
        self.assertCount(audio, "stalls", 1, math.inf)
        self.assertOnly(audio, {"too_early", "too_late", "skipped", "stalls"}, AUDIO)

    def test_a_frozen_reader_is_too_late_and_skips_to_the_head(self):
        before = self.flow(VIDEO)
        self.reader.send_signal(signal.SIGSTOP)
        time.sleep(0.5)
        self.reader.send_signal(signal.SIGCONT)
        time.sleep(0.5)
        d = self.delta(before, self.flow(VIDEO))
        self.assertCount(d, "too_late", 1, math.inf)
        self.assertCount(d, "skipped", 10, math.inf)
        self.assertOnly(d, {"too_late", "skipped"}, VIDEO)


class Access(ReaderTest):
    writer_first = False

    def test_a_flow_not_there_yet_is_not_found_until_it_is(self):
        self.start_reader()
        self.wait_for(lambda: self.flow(VIDEO)["openFailures"]["not_found"] >= 1, "not_found counted")
        self.assertEqual(self.flow(VIDEO)["state"], "flow_not_found")
        self.start_writer()
        self.wait_for(lambda: self.flow(VIDEO)["delivering"], "video delivering")
        video = self.flow(VIDEO)
        self.assertEqual(video["state"], "open")
        self.assertEqual(video["opens"], 1)
        self.assertEqual(video["lost"], 0)

    def test_a_removed_flow_is_lost_and_reopened(self):
        self.start_writer()
        self.start_reader()
        self.wait_for(lambda: self.flow(VIDEO)["delivering"], "video delivering")
        self.command("remove")
        self.wait_for(lambda: self.flow(VIDEO)["openFailures"]["not_found"] >= 1, "not_found counted after removal")
        self.command("create")
        self.wait_for(lambda: self.flow(VIDEO)["delivering"], "video delivering again")
        for flow in (VIDEO, AUDIO):
            f = self.flow(flow)
            self.assertEqual(f["lost"], 1, flow)
            self.assertEqual(f["opens"], 2, flow)


# An OTLP/HTTP receiver taking JSON, which keeps every body by path.
class Collector(HTTPServer):
    def __init__(self):
        self.bodies = {}
        self.lock = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                body = json.loads(handler.rfile.read(int(handler.headers["Content-Length"])))
                with self.lock:
                    self.bodies.setdefault(handler.path, []).append(body)
                handler.send_response(200)
                handler.send_header("Content-Type", "application/json")
                handler.send_header("Content-Length", "2")
                handler.end_headers()
                handler.wfile.write(b"{}")

            def log_message(handler, *args):
                pass

        super().__init__(("127.0.0.1", 0), Handler)

    def received(self, path):
        with self.lock:
            return list(self.bodies.get(path, []))


class Otlp(ReaderTest):
    writer_first = False

    def setUp(self):
        super().setUp()
        self.collector = Collector()
        threading.Thread(target=self.collector.serve_forever, daemon=True).start()

    def tearDown(self):
        super().tearDown()
        self.collector.shutdown()
        self.collector.server_close()

    # Every OTLP log record received, and every data point of the metric
    # `name` of the video flow, newest last.
    def records(self):
        return [r for b in self.collector.received("/v1/logs") for rl in b["resourceLogs"]
                for sl in rl["scopeLogs"] for r in sl["logRecords"]]

    def video_points(self, name):
        def flow(p):
            return any(a["key"] == "mxl.flow.id" and a["value"]["stringValue"] == VIDEO for a in p["attributes"])
        return [p for b in self.collector.received("/v1/metrics") for rm in b["resourceMetrics"]
                for sm in rm["scopeMetrics"] for m in sm["metrics"] if m["name"] == name
                for p in m["sum"]["dataPoints"] if flow(p)]

    def test_counts_and_events_are_pushed_where_a_collector_is_named(self):
        self.start_reader(env={
            "OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{self.collector.server_port}",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/json",
            "OTEL_METRIC_EXPORT_INTERVAL": "500",
            "OTEL_BLRP_SCHEDULE_DELAY": "200",
        })
        self.wait_for(lambda: self.flow(VIDEO)["openFailures"]["not_found"] >= 1, "not_found counted")
        self.start_writer()
        self.wait_for(lambda: self.flow(VIDEO)["delivering"], "video delivering")

        def ok_reads():
            return [int(p["asInt"]) for p in self.video_points("mxl.reader.reads")
                    if {"key": "mxl.read.result", "value": {"stringValue": "ok"}} in p["attributes"]]
        self.wait_for(lambda: any(n > 0 for n in ok_reads()), "ok reads of the video flow over OTLP")
        resource = self.collector.received("/v1/metrics")[0]["resourceMetrics"][0]["resource"]["attributes"]
        self.assertIn({"key": "service.name", "value": {"stringValue": "mxl-reader"}}, resource)

        # The flow missing at the start and found later is an event each way,
        # which only a polling reader raises.
        self.wait_for(lambda: {"mxl.flow.open_failed", "mxl.flow.opened"} <= {r.get("eventName") for r in self.records()},
                      "open_failed and opened log records")


# A controller's end of IS-12: a WebSocket (RFC 6455) carrying NMOS Control
# Protocol commands, written against the standard library alone.
class Ncp:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f"GET /x-nmos/ncp/v1.0 HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n"
                           f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.sock.recv(1)
            if not chunk:
                raise OSError("IS-12 closed during the handshake")
            head += chunk
        if not head.startswith(b"HTTP/1.1 101"):
            raise OSError(f"IS-12 refused the WebSocket: {head!r}")
        self.handle = 0

    def close(self):
        self.sock.close()

    def exactly(self, n):
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise OSError("IS-12 closed")
            data += chunk
        return data

    def send(self, opcode, payload):
        mask = os.urandom(4)
        n = len(payload)
        head = bytes([0x80 | opcode]) + (bytes([0x80 | n]) if n < 126 else
                                         bytes([0x80 | 126]) + struct.pack("!H", n) if n < 65536 else
                                         bytes([0x80 | 127]) + struct.pack("!Q", n))
        self.sock.sendall(head + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def message(self):
        data = b""
        while True:
            b0, b1 = self.exactly(2)
            n = b1 & 0x7F
            n = struct.unpack("!H", self.exactly(2))[0] if n == 126 else struct.unpack("!Q", self.exactly(8))[0] if n == 127 else n
            mask = self.exactly(4) if b1 & 0x80 else None
            payload = self.exactly(n)
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            opcode = b0 & 0x0F
            if opcode == 9:
                self.send(10, payload)
            elif opcode == 8:
                raise OSError("IS-12 closed")
            elif opcode in (0, 1):
                data += payload
                if b0 & 0x80:
                    return json.loads(data)

    # Invokes a method and returns its result; notifications in between are
    # dropped, none being subscribed to.
    def call(self, oid, level, index, arguments):
        self.handle += 1
        self.send(1, json.dumps({"messageType": 0, "commands": [
            {"handle": self.handle, "oid": oid, "methodId": {"level": level, "index": index}, "arguments": arguments}]}).encode())
        while True:
            m = self.message()
            if m.get("messageType") == 1:
                response = next(r for r in m["responses"] if r["handle"] == self.handle)
                return response["result"]

    def get(self, oid, level, index):
        return self.call(oid, 1, 1, {"id": {"level": level, "index": index}})["value"]


# NcReceiverMonitor (BCP-008-01): property and method ids.
OVERALL = (3, 1)
CONNECTION = (4, 4)
CONNECTION_MESSAGE = (4, 5)
LINK = (4, 1)
SYNCHRONIZATION = (4, 7)
GET_LOST = (4, 1)
GET_LATE = (4, 2)
RESET = (4, 3)
INACTIVE, HEALTHY, UNHEALTHY = 0, 1, 3


# The reader as an NMOS Node served by libnvnmos: its Receivers connected
# and disconnected over IS-05, and each one's BCP-008-01 monitor over IS-12.
class Nmos(ReaderTest):
    domain_id = "8e1b7ba2-7c3f-4a5c-9d4e-21c33bd6a0a1"
    flows = (f"monitor:video={VIDEO}", f"monitor:audio={AUDIO}")
    expect = {"monitor:video": EXPECT[VIDEO], "monitor:audio": EXPECT[AUDIO]}

    writer_first = False

    def setUp(self):
        self.nmos_port, self.ncp_port = free_port(), free_port()
        self.options = ("--nmos-seed", "integration-test", "--nmos-port", str(self.nmos_port),
                        "--nmos-control-port", str(self.ncp_port))
        super().setUp()
        self.start_writer()
        self.start_reader()
        self.wait_for(lambda: self.flow("monitor:video")["delivering"] and self.flow("monitor:audio")["delivering"],
                      "both slots delivering")
        self.wait_for(self.nmos_serving, "the NMOS Node serving")
        self.ncp = Ncp(self.ncp_port)

    def tearDown(self):
        # Closed first: a client that leaves the closing handshake unanswered
        # holds the IS-12 server's shutdown for its five-second timeout.
        self.ncp.close()
        super().tearDown()

    def nmos_serving(self):
        try:
            self.nmos("GET", "/x-nmos/node/v1.3/self")
            return True
        except OSError:
            return False

    def nmos(self, method, path, body=None, headers=None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.nmos_port}{path}", method=method,
                                         data=None if body is None else json.dumps(body).encode(),
                                         headers={"Origin": "http://registry.example", "Content-Type": "application/json",
                                                  **(headers or {})})
        with urllib.request.urlopen(request, timeout=2) as r:
            return r.status, r.headers, r.read()

    def json(self, path):
        return json.loads(self.nmos("GET", path)[2])

    def receiver(self, label):
        return next(r for r in self.json("/x-nmos/node/v1.3/receivers/") if r["label"] == label)

    def connection(self, receiver):
        version = self.json("/x-nmos/connection/")[-1]
        return f"/x-nmos/connection/{version}single/receivers/{receiver['id']}"

    def monitor(self, name):
        members = self.ncp.call(1, 2, 1, {"recurse": True})["value"]
        return next(m["oid"] for m in members if m["role"] == f"receiver-monitor-{name}")

    def wait_status(self, oid, prop, value, what, timeout=15):
        self.wait_for(lambda: self.ncp.get(oid, *prop) == value, what, timeout)

    def lost(self, oid):
        return {c["name"]: c["value"] for c in self.ncp.call(oid, *GET_LOST, {})["value"]}

    def test_each_receiver_has_a_monitor_at_its_touchpoint(self):
        device = self.json("/x-nmos/node/v1.3/devices/")[0]
        self.assertTrue(any(c["type"].startswith("urn:x-nmos:control:ncp/") for c in device["controls"]), device["controls"])
        for name, label in (("monitor:video", "monitor video"), ("monitor:audio", "monitor audio")):
            receiver = self.receiver(label)
            self.assertEqual(receiver["tags"]["urn:x-nmos:tag:grouphint/v1.0"], [f"monitor:{label.split()[1].title()}"])
            self.assertTrue(receiver["subscription"]["active"], label)
            touchpoints = self.ncp.get(self.monitor(name), 1, 7)
            self.assertEqual([t["resource"] for t in touchpoints], [{"resourceType": "receiver", "id": receiver["id"]}], name)
        video = self.receiver("monitor video")
        self.assertEqual(video["caps"]["constraint_sets"][0]["urn:x-nmos:cap:format:frame_width"], {"enum": [48]})
        active = self.json(self.connection(video) + "/active")
        self.assertEqual(active["transport_params"][0], {"mxl_domain_id": self.domain_id, "mxl_flow_id": VIDEO})

    def test_the_connection_follows_the_writer(self):
        oid = self.monitor("monitor:video")
        self.wait_status(oid, CONNECTION, HEALTHY, "a healthy connection once the reporting delay has passed")
        self.assertEqual((self.ncp.get(oid, *LINK), self.ncp.get(oid, *SYNCHRONIZATION)), (1, 0), "link up, no external sync")
        self.assertEqual(self.ncp.get(oid, *OVERALL), HEALTHY)
        self.ncp.call(oid, *RESET, {})
        self.assertEqual(self.lost(oid), {"skipped": 0, "missing": 0})
        self.writer.stdin.write("pause 3\n")
        self.writer.stdin.flush()
        # A status getting worse is reported at once.
        self.wait_status(oid, CONNECTION, UNHEALTHY, "an unhealthy connection while the writer stops", timeout=3)
        self.assertNotEqual(self.ncp.get(oid, *CONNECTION_MESSAGE), "")
        self.assertEqual(self.writer.stdout.readline().strip(), "done")
        self.wait_status(oid, CONNECTION, HEALTHY, "a healthy connection again after the reporting delay")
        # The ring overtook the reader during the pause: what it jumped over
        # never arrived.
        self.assertGreater(self.lost(oid)["skipped"], 0)
        self.assertEqual(len(self.ncp.call(oid, *GET_LATE, {})["value"]), 1)

    def test_a_connection_moves_the_slot_and_a_disconnect_stops_it(self):
        oid = self.monitor("monitor:video")
        staged = self.connection(self.receiver("monitor video")) + "/staged"
        status, _, _ = self.nmos("PATCH", staged, {"master_enable": False, "activation": {"mode": "activate_immediate"}})
        self.assertEqual(status, 200)
        self.wait_for(lambda: self.flow("monitor:video")["state"] == "disconnected", "the slot disconnected")
        self.assertEqual(self.flow("monitor:video")["flowId"], "")
        self.wait_status(oid, CONNECTION, INACTIVE, "an inactive monitor once disconnected", timeout=3)
        self.assertFalse(self.receiver("monitor video")["subscription"]["active"])

        status, _, _ = self.nmos("PATCH", staged, {"master_enable": True, "activation": {"mode": "activate_immediate"},
                                                   "transport_params": [{"mxl_domain_id": "auto", "mxl_flow_id": VIDEO}]})
        self.assertEqual(status, 200)
        self.wait_for(lambda: self.flow("monitor:video")["delivering"], "the slot delivering again")
        self.wait_status(oid, CONNECTION, HEALTHY, "a healthy connection after the reporting delay")

        # A domain the reader does not read is refused before it moves anything.
        with self.assertRaises(urllib.error.HTTPError) as refused:
            self.nmos("PATCH", staged, {"transport_params": [{"mxl_domain_id": "3310f209-9351-47c0-b9a2-14c59b6a4c23"}]})
        self.assertEqual(refused.exception.code, 400)
        self.assertTrue(self.flow("monitor:video")["delivering"])

    # Reached the way a controller running in a browser on another origin,
    # such as a registry's web UI, reaches it.
    def test_a_browser_controller_reaches_the_connection_api(self):
        staged = self.connection(self.receiver("monitor audio")) + "/staged"
        status, headers, _ = self.nmos("OPTIONS", staged, headers={
            "Access-Control-Request-Method": "PATCH", "Access-Control-Request-Headers": "content-type"})
        self.assertIn(status, (200, 204))
        self.assertIn("PATCH", headers["Access-Control-Allow-Methods"])
        self.assertIn("content-type", headers["Access-Control-Allow-Headers"].lower())
        status, headers, _ = self.nmos("GET", staged)
        self.assertEqual((status, headers["Access-Control-Allow-Origin"]), (200, "*"))


# A reader that is refused a flow, where root would be let in. libmxl reports
# no permission status to a reader, so this is the library's own check of the
# flow's files. A read refused once the flow is open is not covered: libmxl
# returns no such status from a read.
@unittest.skipUnless(os.geteuid() == 0, "starting the reader as another user needs root")
class Permissions(ReaderTest):
    writer_first = False

    def test_a_flow_the_reader_may_not_open_is_open_permission(self):
        os.chmod(self.domain, 0o755)
        self.start_writer()
        video_dir = os.path.join(self.domain, VIDEO + ".mxl-flow")
        os.chmod(video_dir, 0o700)
        self.start_reader(user="nobody")
        self.wait_for(lambda: self.flow(VIDEO)["openFailures"]["permission"] >= 1, "open permission counted")
        self.assertEqual(self.flow(VIDEO)["state"], "permission_denied")
        self.wait_for(lambda: self.flow(AUDIO)["delivering"], "audio, which nobody may read, delivering")
        os.chmod(video_dir, 0o755)
        self.wait_for(lambda: self.flow(VIDEO)["delivering"], "video delivering once readable")
        video = self.flow(VIDEO)
        self.assertEqual(video["openFailures"]["not_found"], 0)
        self.assertEqual(video["openFailures"]["other"], 0)


class Expectations(ReaderTest):
    expect = {VIDEO: dict(EXPECT[VIDEO], rate="25/1"), AUDIO: EXPECT[AUDIO]}

    def setUp(self):
        self.writer_first = False
        super().setUp()
        self.start_writer()
        self.start_reader()

    def test_a_flow_declaring_other_than_expected_is_wrong_format(self):
        time.sleep(1)
        video = self.flow(VIDEO)
        self.assertEqual(video["reads"]["ok"], 0)
        self.assertGreaterEqual(video["reads"]["wrong_format"], 30)
        self.assertNotEqual(video["formatMismatch"], "")
        self.assertGreaterEqual(self.flow(AUDIO)["reads"]["ok"], 1)


if __name__ == "__main__":
    READER, WRITER = sys.argv[1], sys.argv[2]
    unittest.main(argv=sys.argv[:1], verbosity=2)
