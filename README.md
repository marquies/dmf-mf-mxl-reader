# dmf-mf-mxl-reader

Consumer-side container image for MXL flows. `mxl-reader` holds each flow it is
given open and reads every grain, or every batch of samples, as it lands.

An open reader is what makes mxl-k8s mirror a flow onto the reader's node, and
reading at the head shows whether that copy keeps up. The reader is built inside
a [`dmf-mxl/mxl`](https://github.com/dmf-mxl/mxl) tree against the
`ghcr.io/qvest-digital/go-mxl-runtime` libmxl, the same pair
`dmf-mf-mxl-writer` builds against, so reader and writer share one libmxl ABI.
What it counts and checks of each read, and the `/metrics` text, come from
[`libmxl-monitoring`](https://github.com/marquies/libmxl-monitoring), built
into the same tree and linked statically.

## Image

`ghcr.io/marquies/dmf-mf-mxl-reader:<tag>`, named after the repository that
builds it.

Tags follow `<git-sha>` (every build), `latest` (main only), and `<version>` on
release.

## Usage

```
mxl-reader -d <domain> -f <flow> [-f <flow> ...] [--expect <json>] [-p 9102] [--report-interval 10]
           [--nmos-seed <seed> [--nmos-registry http://<host>[:<port>]] [--nmos-host <host>] [--nmos-label <label>]]
```

A `<flow>` is a flow id, or a named slot `<name>:<video|audio|data>[=<flow id>]`
that an NMOS controller can move to other flows (see NMOS below). Counters,
`--expect` and `/status` key a slot by its flow id, or by `<name>:<kind>` for a
named slot, so one name can hold a video, an audio and a data slot.

The first line on stderr names the reader's version and the libmxl it runs
against; `--version` prints the same and exits.

A domain or flow that cannot be opened yet is retried every second rather than
failing the process, and a flow that goes away is reopened once it is back.

`--expect` states what each flow should carry, keyed by flow id. A flow that
declares something else counts every read as `wrong_format`:

```json
{"<flow id>": {"mediaType": "video/v210", "width": 1920, "height": 1080, "rate": "50/1", "checkLegal": true},
 "<flow id>": {"mediaType": "audio/float32", "sampleRate": 48000, "channels": 2}}
```

The ring view draws each flow's ring with a slot per grain, coloured by when
its last grain arrived against its media timestamp: within a grain period,
within the too-early grace, or later, or by what was wrong with it. Markers
show the head and the reader. Above it: the full id of the flow being read.
Below it: the number of slots, the payload of one slot (a grain, or one sample
of every channel), the payload of the whole ring, and the media time the ring
holds. A strip beside it plots every grain of the last six seconds on a log
scale from 0.1 ms to 1 s: when the reader saw it, and when it was committed on
this node, which libmxl records for the flow's head and so is known for a grain
read while it is the head. Audio writers record no commit time. A port-forward
to 9102 is enough to open it:

```
kubectl -n <namespace> port-forward svc/<reader claim> 9102
```

`checkLegal` also checks sample values: v210 codes 0-3 and 1020-1023, which are
reserved, in the first, middle and last row of each grain, and float audio
outside -1..1 or not finite. It is off unless asked, because a test pattern may
carry reserved codes on purpose: SMPTE bars write super-black as luma code 0.

| path | answers |
| --- | --- |
| `/` | the ring view, a page drawing each flow's ring from `/stream` |
| `/stream` | server-sent events, 20 batches a second: per flow the id of the flow it reads, its ring length, the payload bytes of one slot, period, grace, reader and head index, and every grain read since the last batch as `[index, seen ms, arrival ms, commit ms or null, behind head, kind, count]`; at most 8 at once |
| `/status` | JSON, one object per flow: state, declared format, every counter, last error |
| `/metrics` | Prometheus text, the same counters |
| `/delivering` | 200 while every flow had a read land within the last second, 503 otherwise |
| `/healthz` | 200 while the process serves. A readiness probe belongs here, not on `/delivering`: a reader waiting for a flow is working as intended. |

Every read lands in exactly one outcome of `mxl_reader_reads_total{result}`, the first that applies in this order,
counted in grains for video and data and in samples for audio, except
`too_early` and `too_late`, which count events:

| result | |
| --- | --- |
| `ok` | read in full, as declared and expected |
| `too_early` | not there a grace period after it was due: the read span, or the writer's commit batch for samples, plus 50 ms |
| `too_late` | overwritten before it was read; the reader jumps to the head and adds what it passed to `mxl_reader_skipped_total` |
| `missing` | the ring slot holds an older cycle's grain: the writer or the mirror never wrote this index |
| `invalid` | carries the writer's invalid flag |
| `incomplete` | fewer valid slices than the grain has, taken once a later grain exists |
| `wrong_size` | grain size differs from what the declared v210 geometry needs, one field for an interlaced flow |
| `wrong_format` | the flow declares other than `--expect` states |
| `illegal_values` | a sample outside the legal range, with `checkLegal` |
| `permission_error` | reading was refused |
| `other_error` | any other read failure; the flow is reopened |

`mxl_reader_open_failures_total{reason}` counts attempts to reach a flow that
failed: `domain` unavailable, flow `not_found`, `permission` denied, or
`other`. `mxl_reader_flow_state{state}` names where each flow stands
(`starting`, `domain_unavailable`, `flow_not_found`, `permission_denied`,
`flow_error`, `open`, `disconnected`), and `mxl_reader_flow_info` its media
type and unit once opened. `mxl_reader_stalls_total`, `mxl_reader_opens_total`,
`mxl_reader_lost_total` and `mxl_reader_age_seconds` complete the set, with two
histograms: `mxl_reader_arrival_seconds`, read time minus media timestamp, and
`mxl_reader_commit_seconds`, commit time minus media timestamp for grains read
while they are the head.

## NMOS

With `--nmos-seed` the reader is an NMOS Node (IS-04 v1.3, IS-05 v1.1) as AMWA
BCP-007-03 specifies for MXL, served on the same port as the other endpoints.
Every `-f` is then a named slot, and each slot is an MXL Receiver whose format
follows its kind and whose BCP-004-01 capabilities follow its `--expect`
(geometry, rate, sample rate, channels). Receivers are grouped by the
`urn:x-nmos:tag:grouphint/v1.0` tag `<name>:Video`, `<name>:Audio` or
`<name>:Data`.

A controller connects a slot through the Connection API with a single leg of
`mxl_domain_id` and `mxl_flow_id`. `auto` for the domain resolves to the id in
`<domain>/domain_def.json`; another domain is refused, and `mxl_flow_id` takes
no `auto`. An immediate activation moves the slot's reader to the flow named,
and `master_enable` false moves it off; a flow that is not there yet is waited
for, as at start. Scheduled activations answer 501. A slot booked without a
flow reads nothing and does not count against `/delivering` until connected.

Resource ids derive from the seed, so they survive restarts. With
`--nmos-registry` the node registers with that IS-04 Registration API, sends a
heartbeat every five seconds, registers again when the registry has dropped
it, and posts a receiver again when its subscription changes. The node is
advertised at `--nmos-host` (default: the hostname) and `-p`.

The NMOS APIs are unauthenticated, as IS-04 and IS-05 are: whoever reaches the
port can move a slot to any flow in the domain.

An id that is not a lowercase flow UUID, or an `--expect` that is unreadable,
names a flow not read, or a key the reader does not check, exits with status 2, as does a
slot without a flow, a flow read twice, or an unnamed flow beside `--nmos-seed`; a port that cannot be bound exits with status 1.

## Build locally

```bash
docker build -t dmf-mf-mxl-reader:dev .
```

The build runs the unit tests in `tests/unittests`: `mxl-reader-tests`, the
`--expect` handling in `src/checks.hpp` and the request handling in
`src/http.hpp`, and `mxl-reader-nmos-tests`, the NMOS Node in `src/nmos.hpp`,
and fails with them. The checks a grain or a
batch of samples goes through are tested in libmxl-monitoring, which
`MONITORING_REF` pins.

It then runs `tests/integration/integration-test.py`: `mxl-pattern-writer`,
built from the same directory, writes a video and an audio flow into a domain
under `/dev/shm`, the reader reads them, and each test has the writer produce
one fault and checks what `/status` counted: clean writing, grains flagged
invalid, grains left part-written, reserved v210 codes, grains of the wrong
size, a writer pausing within and beyond the ring, a frozen reader, a flow not
there yet, a removed flow, a flow the reader may not open, and a flow declaring
other than `--expect` states. A part-written grain and a writer's gap both read
as `invalid`, since libmxl marks them so; `missing` and `incomplete` are left to
the libmxl-monitoring tests. The permission test runs the reader as `nobody`
and is skipped unless the suite runs as root, as it does in the image build.
`permission_error` on a read is not covered: libmxl returns no permission status
from a read.

`GO_MXL_TAG` and `MXL_REF` move together with `dmf-mf-mxl-writer`'s, so the
reader links the libmxl the writer and the mxl-k8s gateway run.
