# dmf-mf-mxl-reader

Consumer-side container image for MXL flows. `mxl-reader` holds each flow it is
given open and reads every grain, or every batch of samples, as it lands.

An open reader is what makes mxl-k8s mirror a flow onto the reader's node, and
reading at the head shows whether that copy keeps up. The reader is built inside
a [`dmf-mxl/mxl`](https://github.com/dmf-mxl/mxl) tree against the
`ghcr.io/qvest-digital/go-mxl-runtime` libmxl, the same pair
`dmf-mf-mxl-writer` builds against, so reader and writer share one libmxl ABI.

## Image

`ghcr.io/marquies/dmf-mf-mxl-reader:<tag>`, named after the repository that
builds it.

Tags follow `<git-sha>` (every build), `latest` (main only), and `<version>` on
release.

## Usage

```
mxl-reader -d <domain> -f <flow-id> [-f <flow-id> ...] [--expect <json>] [-p 9102] [--report-interval 10]
```

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
show the head and the reader. Below it: the number of slots, the payload of
one slot (a grain, or one sample of every channel), the payload of the whole
ring, and the media time the ring holds. A strip beside it plots every grain of the last
six seconds on a log scale from 0.1 ms to 1 s: when the reader saw it, and when
it was committed on this node, which libmxl records for the flow's head and so
is known for a grain read while it is the head. Audio writers record no commit
time. A port-forward to 9102 is enough to open it:

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
| `/stream` | server-sent events, 20 batches a second: per flow its ring length, the payload bytes of one slot, period, grace, reader and head index, and every grain read since the last batch as `[index, seen ms, arrival ms, commit ms or null, behind head, kind, count]`; at most 8 at once |
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
`flow_error`, `open`), and `mxl_reader_flow_info` its media type and unit once
opened. `mxl_reader_stalls_total`, `mxl_reader_opens_total`,
`mxl_reader_lost_total` and `mxl_reader_age_seconds` complete the set.

An id that is not a lowercase flow UUID, or an `--expect` that is unreadable,
names a flow not read, or a key the reader does not check, exits with status 2; a port that cannot be bound exits with status 1.

## Build locally

```bash
docker build -t dmf-mf-mxl-reader:dev .
```

The build runs `mxl-reader-tests`, the checks in `src/checks.hpp` and the
request handling in `src/http.hpp`, and fails with them.

`GO_MXL_TAG` and `MXL_REF` move together with `dmf-mf-mxl-writer`'s, so the
reader links the libmxl the writer and the mxl-k8s gateway run.
