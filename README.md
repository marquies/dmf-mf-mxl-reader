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

A domain or flow that cannot be opened yet is retried every second rather than
failing the process, and a flow that goes away is reopened once it is back.

`--expect` states what each flow should carry, keyed by flow id. A flow that
declares something else counts every read as `wrong_format`:

```json
{"<flow id>": {"mediaType": "video/v210", "width": 1920, "height": 1080, "rate": "50/1", "checkLegal": true},
 "<flow id>": {"mediaType": "audio/float32", "sampleRate": 48000, "channels": 2}}
```

`checkLegal` also checks sample values: v210 codes 0-3 and 1020-1023, which are
reserved, in the first, middle and last row of each grain, and float audio
outside -1..1 or not finite. It is off unless asked, because a test pattern may
carry reserved codes on purpose: SMPTE bars write super-black as luma code 0.

| path | answers |
| --- | --- |
| `/status` | JSON, one object per flow: state, declared format, every counter, last error |
| `/metrics` | Prometheus text, the same counters |
| `/delivering` | 200 while every flow had a read land within the last second, 503 otherwise |
| `/healthz` | 200 while the process serves. A readiness probe belongs here, not on `/delivering`: a reader waiting for a flow is working as intended. |

Every read lands in exactly one outcome of `mxl_reader_reads_total{result}`,
counted in grains for video and data and in samples for audio, except
`too_early` and `too_late`, which count events:

| result | |
| --- | --- |
| `ok` | read in full, as declared and expected |
| `too_early` | not there a grace period after it was due: the read span, or the writer's commit batch for samples, plus 50 ms |
| `too_late` | overwritten before it was read; the reader jumps to the head and adds what it passed to `mxl_reader_skipped_total` |
| `invalid` | carries the writer's invalid flag |
| `incomplete` | fewer valid slices than the grain has |
| `wrong_size` | grain size differs from what the declared v210 geometry needs |
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

An id that is not a lowercase flow UUID, or an unreadable `--expect`, exits
with status 2; a port that cannot be bound exits with status 1.

## Build locally

```bash
docker build -t dmf-mf-mxl-reader:dev .
```

`GO_MXL_TAG` and `MXL_REF` move together with `dmf-mf-mxl-writer`'s, so the
reader links the libmxl the writer and the mxl-k8s gateway run.
