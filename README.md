# dmf-mf-mxl-reader

Consumer-side container image for MXL flows. `mxl-reader` holds each flow it is
given open and reads every grain, or every batch of samples, as it lands.

An open reader is what makes mxl-k8s mirror a flow onto the reader's node, and
reading at the head shows whether that copy keeps up. The reader is built inside
a [`dmf-mxl/mxl`](https://github.com/dmf-mxl/mxl) tree against the
`ghcr.io/qvest-digital/go-mxl-runtime` libmxl, the same pair
`dmf-mf-mxl-writer` builds against, so reader and writer share one libmxl ABI.

## Image

`ghcr.io/qvest-digital/dmf-mf-mxl-reader:<tag>`

Tags follow `<git-sha>` (every build), `latest` (main only), and `<version>` on
release.

## Usage

```
mxl-reader -d <domain> -f <flow-id> [-f <flow-id> ...] [-p 9102] [--report-interval 10]
```

A flow that cannot be opened yet, because it is still being mirrored or its
writer has not started, is retried every second rather than failing the
process. A flow that goes away is reopened once it is back.

| path | answers |
| --- | --- |
| `/metrics` | Prometheus text, one series per flow |
| `/delivering` | 200 while every flow had a read land within the last second, 503 otherwise |
| `/healthz` | 200 while the process serves. A readiness probe belongs here, not on `/delivering`: a reader waiting for a flow is working as intended. |

| metric | |
| --- | --- |
| `mxl_reader_flow_open` | 1 while a reader holds the flow |
| `mxl_reader_flow_delivering` | 1 while reads land within a second |
| `mxl_reader_read_total` | grains (`unit="grains"`) or samples (`unit="samples"`) read; published once the flow has been opened |
| `mxl_reader_invalid_total` | grains read with the invalid flag set |
| `mxl_reader_skipped_total` | grains or samples jumped over to reach the head again after the ring overtook the reader; same unit as `read_total` |
| `mxl_reader_stalls_total` | read timeouts that passed with no read at all, counted once until reads resume |
| `mxl_reader_opens_total` | times the flow was opened |
| `mxl_reader_age_seconds` | wall clock minus the media timestamp of the last read |

A summary line per flow goes to stderr every `--report-interval` seconds. An id that is not a lowercase flow UUID exits with status 2; a domain that cannot be opened or a port that cannot be bound exits with status 1.

## Build locally

```bash
docker build -t dmf-mf-mxl-reader:dev .
```

`GO_MXL_TAG` and `MXL_REF` move together with `dmf-mf-mxl-writer`'s, so the
reader links the libmxl the writer and the mxl-k8s gateway run.
