# VPS deployment

The default deployment is a single, 30-minute BTC capture. Continuous capture is
available as an explicit mode. Canonical data is stored in an absolute host path
outside the repository and is never removed by Compose tasks.

## First deployment

Install Docker with the Compose plugin and [Task](https://taskfile.dev/), then run:

```bash
cp backend/.env.example backend/.env
```

Set `DATA_DIR` in `backend/.env` to an absolute VPS path. Review the remaining
limits, then prepare and validate the deployment:

```bash
task vps:init-data
task build
task preflight
task config
```

`vps:init-data` creates the directory for container UID `10001`. `preflight`
checks the resolved Compose configuration, container write access, and the
minimum available disk space.

## Capture modes

Run one capture in the background:

```bash
task start
task logs:backend
```

Run one capture attached to the console:

```bash
task capture
```

Continuously start a new Capture Run after each configured duration:

```bash
task continuous
```

Stop either mode gracefully:

```bash
task stop
```

Docker sends `SIGTERM`; the trader stops venue sessions, drains accepted events,
syncs the active segment, and finalizes the manifest. Compose allows up to one
minute before forcing termination.

## Validation and export

```bash
task captures
task replay CAPTURE_ID=<capture-uuid>
task export CAPTURE_ID=<capture-uuid> DATASET=<new-dataset-name>
```

Pass `CLI_ARGS=--allow-incomplete` only when intentionally inspecting an
incomplete Capture Run.

## Operations

```bash
task status
task logs
task restart
task down
```

The backend healthcheck verifies that the process is alive and that `DATA_DIR`
has at least `DATA_MIN_FREE_GIB` available. An unhealthy result is an alert; it
does not delete data. Docker JSON logs rotate according to `LOG_MAX_SIZE` and
`LOG_MAX_FILES`.

The frontend listens only on VPS loopback by default. Inspect it through an SSH
tunnel:

```bash
ssh -L 3000:127.0.0.1:3000 <user>@<vps-host>
```

Use a TLS reverse proxy before exposing the frontend publicly.
