# How a Ray Node Maps to a Medios Task

Each Ray head or worker is one Medios task, and each task has one pod record in the framework's store for the stock autoscaler to read ([docs/endpoint-contract.md](endpoint-contract.md)). This page describes what `ray-medios` puts in a Medios task and why.

- `tasks.Plan` (`internal/tasks`) maps a node description and containerizer to a command line, environment, port reservation, and containerizer fields.
- `launch.TaskInfo` (`internal/launch`) converts a `TaskPlan` into a `medios-sdk-go` `TaskInfo`.
- `internal/framework` reconciles `RayCluster` records against Medios offers and task status updates, and `cmd/ray-medios` registers with the Medios controller under the framework name `ray`.

## Containerizer

The containerizer is a required per-cluster parameter (`-containerizer`) with two supported values: `medios` and `docker_shell`. The caller also supplies at least one `-node-attribute` (`name=value`) so tasks land on the intended pool of nodes. The `ray start` command line is identical for both containerizers; containerizer-specific differences are listed below:

| Property | `medios` | `docker_shell` |
| --- | --- | --- |
| Network | Node's network namespace | Node's network namespace (`docker.network: HOST`) for head and workers |
| `LinuxInfo` | None | `ipc_mode: PRIVATE`, `shm_size` equal to the object store size in MiB |
| `/dev/shm` | Node tmpfs | Sized by `shm_size` (Docker defaults to 64 MiB without it) |
| `cpus`, `mem` visibility inside task | Host limits visible; Ray limits passed explicitly | Not enforced by containerizer; Ray limits passed explicitly |
| `SECRET` environment variables | Not delivered to tasks | Ignored |
| Image `ENTRYPOINT` | Not run; task command runs directly | Runs with task command as arguments; images should declare `ENTRYPOINT []` |
| `TaskInfo` image field | `container.type: MEDIOS` with `medios.image` | `container.type: DOCKER` with `docker.image` |
| Status (`v0.1.0`) | Verified for head + worker jobs and autoscaler scale-up/down | Verified for head + worker jobs and autoscaler scale-up/down (Ray 2.60.0) |

### Why `docker_shell` uses `docker.network: HOST`

On the Docker bridge network, three Ray listeners bind `--node-ip-address` directly rather than the wildcard address:

- Ray client server (binds node address and `127.0.0.1`)
- Dashboard agent HTTP (binds node address and `127.0.0.1`)
- Runtime environment agent (binds node address only)

Because `--node-ip-address` is also the address advertised to other Ray nodes, it must be reachable across hosts and cannot be an isolated bridge address. Under bridge networking, the head's three listeners fail to bind on startup and a worker's runtime environment agent exits shortly after registration. `ray-medios` therefore requests `docker.network: HOST` for all `docker_shell` tasks (supported in Medios `>= v0.1.14`). Note that a KubeRay pod has its own network namespace; host networking is an intentional Medios `docker_shell` departure in `v0.1.0`.

## Ports

Medios reserves ports only when a task's `resources` names them, and does not inject the assigned ports into the process. Because tasks on a node share the node's port space, every port a Ray process listens on is carved from the offer and passed explicitly on the `ray start` command line or via environment variables:

| Ray listener | Configured via | Head | Worker |
| --- | --- | --- | --- |
| GCS | `--port` | yes | no |
| Raylet | `--node-manager-port` | yes | yes |
| Object manager | `--object-manager-port` | yes | yes |
| Runtime environment agent | `--runtime-env-agent-port` | yes | yes |
| Metrics export | `--metrics-export-port` | yes | yes |
| Dashboard agent | `--dashboard-agent-listen-port`, `--dashboard-agent-grpc-port` | yes | yes |
| Ray worker processes and drivers | `--worker-port-list` | yes | yes |
| Ray client server | `--ray-client-server-port` | yes | no |
| Dashboard metrics | `DASHBOARD_METRIC_PORT` | yes | no |
| Dashboard | `--dashboard-port` | yes | no |
| Autoscaler metrics | `AUTOSCALER_METRIC_PORT` | yes | no |

Each Ray worker process and attached driver consumes one port from `--worker-port-list`. `-worker-ports` is required and has no default: too few ports (for example, 4 on a busy head) can trigger Ray's `No available ports` error, while overly large values reduce how many Ray nodes fit within a Medios node's offered port range.

## Addresses

- A node's address is the offer's `Hostname`, passed as `--node-ip-address` and as `--address=<head-address>:<gcs-port>` to workers. Both hostnames and IP addresses work.
- If a node's configured DNS nameserver is unreachable while `/etc/hosts` resolves the hostname, gRPC's default c-ares resolver can stall worker startup. Setting `GRPC_DNS_RESOLVER=native` in the group's container `env` removes that delay.
- Medios has no stable DNS names for tasks. `ray-medios` records where the head was placed and passes that address to workers.

## Resources

- `cpus`, `mem`, and `gpus` are read from the group's first container `resources.limits` (falling back to `resources.requests`), matching the stock autoscaler's precedence. Records setting group-level `resources` or sizing `rayStartParams` (`num-cpus`, `num-gpus`, `memory`) are refused so the framework and autoscaler never disagree on node size.
- Because a Medios task can observe the host's total memory rather than its task allocation, `ray-medios` always passes `--num-cpus`, `--num-gpus`, `--memory`, and `--object-store-memory` explicitly.
- The object store size is `30%` of the task's memory (`schedule.ObjectStoreFraction`), with Ray's minimum of `75 MiB` enforced. Under `docker_shell`, `LinuxInfo.ShmSize` is set to the same size in MiB with `ipc_mode: PRIVATE`.

## Identity

- `RAY_CLOUD_INSTANCE_ID` matches the pod record's `metadata.name` (chosen by `ray-medios` and matched by the autoscaler).
- `RAY_NODE_TYPE_NAME` is set to the group name (`headgroup` for the head, `workerGroupSpecs[].groupName` for workers).
- When `spec.enableInTreeAutoscaling` is `true`, the head runs with `RAY_enable_autoscaler_v2=1` and `--no-monitor`.
- Every `TaskInfo` carries `-principal` (`operator:<name>`, `team:<name>`, or `run:<id>`), validated before subscribing.

## Dashboard

The head runs Ray's dashboard with `--include-dashboard=true`, `--dashboard-host=0.0.0.0`, and an offered `--dashboard-port`. Jobs can be submitted to `/api/jobs/` on that port using `Authorization: Bearer <token>`.

## Auth token

The Ray auth token is read from `-auth-token-file` and passed as `RAY_AUTH_TOKEN` alongside `RAY_AUTH_MODE=token`. On Ray 2.60.0, clients (including `ray health-check` in worker startup loops) do not send the token unless `RAY_AUTH_MODE=token` is set.

See [SECURITY.md](../SECURITY.md) regarding plain environment variable transport.

## Container environment

Entries in a group's first container `env` are forwarded to that group's Medios tasks. Two kinds of entry are refused:

- Entries using `valueFrom` (there is no Kubernetes Secret/ConfigMap/Downward API to resolve).
- Entries colliding with environment variables managed by `ray-medios` (such as `RAY_AUTH_TOKEN`, `RAY_CLOUD_INSTANCE_ID`, or `KUBERNETES_SERVICE_HOST`).

> **Ray Client note (Ray 2.60.0 / grpcio 1.85.1):** Under `docker_shell`, Ray Client's forked proxy server can abort in gRPC (`ev_epoll1_linux.cc`) unless `GRPC_ENABLE_FORK_SUPPORT=0` is set in `headGroupSpec`'s container `env`.

## Stores

Each `-store name=path` flag mounts the named Medios store at `path` in every Ray node task, read-write by default (or read-only on the head when `-head-stores-read-only` is set). Offers missing any requested store are skipped. Ray Train requires a read-write mount on the head because its controller writes run state under the storage path.

## Where the driver runs

| Path | Driver process | Recommended use |
| --- | --- | --- |
| Jupyter in the head task | Kernel inside the head task connected to GCS | Notebooks, including Ray Data and Ray Train |
| Jobs REST API on the dashboard port | Process started by the head inside its task | Batch scripts and workloads that outlive the submitting machine |
| Ray Client (`ray://<head>:<client-port>`) | Process on a remote machine | Plain Ray tasks/actors from a local shell (not recommended for Ray Data or Ray Train) |

## Autoscaler

When `spec.enableInTreeAutoscaling` is `true`, the head task writes the base64-encoded CA cert, client cert, and client key into a `0700` `mktemp -d` directory, waits for `ray health-check`, starts `ray kuberay-autoscaler` in the background, and executes `ray start --block` in the foreground.

- `ray-medios` never generates TLS keys or certificates; the caller supplies PEM files via flags.
- `ray-medios` acts on autoscaler patches at the next Medios scheduler event (at most one controller heartbeat interval when offers are suppressed).
- If the autoscaler process exits inside the head task, it is not automatically restarted in `v0.1.0`.

## Offers

`ray-medios` fits as many pending Ray nodes as an offer can hold and submits them together in one `ACCEPT` call. Each task's CPUs, memory, GPUs, and ports are subtracted from the remaining offer before placing the next task, while shared store resources remain intact so multiple tasks on the same offer can mount the same store.

## Task lifecycle & shutdown

- **Worker startup:** Each worker task polls `ray health-check --address <head-gcs>` every 5 s before calling `ray start --block`, mirroring KubeRay's `wait-gcs-ready` init container.
- **Stopping a worker:** `ray-medios` kills the Medios task directly (never `ray stop`, which scans visible host PIDs under shared PID namespaces).
- **Unexpected task exit:** If a task ends without `ray-medios` having requested a kill, `ray-medios` stops and tears down the framework.
- **Lost scheduler stream:** If the Medios controller closes the event stream, `ray-medios` exits with status `1`. Reconnect and `RECONCILE` are planned for a future release.

## Known operational notes

- The container image (`-image`) must provide `ray`, `sh`, `base64`, and `mktemp` on `PATH`; `ray-medios` installs nothing at runtime.
- Restarting a cluster immediately (within ~1–3 s) after a `TEARDOWN` can race with the Medios node destroying the previous framework's tasks and releasing their ports. Wait briefly between teardown and relaunch if reusing the same Medios node.
- An offer whose CPUs or memory are split across multiple resource entries is not yet supported (`launch.TaskInfo` carves each scalar from a single resource entry).
