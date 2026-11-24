# Changelog

All notable changes to `ray-medios` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-24

Initial preview open-source release under `medios-lab`.

### Added
- `ray-medios` Go framework binary (`cmd/ray-medios`) registering with Medios as framework `ray`, with `-version` build metadata support.
- Mutual-TLS HTTPS endpoint (`internal/endpoint`) implementing the KubeRay API subset required by Ray's unmodified `ray kuberay-autoscaler` (`GET`/`PATCH` on `RayCluster`, `GET` on `PodList`).
- Scheduler reconciliation (`internal/schedule`, `internal/framework`) placing Ray head and worker nodes onto Medios offers, launching multiple Ray nodes per offer in a single `ACCEPT` when capacity allows, suppressing/reviving offers, and killing workers listed in `scaleStrategy.workersToDelete`.
- Task planning and Medios `TaskInfo` translation (`internal/tasks`, `internal/launch`) supporting both `medios` and `docker_shell` (`docker.network: HOST`, `IPCMode: PRIVATE`, `ShmSizeMB`) containerizers, explicit Ray port and resource flags, `RAY_AUTH_MODE=token`, container environment passthrough, and Medios store mounts (including `-head-stores-read-only` and shared store resource preservation across multi-task offers).
- Starter `RayCluster` examples (`examples/`) and an end-to-end Ray Train + Ray Data XGBoost Cover Type notebook and container `Dockerfile` (`notebooks/xgboost-covtype/`).
- Technical contracts and mapping documentation (`docs/endpoint-contract.md`, `docs/task-mapping.md`).
