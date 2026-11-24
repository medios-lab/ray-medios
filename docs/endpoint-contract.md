# Endpoint Contract for the Stock KubeRay Autoscaler

`ray-medios` serves an HTTPS endpoint that the unmodified `ray kuberay-autoscaler` command treats as a Kubernetes API server. This page documents what that command asks of the endpoint and what the endpoint answers.

Source references below are paths under `python/ray/autoscaler/` in `ray-project/ray` (originally verified against Ray commit `61ecf914a71f3c9b547a1aaf52ad0389a1bb7a7e` and exercised end-to-end on Ray `2.60.0`).

Each item is tagged with how it was established:

- **observed**: verified against an HTTPS endpoint driven by `ray kuberay-autoscaler`.
- **source**: read from Ray's source code and not yet exercised directly.

## Process inputs

The autoscaler runs as:

```sh
ray kuberay-autoscaler --cluster-name <name> --cluster-namespace <ns> --gcs-address <ip:port>
```

This is the same entry point KubeRay's operator uses in its autoscaler container, with `--gcs-address` added because the head's GCS port is allocated from Medios offers rather than fixed at 6379.

| Input | Meaning | Evidence |
| --- | --- | --- |
| `KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT_HTTPS` | Endpoint address. The scheme is always `https`. | observed |
| `RAY_KUBERAY_CA_CERT_PATH` | CA bundle that verifies the endpoint's certificate. | observed |
| `RAY_KUBERAY_CLIENT_CERT_PATH`, `RAY_KUBERAY_CLIENT_KEY_PATH` | Client certificate. With both set, no service-account token file is read and no `Authorization` header is sent. | observed |
| `KUBERAY_CRD_VER` | API version in the RayCluster path; default `v1`. | source, `_private/kuberay/node_provider.py` |
| `RAY_enable_autoscaler_v2=1` on the head | Without it the command runs the legacy monitor, which this contract does not cover. | observed; source, `v2/utils.py:is_autoscaler_v2` |
| `--no-monitor` on `ray start --head` | Stops the head from starting its own monitor. | observed |
| `RAY_AUTH_TOKEN` / `RAY_AUTH_MODE=token` | Required for authenticated GCS/client access on Ray 2.60.0. | observed |

## Requests

Three request shapes are served. Three more exist in Ray's source behind optional cluster-wide idle termination, described under [Idle termination](#idle-termination).

| Request | When | Evidence |
| --- | --- | --- |
| `GET /apis/ray.io/v1/namespaces/<ns>/rayclusters/<name>` | Twice per update cycle: once by the config producer, once by the provider. | observed |
| `GET /api/v1/namespaces/<ns>/pods?labelSelector=ray.io/cluster%3D<name>` | Once per cycle. | observed |
| `PATCH /apis/ray.io/v1/namespaces/<ns>/rayclusters/<name>` with `Content-Type: application/json-patch+json` | Only when the autoscaler changes the worker count or the delete list. | observed |

Ray's default update cycle interval (`AUTOSCALER_UPDATE_INTERVAL_S`) is 5 s. No `DELETE`, `POST`, or watch request is sent during ordinary node scaling.

### Patch operations

All scaling patches are RFC 6902 JSON Patch arrays against the `RayCluster` record.

| Purpose | Operation | Evidence |
| --- | --- | --- |
| Scale a group up | `replace /spec/workerGroupSpecs/<i>/replicas` with the new total | observed |
| Remove named workers | `replace /spec/workerGroupSpecs/<i>/replicas` with the reduced total, and `replace /spec/workerGroupSpecs/<i>/scaleStrategy` with `{"workersToDelete": [<pod names>]}`, in one array | observed |
| Clear a finished delete list | `replace /spec/workerGroupSpecs/<i>/scaleStrategy` with `{"workersToDelete": []}` | source, `cloud_provider.py:435-442` |

`<i>` is the index of the group in `workerGroupSpecs`, so the endpoint keeps group order stable between a `GET` and the `PATCH` that follows it.

### Idle termination

The provider reads `spec.idleTerminationOptions` from the record (`cloud_provider.py:513-521`). With `timeoutSeconds` unset, the feature is off and none of the requests below is sent. With it set, and once no driver has been attached for that long, the provider acts on the whole cluster according to `policy`, which defaults to `Suspend` (source, `cloud_provider.py:700-870`):

- `Suspend`: `PATCH` with `Content-Type: application/merge-patch+json` and body `{"spec": {"idleSuspend": true}}`. The provider checks that the response body has `spec.idleSuspend` true.
- `Delete`: a JSON Patch of two operations (`test /metadata/resourceVersion` against the last-read value and `add` to `metadata.finalizers`), followed by `DELETE` on the `RayCluster` path.

Because the provider catches a refused suspend, finalizer, or delete request, logs it, and continues (source, `cloud_provider.py:788-876`), refusing those requests at the endpoint would leave the option silently inert. The store therefore refuses any record that carries `spec.idleTerminationOptions`.

## Responses

The client is `KubernetesHttpApiClient` (source, `_private/kuberay/node_provider.py:366-445`).

- Every successful request is answered with HTTP `200` and a JSON body. A `4xx` or `5xx` raises `requests.HTTPError`. Any other status (such as `204`) causes JSON parsing to fail.
- `GET` returns the object or list described below. `PATCH` returns the patched `RayCluster`.
- The client verifies the endpoint certificate against the host given in `KUBERNETES_SERVICE_HOST`, using the CA in `RAY_KUBERAY_CA_CERT_PATH`.
- Each request carries a timeout of `KUBERNETES_REQUEST_TIMEOUT_S`, defaulting to 60 s (`node_provider.py:46`).

## RayCluster record

Fields the autoscaler reads from the `GET` response. A missing required field raises `KeyError` in the autoscaler.

| Field | Required | Read by | Evidence |
| --- | --- | --- | --- |
| `metadata.namespace` | yes | config producer | observed |
| `metadata.name` | yes | config producer | source, `_private/kuberay/autoscaling_config.py:128` |
| `metadata.deletionTimestamp` | no | provider; when set, idle termination is skipped | source, `cloud_provider.py:761` |
| `spec.headGroupSpec` with `rayStartParams` and `template.spec.containers[0].resources` | yes | config producer | source, `autoscaling_config.py:183-420` |
| `spec.workerGroupSpecs[].groupName`, `minReplicas`, `maxReplicas`, `replicas` | yes | both | observed |
| `spec.workerGroupSpecs[].numOfHosts`, `resources`, `labels`, `template.spec.nodeSelector` | no | config producer | source, same file |
| `spec.workerGroupSpecs[].template.spec.containers[0].resources.limits/requests` (`cpu`, `memory`) | yes unless `rayStartParams` or `resources` supplies them | config producer | source, `autoscaling_config.py:384-420` |
| `spec.workerGroupSpecs[].scaleStrategy.workersToDelete` | no | provider | observed |
| `spec.autoscalerOptions` (idle timeout and related options) | no | config producer | source, same file |
| `metadata.finalizers`, `metadata.resourceVersion` | only for idle termination | provider | source, `cloud_provider.py:780-810` |

## Pod list

The response is a Kubernetes `PodList` object with `metadata.resourceVersion` and `items`. Per item the provider reads:

| Field | Meaning to the autoscaler | Evidence |
| --- | --- | --- |
| `metadata.name` | Cloud instance ID. Must equal `RAY_CLOUD_INSTANCE_ID` in that node's environment so the autoscaler can match the pod to the Ray node. | source, `cloud_provider.py:662`; observed |
| `metadata.labels["ray.io/node-type"]` | `head` or `worker`; any other value is ignored. | source, `cloud_provider.py:647-658` |
| `metadata.labels["ray.io/group"]` | The group name, which is the Ray node type. | source, same lines |
| `metadata.deletionTimestamp` | An item that carries it is skipped entirely. | source, `cloud_provider.py:624`; observed |
| `status.containerStatuses[0].state.running` | Present means running; anything else means pending. | source, `cloud_provider.py:671-688` |

An item that leaves the list is treated as terminated.

## Reconciliation behavior

The autoscaler only edits the `RayCluster` record; `ray-medios` reconciles the pod list and Medios tasks to match:

- When `replicas` rises, `ray-medios` launches Medios tasks for that group, inserts waiting pod records, and transitions each to running upon `TASK_RUNNING`.
- When `workersToDelete` names a worker, `ray-medios` kills the Medios task, sets `metadata.deletionTimestamp` on its pod record, and removes the pod record when the task reaches a terminal state. While any named worker still appears in the pod list without `deletionTimestamp`, the provider refuses new launches.
- A launch that would exceed `maxReplicas` is patched to the cap and the remainder is reported as launch failures.
- Replaying an identical launch request sends no second patch.

## Failure behavior of the autoscaler

- An HTTP `5xx` on the `RayCluster` `GET` is retried inside the config producer: `MAX_RAYCLUSTER_FETCH_TRIES = 5` attempts with `RAYCLUSTER_FETCH_RETRY_S = 5` between them (source, `autoscaling_config.py:33-34,76-86`).
- When retries are exhausted mid-loop, `Autoscaler.update_autoscaling_state` catches the exception, logs it, and retries on the next monitor cycle (source, `v2/autoscaler.py:226-231`, `v2/monitor.py:233-243`). An `AuthenticationError` ends the process.
- Exhausted retries, connection refusal, TLS failure, or timeouts during startup are not caught and exit the autoscaler process (source, `v2/instance_manager/config.py:497-498`). `ray-medios` therefore starts listening on the endpoint before launching the head task.

## Implementation & Open Items

The Go implementation in `internal/endpoint` is verified by contract tests in `internal/endpoint/endpoint_test.go` and end-to-end scale-up/scale-down runs against `ray kuberay-autoscaler`.

Open items:

- **Idle cluster termination:** Outside `v0.1.0`; records that set `spec.idleTerminationOptions` are refused.
- **`scaleStrategy` initialization:** Because the stock provider issues RFC 6902 `replace` on `scaleStrategy` and sample `RayCluster` manifests omit `scaleStrategy`, `internal/store` initializes an empty `scaleStrategy` object on each worker group when storing a record.
- **`RayCluster.status` fields:** None were read in the autoscaler source paths listed above.
