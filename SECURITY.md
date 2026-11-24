# Security Policy

## Reporting a Vulnerability

Please report suspected security vulnerabilities privately using GitHub's **Private Vulnerability Reporting** feature on this repository:

1. Navigate to the **Security** tab of `medios-lab/ray-medios`.
2. Select **Report a vulnerability** (or **Advisories** -> **New draft security advisory**).
3. Include a clear description of the issue, reproduction steps, affected versions, and potential impact.

Please do **not** open public GitHub issues for security vulnerabilities.

## Supported Versions

| Version | Supported |
| :--- | :--- |
| `0.1.x` | Preview / best-effort fixes on `main` |

## Security Model & Disclosures (`v0.1.0`)

Operators deploying `ray-medios` `v0.1.0` should account for the following design constraints:

1. **Plain environment variable transport for credentials:**
   - Neither the `medios` nor the `docker_shell` containerizer currently delivers Medios `SECRET` environment variables to tasks.
   - As a result, `RAY_AUTH_TOKEN` and the autoscaler's mutual-TLS credentials (`RAYMEDIOS_CA_CRT_BASE64`, `RAYMEDIOS_CLIENT_CRT_BASE64`, `RAYMEDIOS_CLIENT_KEY_BASE64`) are placed in task `Environment.Variables` as plain values.
   - These values are readable by anyone with access to the Medios controller's task state or host process inspection. Treat controller access accordingly and use short-lived, per-cluster credentials.
2. **Endpoint client-certificate scope:**
   - The HTTPS autoscaler endpoint enforces mutual TLS (`tls.RequireAndVerifyClientCert`, TLS 1.2+ minimum).
   - Any client certificate signed by `-tls-client-ca` is authorized to read and patch any cluster held in that `ray-medios` instance's store. Use a dedicated client CA or certificate per `ray-medios` instance rather than sharing a broad CA across untrusted tenants.
3. **Host network namespace & shared port space:**
   - Under both `medios` and `docker_shell` (`docker.network: HOST`), Ray processes bind ports in the Medios node's network namespace. Protect Ray dashboard (`--dashboard-host=0.0.0.0`), GCS, and client ports with network policy/firewalling in addition to `RAY_AUTH_TOKEN`.
