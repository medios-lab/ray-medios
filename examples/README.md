# Examples

Starter `RayCluster` JSON records for `ray-medios`:

- `raycluster-fixed.json` — Fixed-size cluster (1 head + 1 worker) without the autoscaler endpoint.
- `raycluster-autoscaling.json` — Autoscaling cluster starting at `0` workers (`minReplicas: 0`, `maxReplicas: 4`, `idleTimeoutSeconds: 60`).

## Generating Mutual-TLS Certificates for Autoscaling

`ray-medios` does not generate certificates. For local or single-cluster testing, you can create a dedicated CA, server certificate, and client certificate with `openssl`. Replace `<ENDPOINT_HOST>` with the IP address or DNS name you pass to `-endpoint-host` (use `IP:<addr>` for an IP address or `DNS:<hostname>` for a hostname in `subjectAltName`).

```sh
mkdir -p certs && cd certs

# 1. Dedicated CA for this cluster's autoscaler endpoint
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout ca.key -out ca.crt -days 30 -nodes -subj "/CN=ray-medios-ca"

# 2. Endpoint server certificate (SAN must match -endpoint-host)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout server.key -out server.csr -nodes -subj "/CN=ray-medios-endpoint"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 30 \
  -extfile <(printf "subjectAltName=IP:<ENDPOINT_HOST>\nextendedKeyUsage=serverAuth\n")

# 3. Autoscaler client certificate
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout client.key -out client.csr -nodes -subj "/CN=ray-autoscaler"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 30 \
  -extfile <(printf "extendedKeyUsage=clientAuth\n")

chmod 600 *.key
```

Pass `server.crt`, `server.key`, and `ca.crt` to `-tls-server-cert`, `-tls-server-key`, and `-tls-client-ca`, and pass `ca.crt`, `client.crt`, and `client.key` to `-autoscaler-ca`, `-autoscaler-cert`, and `-autoscaler-key`.
