# caerus-framework-grpc

[![CI](https://github.com/caerus-framework/caerus-framework-grpc/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-grpc/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-grpc/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-grpc)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Caerus Framework gRPC transport component.

This module owns **dial, listen, reconnect, and lifecycle** for gRPC. It does
**not** know your generated service types. Product apps import their own
protobuf stubs (from a separate APIs / genproto module) and:

- **Server:** `Register` services on the framework `*grpc.Server` (like
  `cf_http.SetHandler`), then the component `Run` listens.
- **Client:** keep the `*cf_grpc.Client` peer and build stubs from
  `Conn()` (a live facade that survives config reload).

Several servers and several clients in one process use `WithServerName` /
`WithClientName` the same way multiple `cf_postgres` or `cf_valkey` peers use
`WithName`.

## Wiring

Two wiring shapes. Prefer the **app-owned** golden path.

### App-owned consumer (golden)

`main` declares chassis (including gRPC server and/or client instances) and
the app class. The app resolves peers at `Init`, lists them in
`GetDependencies`, registers services / builds stubs.

```go
fw := cf.New(&cf.FrameworkOptions{
	Logs: &cf.LogsSettings{Format: "json", Level: "info", ConfigSource: "logs"},
	Observability: &cf.ObservabilitySettings{Bind: ":9090", ConfigSource: "observability"},
	Components: []cf.CaerusComponent{
		cf_postgres.New(cf_postgres.WithConfigSource("postgresql", "config/postgresql.json")),
		cf_grpc.NewServer(
			cf_grpc.WithServerName("grpc-public"),
			cf_grpc.WithServerConfigSource("grpc-public", "config/grpc-public.json"),
		),
		cf_grpc.NewClient(
			cf_grpc.WithClientName("auth"),
			cf_grpc.WithClientConfigSource("grpc-auth", "config/grpc-auth.json"),
		),
		app.New(app.Options{}),
	},
})
if err := fw.RunWithSignals(context.Background()); err != nil {
	log.Fatal(err)
}
```

```go
type App struct {
	grpcSrv  *cf_grpc.Server
	authPeer *cf_grpc.Client
	auth     authpb.AuthServiceClient // generated stub
}

func (a *App) GetDependencies() []string {
	return []string{"grpc-public", "auth", cf_logs.ComponentName}
}

func (a *App) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	srv, ok := cf.GetByName[*cf_grpc.Server](fw, "grpc-public")
	if !ok {
		return errors.New("app: grpc-public missing")
	}
	cli, ok := cf.GetByName[*cf_grpc.Client](fw, "auth")
	if !ok {
		return errors.New("app: auth grpc client missing")
	}
	a.grpcSrv = srv
	a.authPeer = cli
	a.auth = authpb.NewAuthServiceClient(cli.Conn()) // or cf_grpc.Stub(cli, authpb.NewAuthServiceClient)

	srv.Register(func(g *grpc.Server) {
		authpb.RegisterAuthServiceServer(g, a) // a implements the service
	})
	return nil
}
```

**Two different names:** component `Name()` (`"auth"`, `"grpc-public"`) is
what `GetDependencies` / `GetByName` use. The configuration **source** name
(`"grpc-auth"`, `"grpc-public"`) is what appears in files, env prefixes, and
`--grpc-auth` path overrides. Prefer matching them when you can.

### Simple `main`-level wiring

```go
logs := cf_logs.New(cf_logs.WithWriter(os.Stdout))
srv := cf_grpc.NewServer(cf_grpc.WithBind(":8100"))
cli := cf_grpc.NewClient(cf_grpc.WithTarget("127.0.0.1:8100"))
fw := cf.New(&cf.FrameworkOptions{Components: []cf.CaerusComponent{logs, srv, cli}})
```

## Lifecycle (listen only in Run)

```mermaid
sequenceDiagram
  participant App
  participant Server as cf_grpc.Server
  participant Client as cf_grpc.Client
  App->>Server: Init (build *grpc.Server, no listen)
  App->>Client: Init (dial / wait Ready)
  App->>Server: Register(pb.Register…)
  App->>Server: Run (Listen + Serve)
  Note over Server: Jobs never call Run — no port claim
```

Wrong: `Init` calls `net.Listen`. Right: `Init` prepares; `Run` binds.

## Configuration

One **source per instance** (not one file with maps of all peers).

Env overlay uses the source’s prefix (default from source name:
`grpc-auth` → `GRPC_AUTH_`). File path override: `--grpc-auth`.

### Server settings (`ServerConfig`)

| Setting | Kind | Default | Meaning |
| --- | --- | --- | --- |
| `bind` | setting | `:8100` | Listen address (`host:port`). Claimed only in `Run`, never in `Init`. Not `:9090` (observability) and not `:8080` (HTTP). Extra servers use `:8101`, then `:8102`. |
| `shutdown_timeout_sec` | tunable | `10` | How long `GracefulStop` may take before hard `Stop`. |
| `keepalive_time_sec` | tunable | `7200` (2h) | Server keepalive ping period (gRPC `ServerParameters.Time`). |
| `keepalive_timeout_sec` | tunable | `20` | Wait for keepalive ping ack before closing the connection. |
| `max_connection_idle_sec` | tunable | `900` (15m) | Close connections idle longer than this (gRPC `MaxConnectionIdle`). |
| `restart_policy` | setting | `handled` | What happens if `bind` or TLS paths change on reload: `handled` (log “restart required”, keep current listener) or `immediate` (`Run` returns so the process can exit and rebind). **No live rebind.** |
| `tls_cert_file` | setting | _(empty)_ | Server certificate PEM path. Set with `tls_key_file` to enable TLS. |
| `tls_key_file` | setting | _(empty)_ | Server private key PEM path (pair with `tls_cert_file`). |
| `tls_ca_file` | setting | _(empty)_ | CA PEM to verify **client** certificates (mTLS). When set without `tls_client_auth`, defaults to `require_and_verify`. |
| `tls_client_auth` | setting | `none` | `none` (no client cert) or `require_and_verify` (mTLS; needs `tls_ca_file`). |

Example `config/grpc-public.json` (plaintext listen; add TLS files for Path B):

```json
{
  "bind": ":8100",
  "shutdown_timeout_sec": 10,
  "restart_policy": "handled"
}
```

`:8100` is this module’s copy-paste default. Observability stays `:9090`.
HTTP stays `:8080`. Do not reuse those numbers for gRPC in the same process.

### Client settings (`ClientConfig`)

| Setting | Kind | Default | Meaning |
| --- | --- | --- | --- |
| `target` | setting | _(required)_ | Dial target (`host:port` or `dns:///name:port`). |
| `insecure` | switch | `true` | Plaintext (h2c) credentials. Construct default is **laptop / `go run`**. Also Path A (mesh): sidecar encrypts. Path B (app TLS): `false` plus PEM / system roots. Cannot combine with TLS files. |
| `connect_timeout_sec` | tunable | `5` | How long `Init` waits for connectivity `Ready`. |
| `keepalive_time_sec` | tunable | `10` | Client keepalive ping period when idle. |
| `keepalive_timeout_sec` | tunable | `1` | Wait for keepalive ping ack. |
| `degraded_mode` | switch | `false` | When true, failed dial / Ready wait does **not** abort `Init` (hard-fail when off). Same idea as `cf_valkey`. |
| `health_when_degraded` | setting | `not_ready` | While disconnected under DegradedMode: `not_ready` (`Health` fails → `/readyz` 503) or `ready` (break-glass LB traffic). |
| `tls_ca_file` | setting | _(empty)_ | CA PEM to verify the **server** certificate. Empty + `insecure: false` uses system roots. |
| `tls_cert_file` / `tls_key_file` | setting | _(empty)_ | Client certificate pair for mTLS (must be set together). |
| `tls_server_name` | setting | _(empty)_ | TLS ServerName (SNI / hostname check). Often needed when dialing by IP. |
| `tls_insecure_skip_verify` | switch | `false` | Skip server cert verify. Lab only. Error log + `grpc_client_tls_insecure_skip_verify=1`. Never production Path B. |

TLS **is implemented** (`tls.go`). Setting TLS PEM paths with `insecure`
omitted flips the client to a secure dial. Explicit `insecure: true` plus
TLS files is rejected.

### Laptop / `go run` (plaintext)

Loopback to a process on the same machine. `insecure: true` stays available
on purpose. This is **not** a cluster path.

```json
{
  "target": "127.0.0.1:8100",
  "insecure": true,
  "connect_timeout_sec": 5,
  "degraded_mode": false,
  "health_when_degraded": "not_ready"
}
```

### Path A — mesh TLS (recommended when Istio/Linkerd is on)

The client still uses `insecure: true` (h2c to the sidecar). The **mesh**
encrypts on the wire. Name it **mesh** so a junior does not copy this JSON
onto a cluster that has no sidecar and think they skipped TLS on purpose.

Kubernetes Service in the **same namespace** (short name → ClusterIP):

```json
{
  "target": "peer-api:8100",
  "insecure": true,
  "connect_timeout_sec": 5,
  "degraded_mode": false,
  "health_when_degraded": "not_ready"
}
```

Same Service from another namespace uses the FQDN, e.g.
`peer-api.tenant-a.svc.cluster.local:8100`.

Wrong: one in-cluster snippet with `insecure: true` that looks like Path B
(app TLS) but is actually Path A (or worse: plaintext with no mesh).

Right: Path A says “mesh encrypts”; Path B says `insecure: false` and PEM.

### Path B — app TLS

No mesh (or you want encryption inside the mesh too). Client sets
`insecure: false` and usually `tls_ca_file`. Set `tls_server_name` when
dialing by IP so the hostname check matches the certificate.

```json
{
  "target": "peer-api:8100",
  "insecure": false,
  "tls_ca_file": "/var/run/secrets/caerus/grpc-ca.pem",
  "tls_server_name": "peer-api.tenant-a.svc.cluster.local",
  "connect_timeout_sec": 5,
  "degraded_mode": false,
  "health_when_degraded": "not_ready"
}
```

Matching **server** Path B (PEM pair; optional mTLS):

```json
{
  "bind": ":8100",
  "tls_cert_file": "/var/run/secrets/caerus/grpc-server.pem",
  "tls_key_file": "/var/run/secrets/caerus/grpc-server-key.pem",
  "tls_ca_file": "/var/run/secrets/caerus/grpc-client-ca.pem",
  "tls_client_auth": "require_and_verify",
  "shutdown_timeout_sec": 10,
  "restart_policy": "handled"
}
```

Omit `tls_ca_file` / `tls_client_auth` when you want server TLS without
client certificates. `tls_client_auth=require_and_verify` needs the CA.

### `tls_insecure_skip_verify` (lab only)

This is a different switch from `insecure`. `insecure: true` is plaintext.
Skip-verify is “HTTPS/gRPC-TLS, but do not check the certificate” — MITM
can present any cert. Error log plus `grpc_client_tls_insecure_skip_verify`
on `/metrics`. Dashboards should alert when that gauge is `1`.

Wrong: Path B plus `tls_insecure_skip_verify: true` on a serve pod.

Right: Path B verifies the server cert (PEM CA or system roots). Skip-verify
is laptop MITM debugging only.

Client **DegradedMode** matches
[`caerus-framework-valkey`](https://github.com/caerus-framework/caerus-framework-valkey)
(`cf_valkey`): Init may succeed without Ready; `/readyz` stays red unless
`health_when_degraded: ready`.

## Options (construct-time)

| Option | Applies to | Meaning |
| --- | --- | --- |
| `WithServerConfig` / `WithClientConfig` | both | static snapshot |
| `WithServerConfigSource` / `WithClientConfigSource` | both | self-registered source |
| `WithBind` / `WithTarget` | server / client | listen or dial address |
| `WithServerName` / `WithClientName` | both | component `Name()` |
| `WithServerLogger` / `WithClientLogger` | both | explicit logger |
| `WithClientDegradedMode` | client | soft Init on dial failure |
| `WithClientTLS` | client | PEM CA / optional client cert+key; sets secure dial |
| `WithTLSServerName` | client | SNI / cert hostname |
| `WithTLSInsecureSkipVerify` | client | lab-only skip verify (error log + gauge) |
| `WithServerTLS` | server | PEM cert+key |
| `WithServerTLSClientCA` | server | mTLS client CA (+ require_and_verify) |
| `WithConnectTimeout` | client | Ready wait |
| `WithShutdownTimeout` | server | GracefulStop deadline |

## Proto / generated stubs

Not in this module. Apps depend on a published APIs / genproto Go module for
`.proto`-generated clients and servers. Wire those stubs in app `Init`
(`Register` on the server; `NewXxxClient(cli.Conn())` on the client).
