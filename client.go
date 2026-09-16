// Package cf_grpc provides Caerus Framework gRPC transport components.
//
// The framework owns dial / listen / reconnect lifecycle. Product apps own
// generated protobuf stubs and service implementations. See README.
package cf_grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// ClientComponentName is the default framework name for a gRPC client.
	ClientComponentName = "grpc-client"
	// ClientComponentStage is the data plane (outbound store-like peer).
	ClientComponentStage = cf.Stage("data")
)

// ClientConfig is file/env settings for one gRPC client instance.
// No field flag tags: multiple clients share a process flag namespace; use
// per-source EnvPrefix and the --<source-name> path override instead.
type ClientConfig struct {
	Target              string   `json:"target,omitempty" yaml:"target,omitempty" env:"TARGET"`
	Insecure            *bool    `json:"insecure,omitempty" yaml:"insecure,omitempty" env:"INSECURE"`
	ConnectTimeoutSec   *float64 `json:"connect_timeout_sec,omitempty" yaml:"connect_timeout_sec,omitempty" env:"CONNECT_TIMEOUT_SEC"`
	KeepaliveTimeSec    *float64 `json:"keepalive_time_sec,omitempty" yaml:"keepalive_time_sec,omitempty" env:"KEEPALIVE_TIME_SEC"`
	KeepaliveTimeoutSec *float64 `json:"keepalive_timeout_sec,omitempty" yaml:"keepalive_timeout_sec,omitempty" env:"KEEPALIVE_TIMEOUT_SEC"`
	DegradedMode        *bool    `json:"degraded_mode,omitempty" yaml:"degraded_mode,omitempty" env:"DEGRADED_MODE"`
	HealthWhenDegraded  string   `json:"health_when_degraded,omitempty" yaml:"health_when_degraded,omitempty" env:"HEALTH_WHEN_DEGRADED"`
}

// ClientOption configures a Client at construction time.
type ClientOption func(*clientOptions)

type clientOptions struct {
	loaded             *ClientConfig
	configSource       string
	configPath         string
	srcEnvPrefix       string
	srcFormat          cf_configuration.Format
	srcFormatSet       bool
	target             string
	insecure           bool
	connectTimeout     time.Duration
	keepaliveTime      time.Duration
	keepaliveTimeout   time.Duration
	degradedMode       bool
	healthWhenDegraded string
	logger             *slog.Logger
	loggerSet          bool
	name               string
}

// WithClientConfig applies a static configuration snapshot.
func WithClientConfig(cfg ClientConfig) ClientOption {
	return func(o *clientOptions) { o.loaded = &cfg }
}

// WithClientConfigSource binds the client to a self-registered configuration source.
func WithClientConfigSource(name, path string, opts ...SourceOption) ClientOption {
	return func(o *clientOptions) {
		so := sourceOptions{envPrefix: defaultSourceEnvPrefix(name)}
		for _, opt := range opts {
			opt(&so)
		}
		o.configSource = name
		o.configPath = path
		o.srcEnvPrefix = so.envPrefix
		o.srcFormat = so.format
		o.srcFormatSet = so.formatSet
	}
}

// WithTarget sets the dial target (host:port or dns:///name:port).
func WithTarget(target string) ClientOption {
	return func(o *clientOptions) { o.target = target }
}

// WithInsecure uses insecure transport credentials (default true for local v1).
func WithInsecure(enabled bool) ClientOption {
	return func(o *clientOptions) { o.insecure = enabled }
}

// WithConnectTimeout sets how long Init waits for connectivity.Ready.
func WithConnectTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.connectTimeout = d }
}

// WithClientName sets a custom component name for multiple clients in one process.
func WithClientName(name string) ClientOption {
	return func(o *clientOptions) { o.name = name }
}

// WithClientLogger sets an explicit logger; skips the framework logs subscription.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(o *clientOptions) {
		o.logger = logger
		o.loggerSet = true
	}
}

// WithClientDegradedMode allows Init to succeed when the dial/ready wait fails.
func WithClientDegradedMode(enabled bool) ClientOption {
	return func(o *clientOptions) { o.degradedMode = enabled }
}

// Client is one outbound gRPC connection peer.
type Client struct {
	mu sync.Mutex

	configSource string
	configPath   string
	srcEnvPrefix string
	srcFormat    cf_configuration.Format
	srcFormatSet bool

	target             string
	insecure           bool
	connectTimeout     time.Duration
	keepaliveTime      time.Duration
	keepaliveTimeout   time.Duration
	degradedMode       bool
	healthWhenDegraded string

	name      string
	logger    *slog.Logger
	loggerSet bool
	logsSub   *cf_logs.Subscription
	fw        *cf.CaerusFramework

	conn   *grpc.ClientConn
	facade *liveConn

	initialized         bool
	liveConnected       atomic.Bool
	degradedUnreachable atomic.Bool
	degradedModeUses    atomic.Uint64
	reconnects          atomic.Uint64
	connectFailures     atomic.Uint64
}

// NewClient creates an inert gRPC client component. Dial happens at Init.
func NewClient(opts ...ClientOption) *Client {
	o := clientOptions{
		insecure:           true,
		connectTimeout:     5 * time.Second,
		keepaliveTime:      10 * time.Second,
		keepaliveTimeout:   time.Second,
		healthWhenDegraded: "not_ready",
		logger:             slog.Default(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	c := &Client{
		configSource:       o.configSource,
		configPath:         o.configPath,
		srcEnvPrefix:       o.srcEnvPrefix,
		srcFormat:          o.srcFormat,
		srcFormatSet:       o.srcFormatSet,
		target:             o.target,
		insecure:           o.insecure,
		connectTimeout:     o.connectTimeout,
		keepaliveTime:      o.keepaliveTime,
		keepaliveTimeout:   o.keepaliveTimeout,
		degradedMode:       o.degradedMode,
		healthWhenDegraded: normalizeHealthWhenDegraded(o.healthWhenDegraded),
		name:               o.name,
		logger:             o.logger,
		loggerSet:          o.loggerSet,
	}
	c.facade = &liveConn{c: c}
	if o.loaded != nil {
		c.applyClientConfig(*o.loaded)
	}
	return c
}

// Name implements cf.CaerusComponent.
func (c *Client) Name() string {
	if c.name != "" {
		return c.name
	}
	return ClientComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (c *Client) GetInitOrderStage() cf.Stage { return ClientComponentStage }

// GetDependencies implements cf.Dependencies.
func (c *Client) GetDependencies() []string {
	deps := []string{cf_logs.ComponentName}
	if c.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar.
func (c *Client) RegisterConfigSources(conf any) error {
	configuration, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_grpc: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if c.configSource == "" {
		return nil
	}
	return cf_configuration.AddSource(configuration, cf_configuration.Source[ClientConfig]{
		Name:      c.configSource,
		Path:      c.configPath,
		Format:    resolveSourceFormat(c.configPath, c.srcFormat, c.srcFormatSet),
		Owner:     c.Name(),
		EnvPrefix: c.srcEnvPrefix,
		Validate:  validateClientConfig,
	})
}

// Init dials the target and waits for Ready (or continues under DegradedMode).
func (c *Client) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.fw = fw
	if !c.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			c.logsSub = logs.OnReconfigureFor(c.Name(), func(l *slog.Logger) { c.logger = l })
		}
	}
	if c.configSource != "" {
		if err := c.applyClientConfigFromSource(); err != nil {
			c.unsubscribeLogs()
			return err
		}
	}
	if strings.TrimSpace(c.target) == "" {
		c.unsubscribeLogs()
		return errors.New("cf_grpc: client target is required")
	}

	conn, err := c.dialLocked()
	if err != nil {
		if !c.degradedMode {
			c.unsubscribeLogs()
			return err
		}
		c.connectFailures.Add(1)
		c.degradedModeUses.Add(1)
		c.degradedUnreachable.Store(true)
		c.liveConnected.Store(false)
		c.initialized = true
		c.logger.Error("cf_grpc: DegradedMode — dial failed; Init continues with nil conn",
			"err", err,
			"target", c.target,
			"health_when_degraded", c.healthWhenDegraded,
		)
		return nil
	}
	if err := c.waitReadyLocked(ctx, conn); err != nil {
		_ = conn.Close()
		if !c.degradedMode {
			c.unsubscribeLogs()
			return err
		}
		c.connectFailures.Add(1)
		c.degradedModeUses.Add(1)
		c.degradedUnreachable.Store(true)
		c.liveConnected.Store(false)
		c.initialized = true
		c.logger.Error("cf_grpc: DegradedMode — connect timeout; Init continues with nil conn",
			"err", err,
			"target", c.target,
			"health_when_degraded", c.healthWhenDegraded,
		)
		return nil
	}
	c.conn = conn
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	c.initialized = true
	c.logger.Info("cf_grpc: client initialized", "target", c.target)
	return nil
}

// Conn returns a stable grpc.ClientConnInterface that always forwards to the
// current underlying connection. Build typed stubs from this once at Init.
func (c *Client) Conn() grpc.ClientConnInterface {
	return c.facade
}

// UnderlyingConn returns the current *grpc.ClientConn, or nil.
// Prefer Conn() for stubs so reload stays live.
func (c *Client) UnderlyingConn() *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// OnConfigReload implements cf.ConfigReloader. Failed redial keeps last-good.
func (c *Client) OnConfigReload(source string, cfg any) {
	if source != c.configSource {
		return
	}
	loaded, ok := cfg.(*ClientConfig)
	if !ok {
		c.logger.Error("cf_grpc: client config reload rejected", "source", source, "type", fmt.Sprintf("%T", cfg))
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initialized {
		return
	}
	prevTarget := c.target
	c.applyClientConfig(*loaded)
	if strings.TrimSpace(c.target) == "" {
		c.logger.Error("cf_grpc: client reload ignored; empty target", "source", source)
		c.target = prevTarget
		return
	}
	conn, err := c.dialLocked()
	if err != nil {
		c.connectFailures.Add(1)
		c.logger.Error("cf_grpc: client reload dial failed; keeping last-good", "err", err, "target", c.target)
		c.target = prevTarget
		return
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), c.connectTimeout)
	defer cancel()
	if err := c.waitReadyLocked(waitCtx, conn); err != nil {
		_ = conn.Close()
		c.connectFailures.Add(1)
		c.logger.Error("cf_grpc: client reload ready wait failed; keeping last-good", "err", err, "target", c.target)
		c.target = prevTarget
		return
	}
	old := c.conn
	c.conn = conn
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	c.reconnects.Add(1)
	if old != nil {
		_ = old.Close()
	}
	c.logger.Info("cf_grpc: client reloaded", "source", source, "target", c.target)
}

// Health implements cf.HealthProvider.
func (c *Client) Health(ctx context.Context) error {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initialized {
		return errors.New("cf_grpc: client not initialized")
	}
	if c.conn != nil && c.conn.GetState() == connectivity.Ready {
		return nil
	}
	if c.degradedMode && c.healthWhenDegraded == "ready" {
		return nil
	}
	if c.conn == nil {
		return errors.New("cf_grpc: client not connected")
	}
	return fmt.Errorf("cf_grpc: client connectivity %s", c.conn.GetState())
}

// Metrics implements cf_observability.MetricsProvider.
func (c *Client) Metrics() []cf_observability.Metric {
	if !c.initialized {
		return nil
	}
	connected := 0.0
	if c.liveConnected.Load() {
		connected = 1
	}
	degraded := 0.0
	if c.degradedUnreachable.Load() {
		degraded = 1
	}
	return []cf_observability.Metric{
		{Name: "grpc_client_connected", Help: "1 when the client has a Ready connection.", Value: connected, Labels: map[string]string{"component": c.Name()}},
		{Name: "grpc_client_degraded_unreachable", Help: "1 when running without a Ready connection under DegradedMode.", Value: degraded, Labels: map[string]string{"component": c.Name()}},
		{Name: "grpc_client_degraded_mode_uses_total", Help: "Times Init continued after a failed connect because DegradedMode was enabled.", Value: float64(c.degradedModeUses.Load()), Labels: map[string]string{"component": c.Name()}},
		{Name: "grpc_client_reconnects_total", Help: "Successful client reconnects after config reload.", Value: float64(c.reconnects.Load()), Labels: map[string]string{"component": c.Name()}},
		{Name: "grpc_client_connect_failures_total", Help: "Failed dial or ready-wait attempts.", Value: float64(c.connectFailures.Load()), Labels: map[string]string{"component": c.Name()}},
	}
}

// Shutdown closes the underlying connection and unsubscribes from logs.
func (c *Client) Shutdown(ctx context.Context) error {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.liveConnected.Store(false)
	c.initialized = false
	c.unsubscribeLogs()
	return nil
}

func (c *Client) unsubscribeLogs() {
	if c.logsSub != nil {
		c.logsSub.Unsubscribe()
		c.logsSub = nil
	}
}

func (c *Client) applyClientConfigFromSource() error {
	configuration, ok := cf.Get[*cf_configuration.Configuration](c.fw)
	if !ok {
		return errors.New("cf_grpc: configuration component not registered")
	}
	cfg, ok := cf_configuration.Get[ClientConfig](configuration, c.configSource)
	if !ok {
		return fmt.Errorf("cf_grpc: configuration source %q not found", c.configSource)
	}
	c.applyClientConfig(cfg)
	return nil
}

func (c *Client) applyClientConfig(cfg ClientConfig) {
	if cfg.Target != "" {
		c.target = cfg.Target
	}
	if cfg.Insecure != nil {
		c.insecure = *cfg.Insecure
	}
	if cfg.ConnectTimeoutSec != nil {
		c.connectTimeout = time.Duration(*cfg.ConnectTimeoutSec * float64(time.Second))
	}
	if cfg.KeepaliveTimeSec != nil {
		c.keepaliveTime = time.Duration(*cfg.KeepaliveTimeSec * float64(time.Second))
	}
	if cfg.KeepaliveTimeoutSec != nil {
		c.keepaliveTimeout = time.Duration(*cfg.KeepaliveTimeoutSec * float64(time.Second))
	}
	if cfg.DegradedMode != nil {
		c.degradedMode = *cfg.DegradedMode
	}
	if cfg.HealthWhenDegraded != "" {
		c.healthWhenDegraded = normalizeHealthWhenDegraded(cfg.HealthWhenDegraded)
	}
}

func (c *Client) dialLocked() (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.keepaliveTime,
			Timeout:             c.keepaliveTimeout,
			PermitWithoutStream: true,
		}),
	}
	if c.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, errors.New("cf_grpc: secure dial not implemented yet; set insecure: true or WithInsecure(true)")
	}
	conn, err := grpc.NewClient(c.target, opts...)
	if err != nil {
		return nil, fmt.Errorf("cf_grpc: dial %s: %w", c.target, err)
	}
	return conn, nil
}

func (c *Client) waitReadyLocked(ctx context.Context, conn *grpc.ClientConn) error {
	timeout := c.connectTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return errors.New("cf_grpc: connection shut down while waiting for Ready")
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			return fmt.Errorf("cf_grpc: timed out waiting for Ready on %s (last state %s)", c.target, state)
		}
	}
}

func validateClientConfig(cfg *ClientConfig) error {
	for name, value := range map[string]*float64{
		"connect_timeout_sec":   cfg.ConnectTimeoutSec,
		"keepalive_time_sec":    cfg.KeepaliveTimeSec,
		"keepalive_timeout_sec": cfg.KeepaliveTimeoutSec,
	} {
		if value != nil {
			if err := validateSeconds(name, *value, true); err != nil {
				return err
			}
		}
	}
	switch strings.ToLower(strings.TrimSpace(cfg.HealthWhenDegraded)) {
	case "", "ready", "not_ready":
	default:
		return fmt.Errorf("cf_grpc: health_when_degraded must be ready or not_ready, got %q", cfg.HealthWhenDegraded)
	}
	return nil
}

func normalizeHealthWhenDegraded(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "ready":
		return "ready"
	default:
		return "not_ready"
	}
}

func validateSeconds(name string, value float64, positive bool) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || (positive && value == 0) {
		if positive {
			return fmt.Errorf("cf_grpc: %s must be positive and finite", name)
		}
		return fmt.Errorf("cf_grpc: %s must be non-negative and finite", name)
	}
	return nil
}

// liveConn forwards RPCs to the Client's current underlying connection.
type liveConn struct {
	c *Client
}

func (l *liveConn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	conn := l.c.UnderlyingConn()
	if conn == nil {
		return errors.New("cf_grpc: client not connected")
	}
	return conn.Invoke(ctx, method, args, reply, opts...)
}

func (l *liveConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn := l.c.UnderlyingConn()
	if conn == nil {
		return nil, errors.New("cf_grpc: client not connected")
	}
	return conn.NewStream(ctx, desc, method, opts...)
}

var _ cf.CaerusComponent = (*Client)(nil)
var _ cf.Dependencies = (*Client)(nil)
var _ cf.HealthProvider = (*Client)(nil)
var _ cf_observability.MetricsProvider = (*Client)(nil)
var _ cf.ConfigReloader = (*Client)(nil)
var _ cf.ConfigSourceRegistrar = (*Client)(nil)
var _ grpc.ClientConnInterface = (*liveConn)(nil)
