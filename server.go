package cf_grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// ServerComponentName is the default framework name for a gRPC server.
	ServerComponentName = "grpc-server"
	// ServerComponentStage is the serving plane (same idea as cf_http).
	ServerComponentStage = cf.Stage("app")
)

// RestartPolicy controls behavior when server bind settings change on reload.
type RestartPolicy string

const (
	// RestartPolicyHandled logs that a process restart is required (default).
	RestartPolicyHandled RestartPolicy = "handled"
	// RestartPolicyImmediate cancels Run so the process can exit and rebind.
	RestartPolicyImmediate RestartPolicy = "immediate"
)

// ServerConfig is file/env settings for one gRPC server instance.
// No field flag tags: multiple servers share a process flag namespace; use
// per-source EnvPrefix and the --<source-name> path override instead.
type ServerConfig struct {
	Bind                 string        `json:"bind,omitempty" yaml:"bind,omitempty" env:"BIND"`
	ShutdownTimeoutSec   *float64      `json:"shutdown_timeout_sec,omitempty" yaml:"shutdown_timeout_sec,omitempty" env:"SHUTDOWN_TIMEOUT_SEC"`
	KeepaliveTimeSec     *float64      `json:"keepalive_time_sec,omitempty" yaml:"keepalive_time_sec,omitempty" env:"KEEPALIVE_TIME_SEC"`
	KeepaliveTimeoutSec  *float64      `json:"keepalive_timeout_sec,omitempty" yaml:"keepalive_timeout_sec,omitempty" env:"KEEPALIVE_TIMEOUT_SEC"`
	MaxConnectionIdleSec *float64      `json:"max_connection_idle_sec,omitempty" yaml:"max_connection_idle_sec,omitempty" env:"MAX_CONNECTION_IDLE_SEC"`
	RestartPolicy        RestartPolicy `json:"restart_policy,omitempty" yaml:"restart_policy,omitempty" env:"RESTART_POLICY"`
}

// ServerOption configures a Server at construction time.
type ServerOption func(*serverOptions)

type serverOptions struct {
	loaded            *ServerConfig
	configSource      string
	configPath        string
	srcEnvPrefix      string
	srcFormat         cf_configuration.Format
	srcFormatSet      bool
	bind              string
	shutdownTimeout   time.Duration
	keepaliveTime     time.Duration
	keepaliveTimeout  time.Duration
	maxConnectionIdle time.Duration
	restartPolicy     RestartPolicy
	logger            *slog.Logger
	loggerSet         bool
	name              string
}

// WithServerConfig applies a static configuration snapshot.
func WithServerConfig(cfg ServerConfig) ServerOption {
	return func(o *serverOptions) { o.loaded = &cfg }
}

// WithServerConfigSource binds the server to a self-registered configuration source.
func WithServerConfigSource(name, path string, opts ...SourceOption) ServerOption {
	return func(o *serverOptions) {
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

// WithBind sets the listen address (host:port).
func WithBind(addr string) ServerOption {
	return func(o *serverOptions) { o.bind = addr }
}

// WithServerName sets a custom component name for multiple servers in one process.
func WithServerName(name string) ServerOption {
	return func(o *serverOptions) { o.name = name }
}

// WithServerLogger sets an explicit logger; skips the framework logs subscription.
func WithServerLogger(logger *slog.Logger) ServerOption {
	return func(o *serverOptions) {
		o.logger = logger
		o.loggerSet = true
	}
}

// WithShutdownTimeout sets the GracefulStop deadline.
func WithShutdownTimeout(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.shutdownTimeout = d }
}

// Server owns one gRPC listen/serve lifecycle for app-registered services.
type Server struct {
	mu sync.Mutex

	configSource string
	configPath   string
	srcEnvPrefix string
	srcFormat    cf_configuration.Format
	srcFormatSet bool

	bind              string
	shutdownTimeout   time.Duration
	keepaliveTime     time.Duration
	keepaliveTimeout  time.Duration
	maxConnectionIdle time.Duration
	restartPolicy     RestartPolicy

	name      string
	logger    *slog.Logger
	loggerSet bool
	logsSub   *cf_logs.Subscription
	fw        *cf.CaerusFramework

	grpcServer  *grpc.Server
	registerFns []func(*grpc.Server)
	registered  bool

	initialized bool
	running     bool
	listening   atomic.Bool
	boundAddr   string
	runCancel   context.CancelFunc

	restartRequired  atomic.Bool
	restartRequested atomic.Bool
	starts           atomic.Uint64
	reloads          atomic.Uint64
}

// NewServer creates an inert gRPC server component. It does not bind a port.
func NewServer(opts ...ServerOption) *Server {
	o := serverOptions{
		bind:              ":9090",
		shutdownTimeout:   10 * time.Second,
		keepaliveTime:     2 * time.Hour,
		keepaliveTimeout:  20 * time.Second,
		maxConnectionIdle: 15 * time.Minute,
		restartPolicy:     RestartPolicyHandled,
		logger:            slog.Default(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	s := &Server{
		configSource:      o.configSource,
		configPath:        o.configPath,
		srcEnvPrefix:      o.srcEnvPrefix,
		srcFormat:         o.srcFormat,
		srcFormatSet:      o.srcFormatSet,
		bind:              o.bind,
		shutdownTimeout:   o.shutdownTimeout,
		keepaliveTime:     o.keepaliveTime,
		keepaliveTimeout:  o.keepaliveTimeout,
		maxConnectionIdle: o.maxConnectionIdle,
		restartPolicy:     o.restartPolicy,
		name:              o.name,
		logger:            o.logger,
		loggerSet:         o.loggerSet,
	}
	if o.loaded != nil {
		s.applyServerConfig(*o.loaded)
	}
	return s
}

// Name implements cf.CaerusComponent.
func (s *Server) Name() string {
	if s.name != "" {
		return s.name
	}
	return ServerComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (s *Server) GetInitOrderStage() cf.Stage { return ServerComponentStage }

// GetDependencies implements cf.Dependencies.
func (s *Server) GetDependencies() []string {
	deps := []string{cf_logs.ComponentName}
	if s.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar.
func (s *Server) RegisterConfigSources(conf any) error {
	configuration, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_grpc: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if s.configSource == "" {
		return nil
	}
	return cf_configuration.AddSource(configuration, cf_configuration.Source[ServerConfig]{
		Name:      s.configSource,
		Path:      s.configPath,
		Format:    resolveSourceFormat(s.configPath, s.srcFormat, s.srcFormatSet),
		Owner:     s.Name(),
		EnvPrefix: s.srcEnvPrefix,
		Validate:  validateServerConfig,
	})
}

// Init prepares *grpc.Server. It does not listen — that is Run only.
func (s *Server) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialized {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.fw = fw
	if !s.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			s.logsSub = logs.OnReconfigureFor(s.Name(), func(l *slog.Logger) { s.logger = l })
		}
	}
	if s.configSource != "" {
		if err := s.applyServerConfigFromSource(); err != nil {
			s.unsubscribeLogs()
			return err
		}
	}
	if strings.TrimSpace(s.bind) == "" {
		s.unsubscribeLogs()
		return errors.New("cf_grpc: server bind is required")
	}
	s.grpcServer = grpc.NewServer(s.serverOptionsLocked()...)
	s.initialized = true
	s.logger.Info("cf_grpc: server initialized", "bind", s.bind)
	return nil
}

// Register adds a callback that registers generated services on the *grpc.Server.
// Call from the app's Init (after this server's Init). Must complete before Run.
func (s *Server) Register(fn func(*grpc.Server)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registered {
		// Too late — services already applied. Append is ignored; scream.
		s.logger.Error("cf_grpc: Register after services were applied; ignored")
		return
	}
	if s.grpcServer != nil {
		fn(s.grpcServer)
		return
	}
	s.registerFns = append(s.registerFns, fn)
}

// GRPCServer returns the underlying *grpc.Server after Init, or nil.
func (s *Server) GRPCServer() *grpc.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grpcServer
}

// Addr returns the bound listen address after Run starts, else the configured bind.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.boundAddr != "" {
		return s.boundAddr
	}
	return s.bind
}

// Run listens and serves until ctx cancel or GracefulStop.
func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return errors.New("cf_grpc: Run before Init")
	}
	if s.running {
		s.mu.Unlock()
		return errors.New("cf_grpc: Run already active")
	}
	for _, fn := range s.registerFns {
		fn(s.grpcServer)
	}
	s.registerFns = nil
	s.registered = true
	if s.grpcServer == nil {
		s.mu.Unlock()
		return errors.New("cf_grpc: missing grpc.Server")
	}
	bind := s.bind
	shutdownTimeout := s.shutdownTimeout
	gs := s.grpcServer
	runCtx, runCancel := context.WithCancel(ctx)
	s.running = true
	s.runCancel = runCancel
	s.restartRequested.Store(false)
	s.mu.Unlock()

	lis, err := net.Listen("tcp", bind)
	if err != nil {
		s.mu.Lock()
		s.running = false
		s.runCancel = nil
		s.mu.Unlock()
		runCancel()
		return fmt.Errorf("cf_grpc: listen %s: %w", bind, err)
	}

	s.mu.Lock()
	s.boundAddr = lis.Addr().String()
	s.mu.Unlock()
	s.listening.Store(true)
	s.starts.Add(1)
	s.logger.Info("cf_grpc: server listening", "addr", lis.Addr().String())

	errCh := make(chan error, 1)
	go func() {
		errCh <- gs.Serve(lis)
	}()

	select {
	case <-runCtx.Done():
		s.listening.Store(false)
		stopped := make(chan struct{})
		go func() {
			gs.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(shutdownTimeout)
		defer timer.Stop()
		select {
		case <-stopped:
		case <-timer.C:
			gs.Stop()
			<-stopped
		}
		serveErr := <-errCh
		s.mu.Lock()
		s.running = false
		s.runCancel = nil
		s.boundAddr = ""
		restart := s.restartRequested.Load()
		s.mu.Unlock()
		if restart {
			return ErrServerRestartRequired
		}
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	case err := <-errCh:
		s.listening.Store(false)
		s.mu.Lock()
		s.running = false
		s.runCancel = nil
		s.boundAddr = ""
		s.mu.Unlock()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return nil
	}
}

// ErrServerRestartRequired is returned by Run when restart_policy=immediate
// after a bind-changing config reload.
var ErrServerRestartRequired = errors.New("cf_grpc: server settings changed; immediate restart requested")

// OnConfigReload implements cf.ConfigReloader. Bind changes do not rebind live.
func (s *Server) OnConfigReload(source string, cfg any) {
	if source != s.configSource {
		return
	}
	loaded, ok := cfg.(*ServerConfig)
	if !ok {
		s.logger.Error("cf_grpc: server config reload rejected", "source", source, "type", fmt.Sprintf("%T", cfg))
		return
	}
	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return
	}
	bindChanged := loaded.Bind != "" && loaded.Bind != s.bind
	if loaded.RestartPolicy != "" {
		s.restartPolicy = loaded.RestartPolicy
	}
	if loaded.ShutdownTimeoutSec != nil {
		s.shutdownTimeout = time.Duration(*loaded.ShutdownTimeoutSec * float64(time.Second))
	}
	if bindChanged {
		switch s.restartPolicy {
		case RestartPolicyImmediate:
			s.restartRequired.Store(true)
			s.restartRequested.Store(true)
			cancel := s.runCancel
			s.mu.Unlock()
			s.logger.Error("cf_grpc: server bind changed; restart_policy=immediate — stopping so the process can rebind",
				"source", source, "bind", loaded.Bind)
			if cancel != nil {
				cancel()
			}
			s.reloads.Add(1)
			return
		default:
			s.restartRequired.Store(true)
			s.logger.Error("cf_grpc: server bind changed; restart required — current listener stays until a new process rebinds",
				"source", source, "bind", loaded.Bind)
		}
	}
	s.mu.Unlock()
	s.reloads.Add(1)
	s.logger.Info("cf_grpc: server configuration reloaded", "source", source)
}

// Health implements cf.HealthProvider.
func (s *Server) Health(ctx context.Context) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.initialized {
		return errors.New("cf_grpc: server not initialized")
	}
	if !s.listening.Load() {
		return errors.New("cf_grpc: server not listening")
	}
	return nil
}

// Metrics implements cf_observability.MetricsProvider.
func (s *Server) Metrics() []cf_observability.Metric {
	if !s.initialized {
		return nil
	}
	listening := 0.0
	if s.listening.Load() {
		listening = 1
	}
	restart := 0.0
	if s.restartRequired.Load() {
		restart = 1
	}
	return []cf_observability.Metric{
		{Name: "grpc_server_listening", Help: "1 when the gRPC server is accepting connections.", Value: listening, Labels: map[string]string{"component": s.Name()}},
		{Name: "grpc_server_restart_required", Help: "1 when bind settings changed and a process restart is needed.", Value: restart, Labels: map[string]string{"component": s.Name()}},
		{Name: "grpc_server_starts_total", Help: "Times Run successfully began listening.", Value: float64(s.starts.Load()), Labels: map[string]string{"component": s.Name()}},
		{Name: "grpc_server_reloads_total", Help: "Config reload notifications handled.", Value: float64(s.reloads.Load()), Labels: map[string]string{"component": s.Name()}},
	}
}

// Shutdown stops the server if running and unsubscribes from logs.
func (s *Server) Shutdown(ctx context.Context) error {
	_ = ctx
	s.mu.Lock()
	cancel := s.runCancel
	gs := s.grpcServer
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if gs != nil {
		gs.GracefulStop()
	}
	s.mu.Lock()
	s.listening.Store(false)
	s.initialized = false
	s.running = false
	s.grpcServer = nil
	s.registered = false
	s.unsubscribeLogs()
	s.mu.Unlock()
	return nil
}

func (s *Server) unsubscribeLogs() {
	if s.logsSub != nil {
		s.logsSub.Unsubscribe()
		s.logsSub = nil
	}
}

func (s *Server) applyServerConfigFromSource() error {
	configuration, ok := cf.Get[*cf_configuration.Configuration](s.fw)
	if !ok {
		return errors.New("cf_grpc: configuration component not registered")
	}
	cfg, ok := cf_configuration.Get[ServerConfig](configuration, s.configSource)
	if !ok {
		return fmt.Errorf("cf_grpc: configuration source %q not found", s.configSource)
	}
	s.applyServerConfig(cfg)
	return nil
}

func (s *Server) applyServerConfig(cfg ServerConfig) {
	if cfg.Bind != "" {
		s.bind = cfg.Bind
	}
	if cfg.ShutdownTimeoutSec != nil {
		s.shutdownTimeout = time.Duration(*cfg.ShutdownTimeoutSec * float64(time.Second))
	}
	if cfg.KeepaliveTimeSec != nil {
		s.keepaliveTime = time.Duration(*cfg.KeepaliveTimeSec * float64(time.Second))
	}
	if cfg.KeepaliveTimeoutSec != nil {
		s.keepaliveTimeout = time.Duration(*cfg.KeepaliveTimeoutSec * float64(time.Second))
	}
	if cfg.MaxConnectionIdleSec != nil {
		s.maxConnectionIdle = time.Duration(*cfg.MaxConnectionIdleSec * float64(time.Second))
	}
	if cfg.RestartPolicy != "" {
		s.restartPolicy = cfg.RestartPolicy
	}
}

func (s *Server) serverOptionsLocked() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: s.maxConnectionIdle,
			Time:              s.keepaliveTime,
			Timeout:           s.keepaliveTimeout,
		}),
	}
}

func validateServerConfig(cfg *ServerConfig) error {
	for name, value := range map[string]*float64{
		"shutdown_timeout_sec":    cfg.ShutdownTimeoutSec,
		"keepalive_time_sec":      cfg.KeepaliveTimeSec,
		"keepalive_timeout_sec":   cfg.KeepaliveTimeoutSec,
		"max_connection_idle_sec": cfg.MaxConnectionIdleSec,
	} {
		if value != nil {
			if err := validateSeconds(name, *value, true); err != nil {
				return err
			}
		}
	}
	switch cfg.RestartPolicy {
	case "", RestartPolicyHandled, RestartPolicyImmediate:
	default:
		return fmt.Errorf("cf_grpc: unknown restart_policy %q (want %q or %q)",
			cfg.RestartPolicy, RestartPolicyHandled, RestartPolicyImmediate)
	}
	return nil
}

var _ cf.CaerusComponent = (*Server)(nil)
var _ cf.Dependencies = (*Server)(nil)
var _ cf.Runnable = (*Server)(nil)
var _ cf.HealthProvider = (*Server)(nil)
var _ cf_observability.MetricsProvider = (*Server)(nil)
var _ cf.ConfigReloader = (*Server)(nil)
var _ cf.ConfigSourceRegistrar = (*Server)(nil)
