package cf_grpc

import (
	"strings"

	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
)

// SourceOption configures a self-registered configuration source.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	envPrefix string
	format    cf_configuration.Format
	formatSet bool
}

// WithSourceEnvPrefix overrides the environment prefix for a source.
func WithSourceEnvPrefix(prefix string) SourceOption {
	return func(o *sourceOptions) { o.envPrefix = prefix }
}

// WithSourceFormat forces the source file format.
func WithSourceFormat(format cf_configuration.Format) SourceOption {
	return func(o *sourceOptions) {
		o.format = format
		o.formatSet = true
	}
}

func defaultSourceEnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

func resolveSourceFormat(path string, forced cf_configuration.Format, formatSet bool) cf_configuration.Format {
	if formatSet {
		return forced
	}
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") {
		return cf_configuration.FormatYAML
	}
	return cf_configuration.FormatJSON
}
