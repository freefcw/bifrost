package circuitbreaker

import (
	"fmt"
	"time"
)

const (
	// PluginName is the config.json plugins[].name that turns the breaker on.
	// Absent, or present with enabled false, the plugin is not loaded and the
	// gateway behaves as it does without it.
	PluginName = "circuitbreaker"

	defaultFailureThreshold = 2
	defaultCooldownSeconds  = 15
	defaultSlowSeconds      = 10
	defaultMaxProbes        = 1

	// abandonAfter drops in-flight marks whose request never settled, so one
	// leaked attempt cannot pin the concurrency cap.
	abandonAfter = 15 * time.Minute
)

// Config is the plugins[].config object. Nil pointers keep the defaults.
// slow_threshold_seconds and max_inflight use a pointer so 0 is a real value:
// 0 disables slow shedding, and 0 leaves the in-flight cap off.
type Config struct {
	// FailureThreshold is how many consecutive counted failures open a circuit.
	FailureThreshold *int `json:"failure_threshold,omitempty"`
	// CooldownSeconds is how long a circuit stays open before one probe is allowed.
	CooldownSeconds *int `json:"cooldown_seconds,omitempty"`
	// SlowThresholdSeconds treats an attempt that has produced no response byte
	// for this long as a sign the upstream is hanging. Later attempts are then
	// held to MaxProbes in flight. 0 disables that signal.
	SlowThresholdSeconds *int `json:"slow_threshold_seconds,omitempty"`
	// MaxInFlight is a hard cap on attempts in flight to one provider+model,
	// including while it still looks healthy. 0 means no cap. A simultaneous
	// burst at the start of an outage is otherwise admitted until a failure
	// returns or a call crosses SlowThresholdSeconds.
	MaxInFlight *int `json:"max_inflight,omitempty"`
	// MaxProbes is how many attempts may be in flight once the circuit is
	// half-open or already degraded (a failure recorded, or a call past the
	// slow threshold). The provider-level probe is always a single attempt.
	MaxProbes *int `json:"max_probes,omitempty"`
	// CountRateLimit counts HTTP 429 as a failure of the key that received it.
	// Other keys of the same provider keep taking traffic.
	CountRateLimit *bool `json:"count_rate_limit,omitempty"`
}

// settings is the resolved config. Zero slow or maxInFlight means off.
type settings struct {
	failureThreshold int
	cooldown         time.Duration
	slow             time.Duration
	maxInFlight      int
	maxProbes        int
	countRateLimit   bool
}

func resolve(cfg *Config) (settings, error) {
	s := settings{
		failureThreshold: defaultFailureThreshold,
		cooldown:         defaultCooldownSeconds * time.Second,
		slow:             defaultSlowSeconds * time.Second,
		maxProbes:        defaultMaxProbes,
	}
	if cfg == nil {
		return s, nil
	}
	if cfg.FailureThreshold != nil {
		if *cfg.FailureThreshold < 1 {
			return settings{}, fmt.Errorf("circuitbreaker failure_threshold must be >= 1")
		}
		s.failureThreshold = *cfg.FailureThreshold
	}
	if cfg.CooldownSeconds != nil {
		if *cfg.CooldownSeconds < 1 {
			return settings{}, fmt.Errorf("circuitbreaker cooldown_seconds must be >= 1")
		}
		s.cooldown = time.Duration(*cfg.CooldownSeconds) * time.Second
	}
	if cfg.SlowThresholdSeconds != nil {
		if *cfg.SlowThresholdSeconds < 0 {
			return settings{}, fmt.Errorf("circuitbreaker slow_threshold_seconds must be >= 0")
		}
		s.slow = time.Duration(*cfg.SlowThresholdSeconds) * time.Second
	}
	if cfg.MaxInFlight != nil {
		if *cfg.MaxInFlight < 0 {
			return settings{}, fmt.Errorf("circuitbreaker max_inflight must be >= 0")
		}
		s.maxInFlight = *cfg.MaxInFlight
	}
	if cfg.MaxProbes != nil {
		if *cfg.MaxProbes < 1 {
			return settings{}, fmt.Errorf("circuitbreaker max_probes must be >= 1")
		}
		s.maxProbes = *cfg.MaxProbes
	}
	if cfg.CountRateLimit != nil {
		s.countRateLimit = *cfg.CountRateLimit
	}
	return s, nil
}

func (s settings) probeWindow() time.Duration {
	if s.cooldown > 0 {
		return s.cooldown
	}
	return 30 * time.Second
}
