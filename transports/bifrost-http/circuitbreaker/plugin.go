// Package circuitbreaker is an opt-in upstream health tracker. It records how
// each provider, model and key answers, and skips attempts whose circuit is
// open so a configured fallback runs without waiting on a dead upstream.
//
// Loaded only when config.json lists plugins[].name "circuitbreaker" with
// enabled true. With the plugin absent, request handling is unchanged aside
// from one context lookup in key selection that finds nothing.
package circuitbreaker

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

// KeyLister returns the keys configured for a provider. The HTTP gateway
// passes its live key list so a circuit can tell when every key is open.
// Nil means only keys this process has already seen are known.
type KeyLister func(provider schemas.ModelProvider) []schemas.Key

type scope uint8

const (
	scopeProvider scope = iota + 1
	scopeModel
	scopeKey
)

// circuitID is the map key. Strings are the request's own, so a lookup does
// not allocate.
type circuitID struct {
	scope    scope
	provider schemas.ModelProvider
	model    string
	keyID    string
}

func (id circuitID) String() string {
	switch id.scope {
	case scopeProvider:
		return string(id.provider)
	case scopeModel:
		return string(id.provider) + "/" + id.model
	default:
		return string(id.provider) + "/" + id.model + " key " + id.keyID
	}
}

// snap is the last counted upstream error, replayed when a later attempt is
// skipped so the caller sees that error instead of one the breaker invented.
type snap struct {
	status  int
	typ     string
	message string
}

// flight is one admitted attempt that has not settled.
type flight struct {
	id      string
	start   time.Time
	sawByte bool
}

// circuit is the state of one provider, one provider+model, or one key.
// The plugin mutex guards it; the circuit does not have its own lock.
type circuit struct {
	fails      int
	openUntil  time.Time
	changedAt  time.Time // when the last counted failure was recorded
	probing    bool
	probeStart time.Time
	last       snap
	pending    []flight
}

type admission struct {
	id            string
	at            time.Time
	provider      schemas.ModelProvider
	model         string
	providerProbe bool
	modelProbe    bool
}

type (
	admissionKey struct{}
	replayKey    struct{}
	settledKey   struct{}
	sawByteKey   struct{}
)

// Plugin implements schemas.LLMPlugin.
type Plugin struct {
	cfg  settings
	log  schemas.Logger
	keys KeyLister
	now  func() time.Time

	// mu guards circuits. The map is process state reused across requests;
	// a live-store table cannot host this read-modify-write (concurrent
	// admits must not lose a failure count). Entries exist only for upstreams
	// that are in flight or unhealthy. A provider, model or key entry is removed
	// once it is healthy and has nothing in flight, so one-off model names do
	// not stay in the map.
	mu       sync.Mutex
	circuits map[circuitID]*circuit
	seq      atomic.Uint64
}

// Init builds a breaker. A nil config uses the defaults. A nil logger discards
// log lines. keys may be nil.
func Init(cfg *Config, logger schemas.Logger, keys KeyLister) (*Plugin, error) {
	s, err := resolve(cfg)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = discardLog{}
	}
	return &Plugin{
		cfg:      s,
		log:      logger,
		keys:     keys,
		now:      time.Now,
		circuits: map[circuitID]*circuit{},
	}, nil
}

func (p *Plugin) GetName() string { return PluginName }

func (p *Plugin) Cleanup() error { return nil }

func (p *Plugin) PreRequestHook(*schemas.BifrostContext, *schemas.BifrostRequest) error {
	return nil
}

// PreLLMHook runs once per attempt, after routing has chosen the provider.
// An open circuit is answered with the last real upstream error and
// AllowFallbacks left on, so the fallback chain runs and a chain that is
// entirely open still surfaces that upstream error.
func (p *Plugin) PreLLMHook(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	if ctx == nil || req == nil {
		return req, nil, nil
	}
	ctx.ClearValue(admissionKey{})
	ctx.SetValue(replayKey{}, false)
	ctx.SetValue(settledKey{}, false)
	ctx.SetValue(sawByteKey{}, false)

	provider, model, _ := req.GetRequestFields()
	if provider == "" || model == "" {
		ctx.ClearValue(schemas.BifrostContextKeyCircuitSkipKeyIDs)
		return req, nil, nil
	}
	pinned := p.pinnedKeyID(ctx, provider)

	skip, block, adm := p.admit(provider, model, p.nextFlight(), pinned)
	if block != nil {
		ctx.SetValue(replayKey{}, true)
		ctx.ClearValue(schemas.BifrostContextKeyCircuitSkipKeyIDs)
		return req, &schemas.LLMPluginShortCircuit{Error: block}, nil
	}
	if len(skip) == 0 {
		ctx.ClearValue(schemas.BifrostContextKeyCircuitSkipKeyIDs)
	} else {
		ctx.SetValue(schemas.BifrostContextKeyCircuitSkipKeyIDs, skip)
	}
	ctx.SetValue(admissionKey{}, adm)
	return req, nil, nil
}

// PostLLMHook records the attempt. Stream chunks before the terminal chunk
// only mark that a byte arrived, so a healthy long stream is not a hang.
func (p *Plugin) PostLLMHook(ctx *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if ctx == nil {
		return resp, bifrostErr, nil
	}
	if replayed, _ := ctx.Value(replayKey{}).(bool); replayed {
		return resp, bifrostErr, nil
	}
	adm, ok := ctx.Value(admissionKey{}).(admission)
	if !ok || adm.id == "" {
		return resp, bifrostErr, nil
	}
	if !terminal(ctx, bifrostErr) {
		if saw, _ := ctx.Value(sawByteKey{}).(bool); !saw {
			ctx.SetValue(sawByteKey{}, true)
			p.markByte(adm)
		}
		return resp, bifrostErr, nil
	}
	if settled, _ := ctx.Value(settledKey{}).(bool); settled {
		return resp, bifrostErr, nil
	}
	ctx.SetValue(settledKey{}, true)
	p.settle(ctx, adm, bifrostErr)
	if replay := p.replayIfKeysSkipped(adm, bifrostErr); replay != nil {
		return resp, replay, nil
	}
	return resp, bifrostErr, nil
}

func terminal(ctx *schemas.BifrostContext, err *schemas.BifrostError) bool {
	if err != nil {
		return true
	}
	if end, _ := ctx.Value(schemas.BifrostContextKeyStreamEndIndicator).(bool); end {
		return true
	}
	// Unary attempts have no stream clock. A stream chunk without the end
	// marker is not the outcome.
	_, streaming := ctx.Value(schemas.BifrostContextKeyStreamStartTime).(time.Time)
	return !streaming
}

func (p *Plugin) nextFlight() string {
	return strconv.FormatUint(p.seq.Add(1), 10)
}

// pinnedKeyID is the key the caller named for this attempt, by id or by name.
// A name is resolved through the key list the HTTP gateway passes in. Core
// returns a name pin before it applies the skip list, so the pin has to be
// turned into an id here.
func (p *Plugin) pinnedKeyID(ctx *schemas.BifrostContext, provider schemas.ModelProvider) string {
	if id, _ := ctx.Value(schemas.BifrostContextKeyAPIKeyID).(string); strings.TrimSpace(id) != "" {
		return strings.TrimSpace(id)
	}
	name, _ := ctx.Value(schemas.BifrostContextKeyAPIKeyName).(string)
	name = strings.TrimSpace(name)
	if name == "" || p.keys == nil {
		return ""
	}
	for _, key := range p.keys(provider) {
		if key.Name == name && key.ID != "" {
			return key.ID
		}
	}
	return ""
}

// admit decides whether this attempt may call the upstream. skip is the set of
// key IDs key selection should pass over. block is set when the attempt itself
// must not run; its error is the last upstream error for that circuit.
func (p *Plugin) admit(provider schemas.ModelProvider, model, id, pinned string) (map[string]struct{}, *schemas.BifrostError, admission) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.sweep(now)
	adm := admission{id: id, at: now, provider: provider, model: model}

	provID := circuitID{scope: scopeProvider, provider: provider}
	prov := p.circuits[provID]
	if prov.hardOpen(now) {
		return nil, blockError(prov, provider, model), admission{}
	}
	claimProvider := false
	if prov.halfOpen(now) {
		if !prov.probeFree(now, p.cfg.probeWindow()) {
			return nil, blockError(prov, provider, model), admission{}
		}
		claimProvider = true
	}

	modID := circuitID{scope: scopeModel, provider: provider, model: model}
	mod := p.circuits[modID]
	if mod.hardOpen(now) {
		return nil, blockError(mod, provider, model), admission{}
	}
	if mod.halfOpen(now) {
		if mod.young(now, p.cfg.probeWindow()) >= p.cfg.maxProbes {
			return nil, blockError(mod, provider, model), admission{}
		}
		adm.modelProbe = true
	} else if mod.atCap(now, p.cfg) {
		return nil, blockError(mod, provider, model), admission{}
	}

	if pinned != "" {
		if k := p.circuits[circuitID{scope: scopeKey, provider: provider, model: model, keyID: pinned}]; k.hardOpen(now) {
			return nil, blockError(k, provider, model), admission{}
		}
	}
	skip, allBlocked, keyErr := p.keySkip(provider, model, now)
	if allBlocked {
		if keyErr != nil {
			return nil, keyErr, admission{}
		}
		return nil, shedError(provider, model), admission{}
	}

	if claimProvider {
		if prov == nil {
			prov = p.add(provID)
		}
		prov.probing = true
		prov.probeStart = now
		adm.providerProbe = true
		p.log.Info("[circuitbreaker] probe %s", provID)
	}
	if mod == nil {
		mod = p.add(modID)
	}
	mod.pending = append(mod.pending, flight{id: id, start: now})
	if adm.modelProbe {
		p.log.Info("[circuitbreaker] probe %s", modID)
	}
	return skip, nil, adm
}

// keySkip lists hard-open keys for this provider and model. allBlocked is set
// when a key list was provided and every key that can serve the model is open.
func (p *Plugin) keySkip(provider schemas.ModelProvider, model string, now time.Time) (map[string]struct{}, bool, *schemas.BifrostError) {
	var openIDs []string
	var best snap
	var have bool
	for id, c := range p.circuits {
		if id.scope != scopeKey || id.provider != provider || id.model != model || !c.hardOpen(now) {
			continue
		}
		openIDs = append(openIDs, id.keyID)
		if c.last.status != 0 {
			best = c.last
			have = true
		}
	}
	if len(openIDs) == 0 {
		return nil, false, nil
	}
	if p.keys != nil {
		serving := 0
		openSet := make(map[string]struct{}, len(openIDs))
		for _, id := range openIDs {
			openSet[id] = struct{}{}
		}
		all := true
		for _, key := range p.keys(provider) {
			if key.ID == "" || !keyServes(key, model) {
				continue
			}
			serving++
			if _, ok := openSet[key.ID]; !ok {
				all = false
			}
		}
		if serving > 0 && all {
			if have {
				return nil, true, best.replay()
			}
			return nil, true, shedError(provider, model)
		}
	}
	skip := make(map[string]struct{}, len(openIDs))
	for _, id := range openIDs {
		skip[id] = struct{}{}
	}
	return skip, false, nil
}

func (p *Plugin) markByte(adm admission) {
	p.mu.Lock()
	defer p.mu.Unlock()
	mod := p.circuits[circuitID{scope: scopeModel, provider: adm.provider, model: adm.model}]
	if mod == nil {
		return
	}
	for i := range mod.pending {
		if mod.pending[i].id == adm.id {
			mod.pending[i].sawByte = true
			return
		}
	}
}

func (p *Plugin) settle(ctx *schemas.BifrostContext, adm admission, err *schemas.BifrostError) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	modID := circuitID{scope: scopeModel, provider: adm.provider, model: adm.model}
	sawByte := false
	if mod := p.circuits[modID]; mod != nil {
		sawByte = mod.saw(adm.id)
		mod.drop(adm.id)
	}

	if err == nil {
		p.recordTrail(ctx, adm, now)
		p.noteSuccess(adm, selectedKeyID(ctx))
		return
	}
	cancelled := bifrost.ClassifyFailure(err) == schemas.FailureClassCancelled
	if trail, _ := ctx.Value(schemas.BifrostContextKeyAttemptTrail).([]schemas.KeyAttemptRecord); len(trail) > 0 {
		for i := range trail {
			e := err
			// A cancel during backoff never dispatched another attempt. The last
			// trail record is the upstream failure that already happened; the
			// request-level cancel must not hide it.
			if i != len(trail)-1 || (cancelled && trail[i].FailureClass != "") {
				e = errFromRecord(&trail[i])
			}
			probe := adm.providerProbe && i == len(trail)-1
			modelProbe := adm.modelProbe && i == len(trail)-1
			p.observe(adm.provider, adm.model, trail[i].KeyID, e, probe, modelProbe, sawByte, now)
		}
		p.dropIdle(adm.provider, adm.model)
		return
	}
	p.observe(adm.provider, adm.model, selectedKeyID(ctx), err, adm.providerProbe, adm.modelProbe, sawByte, now)
	p.dropIdle(adm.provider, adm.model)
}

// recordTrail counts failures of keys rotated away from before a later
// success, so a 429 on key A still opens A's circuit when key B then answers.
func (p *Plugin) recordTrail(ctx *schemas.BifrostContext, adm admission, now time.Time) {
	trail, _ := ctx.Value(schemas.BifrostContextKeyAttemptTrail).([]schemas.KeyAttemptRecord)
	for i := range trail {
		if trail[i].FailureClass == "" {
			continue
		}
		p.observe(adm.provider, adm.model, trail[i].KeyID, errFromRecord(&trail[i]), false, false, false, now)
	}
}

func (p *Plugin) noteSuccess(adm admission, keyID string) {
	p.finish(circuitID{scope: scopeProvider, provider: adm.provider}, adm.at)
	p.finish(circuitID{scope: scopeModel, provider: adm.provider, model: adm.model}, adm.at)
	if keyID != "" {
		p.finish(circuitID{scope: scopeKey, provider: adm.provider, model: adm.model, keyID: keyID}, adm.at)
	}
	p.dropIdle(adm.provider, adm.model)
}

// finish clears a circuit whose last failure is not newer than this attempt.
// A success that was admitted before a later failure opened the circuit leaves
// that newer state in place.
func (p *Plugin) finish(id circuitID, admittedAt time.Time) {
	c := p.circuits[id]
	if c == nil || c.changedAt.After(admittedAt) {
		return
	}
	if c.fails > 0 || !c.openUntil.IsZero() {
		p.log.Info("[circuitbreaker] closed %s", id)
	}
	c.succeed()
	if id.scope != scopeModel || c.idle() {
		delete(p.circuits, id)
	}
}

func (p *Plugin) observe(provider schemas.ModelProvider, model, keyID string, err *schemas.BifrostError, providerProbe, modelProbe, sawByte bool, now time.Time) {
	if err == nil {
		return
	}
	class := bifrost.ClassifyFailure(err)
	if class == schemas.FailureClassCancelled {
		p.releaseProbe(provider, providerProbe)
		return
	}
	conn := isConnection(err)
	// A stream that already produced a byte and then lost the connection is
	// not the host being unreachable. Bedrock reports that drop with the same
	// message as a failure to connect.
	if conn && sawByte {
		conn = false
	}
	timeout := class == schemas.FailureClassTimeout
	s := snapOf(err)
	status := 0
	if err.StatusCode != nil {
		status = *err.StatusCode
	}

	provID := circuitID{scope: scopeProvider, provider: provider}
	switch {
	case conn || (providerProbe && timeout):
		c := p.must(provID)
		// providerProbe forces the circuit back open: a half-open probe that
		// cannot reach the host must not wait out the threshold again.
		if c.fail(now, p.cfg, providerProbe, s) {
			p.log.Info("[circuitbreaker] open %s after %d connection failures until %s", provID, c.fails, c.openUntil.Format(time.RFC3339))
		}
	case reachedHost(err):
		if c := p.circuits[provID]; c != nil && (c.fails > 0 || !c.openUntil.IsZero() || c.probing) {
			if c.fails > 0 || !c.openUntil.IsZero() {
				p.log.Info("[circuitbreaker] closed %s", provID)
			}
			c.succeed()
			delete(p.circuits, provID)
		}
	default:
		p.releaseProbe(provider, providerProbe)
	}

	switch {
	case conn || class == schemas.FailureClassCancelled:
		return
	case timeout || class == schemas.FailureClassTransient || (class == schemas.FailureClassRateLimit && status >= 500):
		// A 5xx whose body mentions a rate limit is still the model endpoint
		// failing. Only an HTTP 429 is scoped to the key.
		c := p.must(circuitID{scope: scopeModel, provider: provider, model: model})
		if c.fail(now, p.cfg, modelProbe, s) {
			why := "upstream"
			if timeout {
				why = "timeout"
			}
			p.log.Info("[circuitbreaker] open %s after %d %s failures until %s", circuitID{scope: scopeModel, provider: provider, model: model}, c.fails, why, c.openUntil.Format(time.RFC3339))
		}
	case class == schemas.FailureClassRateLimit && p.cfg.countRateLimit && keyID != "" && status == 429:
		id := circuitID{scope: scopeKey, provider: provider, model: model, keyID: keyID}
		c := p.must(id)
		if c.fail(now, p.cfg, false, s) {
			p.log.Info("[circuitbreaker] open %s after %d rate-limit failures until %s", id, c.fails, c.openUntil.Format(time.RFC3339))
		}
	}
}

func (p *Plugin) releaseProbe(provider schemas.ModelProvider, providerProbe bool) {
	if !providerProbe {
		return
	}
	if c := p.circuits[circuitID{scope: scopeProvider, provider: provider}]; c != nil {
		c.probing = false
	}
}

func (p *Plugin) must(id circuitID) *circuit {
	c := p.circuits[id]
	if c == nil {
		c = p.add(id)
	}
	return c
}

func (p *Plugin) add(id circuitID) *circuit {
	c := &circuit{}
	p.circuits[id] = c
	return c
}

func (p *Plugin) sweep(now time.Time) {
	for id, c := range p.circuits {
		if id.scope != scopeModel {
			continue
		}
		if len(c.pending) > 0 {
			kept := c.pending[:0]
			for _, f := range c.pending {
				if now.Sub(f.start) < abandonAfter {
					kept = append(kept, f)
				}
			}
			c.pending = kept
		}
		if c.idle() {
			delete(p.circuits, id)
		}
	}
}

func (p *Plugin) dropIdle(provider schemas.ModelProvider, model string) {
	id := circuitID{scope: scopeModel, provider: provider, model: model}
	if c := p.circuits[id]; c != nil && c.idle() {
		delete(p.circuits, id)
	}
}

func (c *circuit) idle() bool {
	return c != nil && c.fails == 0 && c.openUntil.IsZero() && !c.probing && len(c.pending) == 0
}

func (c *circuit) saw(id string) bool {
	if c == nil {
		return false
	}
	for _, f := range c.pending {
		if f.id == id {
			return f.sawByte
		}
	}
	return false
}

func (c *circuit) hardOpen(now time.Time) bool {
	return c != nil && !c.openUntil.IsZero() && now.Before(c.openUntil)
}

func (c *circuit) halfOpen(now time.Time) bool {
	return c != nil && !c.openUntil.IsZero() && !now.Before(c.openUntil)
}

func (c *circuit) probeFree(now time.Time, window time.Duration) bool {
	if c == nil || !c.probing {
		return true
	}
	return now.Sub(c.probeStart) >= window
}

func (c *circuit) young(now time.Time, window time.Duration) int {
	if c == nil {
		return 0
	}
	n := 0
	for _, f := range c.pending {
		if window <= 0 || now.Sub(f.start) < window {
			n++
		}
	}
	return n
}

func (c *circuit) atCap(now time.Time, cfg settings) bool {
	if c == nil {
		return false
	}
	if cfg.maxInFlight > 0 && len(c.pending) >= cfg.maxInFlight {
		return true
	}
	if !c.degraded(now, cfg) {
		return false
	}
	return len(c.pending) >= cfg.maxProbes
}

func (c *circuit) degraded(now time.Time, cfg settings) bool {
	if c.fails > 0 {
		return true
	}
	if cfg.slow <= 0 {
		return false
	}
	for _, f := range c.pending {
		if !f.sawByte && now.Sub(f.start) >= cfg.slow {
			return true
		}
	}
	return false
}

func (c *circuit) drop(id string) {
	for i := range c.pending {
		if c.pending[i].id == id {
			c.pending[i] = c.pending[len(c.pending)-1]
			c.pending = c.pending[:len(c.pending)-1]
			return
		}
	}
}

// fail records one counted failure. probe forces the circuit open, which is
// how a half-open probe that fails goes straight back to open.
func (c *circuit) fail(now time.Time, cfg settings, probe bool, s snap) bool {
	wasOpen := c.hardOpen(now)
	recovering := c.halfOpen(now)
	c.probing = false
	c.fails++
	if s.status != 0 {
		c.last = s
	}
	if probe || recovering || c.fails >= cfg.failureThreshold {
		c.openUntil = now.Add(cfg.cooldown)
	}
	c.changedAt = now
	return !wasOpen && c.hardOpen(now)
}

func (c *circuit) succeed() {
	c.fails = 0
	c.openUntil = time.Time{}
	c.changedAt = time.Time{}
	c.probing = false
	c.last = snap{}
}

func isConnection(err *schemas.BifrostError) bool {
	if err == nil || err.Error == nil {
		return false
	}
	if err.Error.Message == schemas.ErrProviderDoRequest || err.Error.Message == schemas.ErrProviderNetworkError {
		return true
	}
	if err.Error.Type != nil && *err.Error.Type == schemas.ProviderConnectionFailed {
		return true
	}
	return err.Type != nil && *err.Type == schemas.ProviderConnectionFailed
}

// reachedHost reports whether the upstream answered. A connection failure, a
// timeout, a cancellation and a key-pool refusal did not.
func reachedHost(err *schemas.BifrostError) bool {
	if err == nil {
		return true
	}
	if err.Type != nil && *err.Type == schemas.NoKeySupportsModel {
		return false
	}
	if err.Error != nil && err.Error.Type != nil && *err.Error.Type == schemas.NoKeySupportsModel {
		return false
	}
	if isConnection(err) {
		return false
	}
	switch bifrost.ClassifyFailure(err) {
	case schemas.FailureClassTimeout, schemas.FailureClassCancelled, schemas.FailureClassUnknown:
		return false
	default:
		return true
	}
}

func snapOf(err *schemas.BifrostError) snap {
	s := snap{message: "upstream request failed"}
	if err == nil {
		return s
	}
	if err.StatusCode != nil {
		s.status = *err.StatusCode
	}
	if err.Error != nil {
		if err.Error.Message != "" {
			s.message = trim(err.Error.Message, 512)
		}
		if err.Error.Type != nil {
			s.typ = *err.Error.Type
		}
	}
	if s.typ == "" && err.Type != nil {
		s.typ = *err.Type
	}
	if s.status == 0 {
		s.status = 502
	}
	return s
}

func (s snap) replay() *schemas.BifrostError {
	status := s.status
	out := &schemas.BifrostError{
		StatusCode:     &status,
		AllowFallbacks: schemas.Ptr(true),
		Error:          &schemas.ErrorField{Message: s.message},
	}
	if s.typ != "" {
		typ := s.typ
		out.Type = &typ
		out.Error.Type = &typ
	}
	return out
}

func blockError(c *circuit, provider schemas.ModelProvider, model string) *schemas.BifrostError {
	if c != nil && c.last.status != 0 {
		return c.last.replay()
	}
	return shedError(provider, model)
}

func shedError(provider schemas.ModelProvider, model string) *schemas.BifrostError {
	status := 503
	typ := "circuit_open"
	return &schemas.BifrostError{
		StatusCode:     &status,
		Type:           &typ,
		AllowFallbacks: schemas.Ptr(true),
		Error: &schemas.ErrorField{
			Type:    &typ,
			Message: "upstream " + string(provider) + "/" + model + " is not admitting requests",
		},
	}
}

func errFromRecord(rec *schemas.KeyAttemptRecord) *schemas.BifrostError {
	status := 500
	if rec.StatusCode != nil {
		status = *rec.StatusCode
	}
	typ := string(rec.FailureClass)
	if rec.FailReason != nil && *rec.FailReason != "" {
		typ = *rec.FailReason
	}
	return &schemas.BifrostError{
		StatusCode: &status,
		Type:       &typ,
		Error:      &schemas.ErrorField{Type: &typ, Message: typ},
	}
}

func selectedKeyID(ctx *schemas.BifrostContext) string {
	id, _ := ctx.Value(schemas.BifrostContextKeySelectedKeyID).(string)
	return id
}

func keyServes(key schemas.Key, model string) bool {
	if key.Enabled != nil && !*key.Enabled {
		return false
	}
	if key.Models.IsEmpty() || !key.Models.IsAllowed(model) || key.BlacklistedModels.IsBlocked(model) {
		return false
	}
	// An empty secret with no OAuth config cannot be selected. Core's
	// provider-specific credential checks stay in core; an empty pool there
	// still replays the stored upstream error.
	if strings.TrimSpace(key.Value.GetValue()) == "" && key.OAuthKeyConfig == nil {
		return false
	}
	if key.VLLMKeyConfig != nil && key.VLLMKeyConfig.ModelName != "" {
		served := model
		if key.Aliases != nil {
			served = key.Aliases.Resolve(model)
		}
		if key.VLLMKeyConfig.ModelName != served {
			return false
		}
	}
	return true
}

func (p *Plugin) replayIfKeysSkipped(adm admission, err *schemas.BifrostError) *schemas.BifrostError {
	if !skippedEveryKey(err) {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for id, c := range p.circuits {
		if id.scope == scopeKey && id.provider == adm.provider && id.model == adm.model && c.hardOpen(now) && c.last.status != 0 {
			return c.last.replay()
		}
	}
	return nil
}

func skippedEveryKey(err *schemas.BifrostError) bool {
	if err == nil || err.Error == nil {
		return false
	}
	return strings.Contains(err.Error.Message, "circuit breaker skipped every key")
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

type discardLog struct{}

func (discardLog) Debug(string, ...any)                   {}
func (discardLog) Info(string, ...any)                    {}
func (discardLog) Warn(string, ...any)                    {}
func (discardLog) Error(string, ...any)                   {}
func (discardLog) Fatal(string, ...any)                   {}
func (discardLog) SetLevel(schemas.LogLevel)              {}
func (discardLog) SetOutputType(schemas.LoggerOutputType) {}
func (discardLog) LogHTTPRequest(schemas.LogLevel, string) schemas.LogEventBuilder {
	return discardEvent{}
}

type discardEvent struct{}

func (discardEvent) Str(string, string) schemas.LogEventBuilder { return discardEvent{} }
func (discardEvent) Int(string, int) schemas.LogEventBuilder    { return discardEvent{} }
func (discardEvent) Int64(string, int64) schemas.LogEventBuilder {
	return discardEvent{}
}
func (discardEvent) Send() {}
