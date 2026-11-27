package circuitbreaker

import (
	"context"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

func ptr[T any](v T) *T { return &v }

func testPlugin(t *testing.T, cfg *Config, keys KeyLister) *Plugin {
	t.Helper()
	p, err := Init(cfg, nil, keys)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	return p
}

func chat(provider schemas.ModelProvider, model string, fallbacks ...schemas.Fallback) *schemas.BifrostRequest {
	return &schemas.BifrostRequest{
		RequestType: schemas.ChatCompletionRequest,
		ChatRequest: &schemas.BifrostChatRequest{
			Provider:  provider,
			Model:     model,
			Fallbacks: fallbacks,
		},
	}
}

func ctxWith(id string) *schemas.BifrostContext {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	ctx.SetValue(schemas.BifrostContextKeyRequestID, id)
	return ctx
}

func statusErr(status int, message, typ string) *schemas.BifrostError {
	err := &schemas.BifrostError{
		StatusCode: &status,
		Error:      &schemas.ErrorField{Message: message},
	}
	if typ != "" {
		err.Type = &typ
		err.Error.Type = &typ
	}
	return err
}

func connectionErr() *schemas.BifrostError {
	typ := schemas.ProviderConnectionFailed
	return &schemas.BifrostError{
		StatusCode: ptr(502),
		Type:       &typ,
		Error: &schemas.ErrorField{
			Message: schemas.ErrProviderDoRequest,
			Type:    &typ,
		},
	}
}

func timeoutErr() *schemas.BifrostError {
	typ := schemas.RequestTimedOut
	return &schemas.BifrostError{
		IsBifrostError: true,
		StatusCode:     ptr(504),
		Type:           &typ,
		Error:          &schemas.ErrorField{Message: "request timed out", Type: &typ},
	}
}

// round runs one attempt. It returns the error the breaker used to skip the
// attempt, or nil when the attempt was admitted. err is the upstream result
// recorded for an admitted attempt.
func (p *Plugin) round(ctx *schemas.BifrostContext, req *schemas.BifrostRequest, err *schemas.BifrostError) *schemas.BifrostError {
	_, sc, hookErr := p.PreLLMHook(ctx, req)
	if hookErr != nil {
		return statusErr(500, hookErr.Error(), "hook")
	}
	if sc != nil && sc.Error != nil {
		_, _, _ = p.PostLLMHook(ctx, nil, sc.Error)
		return sc.Error
	}
	_, _, _ = p.PostLLMHook(ctx, nil, err)
	return nil
}

func TestResolveDefaultsAndRejects(t *testing.T) {
	s, err := resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.failureThreshold != 2 || s.cooldown != 15*time.Second || s.slow != 10*time.Second || s.maxProbes != 1 || s.maxInFlight != 0 || s.countRateLimit {
		t.Fatalf("defaults = %+v", s)
	}
	if _, err := resolve(&Config{FailureThreshold: ptr(0)}); err == nil {
		t.Fatal("expected failure_threshold 0 to be rejected")
	}
	if _, err := resolve(&Config{SlowThresholdSeconds: ptr(-1)}); err == nil {
		t.Fatal("expected negative slow threshold to be rejected")
	}
	off, err := resolve(&Config{SlowThresholdSeconds: ptr(0), MaxInFlight: ptr(0), CountRateLimit: ptr(false)})
	if err != nil {
		t.Fatal(err)
	}
	if off.slow != 0 || off.maxInFlight != 0 || off.countRateLimit {
		t.Fatalf("explicit zeros = %+v", off)
	}
}

func TestClientErrorDoesNotOpen(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(1)}, nil)
	req := chat(schemas.OpenAI, "gpt-4o")
	for i := 0; i < 3; i++ {
		got := p.round(ctxWith("c"), req, statusErr(400, "bad request", "invalid_request_error"))
		if got != nil && got.StatusCode != nil && *got.StatusCode == 503 {
			t.Fatalf("client error opened the circuit: %+v", got.Error)
		}
	}
	if sc := p.round(ctxWith("c2"), req, nil); sc != nil {
		t.Fatalf("circuit blocked after client errors: %s", sc.Error.Message)
	}
}

func TestUpstreamErrorOpensAndReplays(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30)}, nil)
	req := chat(schemas.OpenAI, "gpt-4o")
	up := statusErr(500, "internal error", "server_error")
	if got := p.round(ctxWith("a"), req, up); got != nil {
		t.Fatalf("first failure blocked: %v", got.Error)
	}
	if got := p.round(ctxWith("b"), req, up); got != nil {
		t.Fatalf("second failure blocked: %v", got.Error)
	}
	got := p.round(ctxWith("c"), req, up)
	if got == nil || got.StatusCode == nil || *got.StatusCode != 500 || got.Error.Message != "internal error" {
		t.Fatalf("replay = %+v", got)
	}
	if got.Type != nil && *got.Type == "circuit_open" {
		t.Fatal("replayed a synthetic circuit_open error")
	}
	// A replayed skip must not extend the failure count.
	before := p.circuits[circuitID{scope: scopeModel, provider: schemas.OpenAI, model: "gpt-4o"}].fails
	_ = p.round(ctxWith("d"), req, up)
	after := p.circuits[circuitID{scope: scopeModel, provider: schemas.OpenAI, model: "gpt-4o"}].fails
	if after != before {
		t.Fatalf("replay changed fails from %d to %d", before, after)
	}
}

func TestConnectionFailureIsProviderScoped(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30)}, nil)
	a := chat(schemas.OpenAI, "model-a")
	if p.round(ctxWith("1"), a, connectionErr()) != nil || p.round(ctxWith("2"), a, connectionErr()) != nil {
		t.Fatal("opening failures should still be admitted")
	}
	if got := p.round(ctxWith("3"), chat(schemas.OpenAI, "model-b"), nil); got == nil || got.Error.Message != schemas.ErrProviderDoRequest {
		t.Fatalf("other model on the dead provider = %v", got)
	}
	if got := p.round(ctxWith("4"), chat(schemas.Groq, "model-a"), nil); got != nil {
		t.Fatalf("different provider was blocked: %v", got.Error)
	}
}

func TestTimeoutOpensModelOnly(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(1), CooldownSeconds: ptr(30)}, nil)
	if p.round(ctxWith("t"), chat(schemas.OpenAI, "slow"), timeoutErr()) != nil {
		t.Fatal("timeout should be admitted")
	}
	if got := p.round(ctxWith("t2"), chat(schemas.OpenAI, "slow"), nil); got == nil || got.StatusCode == nil || *got.StatusCode != 504 {
		t.Fatalf("same model = %v", got)
	}
	if got := p.round(ctxWith("t3"), chat(schemas.OpenAI, "other"), nil); got != nil {
		t.Fatalf("other model blocked by a timeout: %v", got.Error)
	}
}

func TestRateLimitIsPerKey(t *testing.T) {
	keys := []schemas.Key{
		{ID: "key-a", Models: schemas.WhiteList{"*"}, Weight: 1},
		{ID: "key-b", Models: schemas.WhiteList{"*"}, Weight: 1},
	}
	p := testPlugin(t, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30), CountRateLimit: ptr(true)}, func(schemas.ModelProvider) []schemas.Key {
		return keys
	})
	req := chat(schemas.OpenAI, "gpt-4o")
	limited := statusErr(429, "rate limit exceeded", "rate_limit_error")
	for _, id := range []string{"r1", "r2"} {
		ctx := ctxWith(id)
		ctx.SetValue(schemas.BifrostContextKeyAttemptTrail, []schemas.KeyAttemptRecord{{
			KeyID: "key-a", FailureClass: schemas.FailureClassRateLimit, StatusCode: ptr(429),
		}})
		if got := p.round(ctx, req, limited); got != nil {
			t.Fatalf("rate limit blocked the attempt: %v", got.Error)
		}
	}
	ctx := ctxWith("r3")
	if got := p.round(ctx, req, nil); got != nil {
		t.Fatalf("healthy sibling key should still be tried, got %s", got.Error.Message)
	}
	skip, _ := ctx.Value(schemas.BifrostContextKeyCircuitSkipKeyIDs).(map[string]struct{})
	if _, ok := skip["key-a"]; !ok {
		t.Fatalf("skip set = %v, want key-a", skip)
	}
	if _, ok := skip["key-b"]; ok {
		t.Fatal("key-b was skipped")
	}

	// Every serving key open: replay the upstream 429, do not invent one.
	for _, id := range []string{"r4", "r5"} {
		ctxB := ctxWith(id)
		ctxB.SetValue(schemas.BifrostContextKeyAttemptTrail, []schemas.KeyAttemptRecord{{
			KeyID: "key-b", FailureClass: schemas.FailureClassRateLimit, StatusCode: ptr(429),
		}})
		_ = p.round(ctxB, req, limited)
	}
	next := p.round(ctxWith("r6"), req, nil)
	if next == nil || next.StatusCode == nil || *next.StatusCode != 429 {
		t.Fatalf("all-open = %v, want the upstream 429", next)
	}
	if next.Type != nil && *next.Type == "circuit_open" {
		t.Fatal("all-open returned circuit_open")
	}
}

func TestRateLimitIgnoredByDefault(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(1)}, nil)
	req := chat(schemas.OpenAI, "gpt-4o")
	ctx := ctxWith("rl")
	ctx.SetValue(schemas.BifrostContextKeyAttemptTrail, []schemas.KeyAttemptRecord{{
		KeyID: "key-a", FailureClass: schemas.FailureClassRateLimit, StatusCode: ptr(429),
	}})
	if got := p.round(ctx, req, statusErr(429, "rate limit exceeded", "rate_limit_error")); got != nil {
		t.Fatalf("default config counted a 429: %v", got.Error)
	}
	if got := p.round(ctxWith("rl2"), req, nil); got != nil {
		t.Fatalf("429 opened a circuit: %v", got.Error)
	}
}

func TestHalfOpenAllowsOneProbe(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(1), CooldownSeconds: ptr(30)}, nil)
	start := time.Now()
	p.now = func() time.Time { return start }
	req := chat(schemas.OpenAI, "gpt-4o")
	if p.round(ctxWith("open"), req, statusErr(500, "internal error", "server_error")) != nil {
		t.Fatal("failure should be admitted")
	}
	if p.round(ctxWith("blocked"), req, nil) == nil {
		t.Fatal("expected the circuit to be open")
	}
	start = start.Add(30 * time.Second)
	first := ctxWith("probe")
	if _, sc, _ := p.PreLLMHook(first, req); sc != nil {
		t.Fatal("half-open probe was rejected")
	}
	second := ctxWith("probe-2")
	if _, sc, _ := p.PreLLMHook(second, req); sc == nil {
		t.Fatal("a second half-open probe was admitted")
	}
	_, _, _ = p.PostLLMHook(first, nil, nil)
	if got := p.round(ctxWith("recovered"), req, nil); got != nil {
		t.Fatalf("probe success did not close the circuit: %v", got.Error)
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(10)}, nil)
	start := time.Now()
	p.now = func() time.Time { return start }
	req := chat(schemas.OpenAI, "gpt-4o")
	up := statusErr(500, "internal error", "server_error")
	_ = p.round(ctxWith("1"), req, up)
	_ = p.round(ctxWith("2"), req, up)
	start = start.Add(10 * time.Second)
	probe := ctxWith("probe")
	if _, sc, _ := p.PreLLMHook(probe, req); sc != nil {
		t.Fatal("probe rejected")
	}
	_, _, _ = p.PostLLMHook(probe, nil, up)
	start = start.Add(time.Second)
	if got := p.round(ctxWith("still"), req, nil); got == nil {
		t.Fatal("failed probe left the circuit closed")
	}
}

func TestMaxInFlightCapsABurst(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(100), MaxInFlight: ptr(2), SlowThresholdSeconds: ptr(0)}, nil)
	req := chat(schemas.OpenAI, "gpt-4o")
	admitted := 0
	for i := 0; i < 10; i++ {
		_, sc, _ := p.PreLLMHook(ctxWith(string(rune('a'+i))), req)
		if sc == nil {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("admitted %d of a burst of 10, want 2", admitted)
	}
}

func TestSlowCallShedsLaterAttempts(t *testing.T) {
	p := testPlugin(t, &Config{
		FailureThreshold:     ptr(100),
		SlowThresholdSeconds: ptr(1),
		MaxInFlight:          ptr(0),
	}, nil)
	start := time.Now()
	p.now = func() time.Time { return start }
	req := chat(schemas.OpenAI, "gpt-4o")
	hang := ctxWith("hang")
	if _, sc, _ := p.PreLLMHook(hang, req); sc != nil {
		t.Fatal("first attempt rejected")
	}
	start = start.Add(time.Second)
	if _, sc, _ := p.PreLLMHook(ctxWith("later"), req); sc == nil {
		t.Fatal("attempt admitted while another call had produced no byte past the slow threshold")
	}
	// A byte from the in-flight call means the upstream is responding.
	hang.SetValue(schemas.BifrostContextKeyStreamStartTime, start)
	_, _, _ = p.PostLLMHook(hang, &schemas.BifrostResponse{}, nil)
	if _, sc, _ := p.PreLLMHook(ctxWith("after-byte"), req); sc != nil {
		t.Fatal("shed after the in-flight call had produced a byte")
	}
}

func TestCancellationDoesNotCount(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(1)}, nil)
	req := chat(schemas.OpenAI, "gpt-4o")
	typ := schemas.RequestCancelled
	cancel := &schemas.BifrostError{Type: &typ, Error: &schemas.ErrorField{Type: &typ, Message: "cancelled"}}
	if got := p.round(ctxWith("x"), req, cancel); got != nil {
		t.Fatalf("cancel blocked: %v", got.Error)
	}
	if got := p.round(ctxWith("y"), req, nil); got != nil {
		t.Fatalf("cancel opened the circuit: %v", got.Error)
	}
}

func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	p := testPlugin(t, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30)}, nil)
	req := chat(schemas.OpenAI, "gpt-4o")
	up := statusErr(500, "internal error", "server_error")
	_ = p.round(ctxWith("f"), req, up)
	_ = p.round(ctxWith("ok"), req, nil)
	_ = p.round(ctxWith("f2"), req, up)
	if got := p.round(ctxWith("still"), req, nil); got != nil {
		t.Fatalf("one failure after a success opened the circuit: %v", got.Error)
	}
}

func TestClassifyMatchesCore(t *testing.T) {
	if bifrost.ClassifyFailure(connectionErr()) != schemas.FailureClassTransient || !isConnection(connectionErr()) {
		t.Fatal("connection error was not classified as a transport failure")
	}
	if bifrost.ClassifyFailure(timeoutErr()) != schemas.FailureClassTimeout {
		t.Fatal("timeout was not classified as a timeout")
	}
	if bifrost.ClassifyFailure(statusErr(500, "internal error", "server_error")) != schemas.FailureClassTransient {
		t.Fatal("500 was not transient")
	}
	if bifrost.ClassifyFailure(statusErr(400, "bad request", "invalid_request_error")) != schemas.FailureClassCallerFault {
		t.Fatal("400 was not a caller fault")
	}
	if bifrost.ClassifyFailure(statusErr(429, "rate limit exceeded", "rate_limit_error")) != schemas.FailureClassRateLimit {
		t.Fatal("429 was not a rate limit")
	}
}

func BenchmarkAdmitHealthy(b *testing.B) {
	p, err := Init(nil, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	req := chat(schemas.OpenAI, "gpt-4o")
	ctx := ctxWith("bench")
	b.ReportAllocs()
	for b.Loop() {
		_, sc, _ := p.PreLLMHook(ctx, req)
		if sc != nil {
			b.Fatal("healthy admit blocked")
		}
		_, _, _ = p.PostLLMHook(ctx, nil, nil)
	}
}
