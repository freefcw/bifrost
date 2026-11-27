package circuitbreaker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

const chatOK = `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"%s"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`

func okHandler(hits *atomic.Int32, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, chatOK, body)
	}
}

func statusHandler(hits *atomic.Int32, status int, message, typ string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"message":%q,"type":%q}}`, message, typ)
	}
}

// closeHandler accepts the request and drops the connection before a response
// byte, which is how a tunnel reset reaches the gateway.
func closeHandler(hits *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		_ = conn.Close()
	}
}

func hangHandler(hits *atomic.Int32, d time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
		}
	}
}

type memAccount struct {
	mu      sync.Mutex
	configs map[schemas.ModelProvider]*schemas.ProviderConfig
	keys    map[schemas.ModelProvider][]schemas.Key
}

func (a *memAccount) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]schemas.ModelProvider, 0, len(a.configs))
	for provider := range a.configs {
		out = append(out, provider)
	}
	return out, nil
}

func (a *memAccount) GetKeysForProvider(_ context.Context, provider schemas.ModelProvider) ([]schemas.Key, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	keys := a.keys[provider]
	if len(keys) == 0 {
		return nil, fmt.Errorf("no keys for %s", provider)
	}
	out := make([]schemas.Key, len(keys))
	copy(out, keys)
	return out, nil
}

func (a *memAccount) GetConfigForProvider(provider schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cfg := a.configs[provider]
	if cfg == nil {
		return nil, fmt.Errorf("no config for %s", provider)
	}
	copy := *cfg
	return &copy, nil
}

func (a *memAccount) GetConfiguredMCPClients() ([]string, error) { return nil, nil }

func (a *memAccount) GetConfigForMCPClient(string) (*schemas.MCPClientConfig, error) {
	return nil, fmt.Errorf("no mcp client")
}

func (a *memAccount) GetConfigForMCPClientByName(string) (*schemas.MCPClientConfig, error) {
	return nil, fmt.Errorf("no mcp client")
}

func (a *memAccount) listKeys(provider schemas.ModelProvider) []schemas.Key {
	keys, _ := a.GetKeysForProvider(context.Background(), provider)
	return keys
}

func providerConfig(baseURL string, timeoutSec int) *schemas.ProviderConfig {
	return &schemas.ProviderConfig{
		NetworkConfig: schemas.NetworkConfig{
			BaseURL:                        baseURL,
			AllowPrivateNetwork:            true,
			DefaultRequestTimeoutInSeconds: timeoutSec,
			MaxRetries:                     0,
		},
		ConcurrencyAndBufferSize: schemas.ConcurrencyAndBufferSize{Concurrency: 20, BufferSize: 64},
	}
}

func key(id, value string) schemas.Key {
	return schemas.Key{
		ID: id, Name: id, Value: *schemas.NewSecretVar(value),
		Models: schemas.WhiteList{"*"}, Weight: 1,
	}
}

type gateway struct {
	client *bifrost.Bifrost
	plugin *Plugin
}

func startGateway(t *testing.T, acc *memAccount, cfg *Config) *gateway {
	t.Helper()
	plugin := testPlugin(t, cfg, func(provider schemas.ModelProvider) []schemas.Key {
		return acc.listKeys(provider)
	})
	client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{
		Account:    acc,
		Logger:     bifrost.NewDefaultLogger(schemas.LogLevelError),
		LLMPlugins: []schemas.LLMPlugin{plugin},
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(client.Shutdown)
	return &gateway{client: client, plugin: plugin}
}

func (g *gateway) call(provider schemas.ModelProvider, model string, fallback schemas.Fallback, pin string) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(20*time.Second))
	if pin != "" {
		ctx.SetValue(schemas.BifrostContextKeyAPIKeyID, pin)
	}
	req := &schemas.BifrostChatRequest{
		Provider: provider,
		Model:    model,
		Input: []schemas.ChatMessage{{
			Role:    schemas.ChatMessageRoleUser,
			Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("hi")},
		}},
	}
	if fallback.Provider != "" {
		req.Fallbacks = []schemas.Fallback{fallback}
	}
	return g.client.ChatCompletionRequest(ctx, req)
}

func outcome(resp *schemas.BifrostChatResponse, err *schemas.BifrostError) string {
	if err != nil {
		msg := ""
		if err.Error != nil {
			msg = err.Error.Message
		}
		code := 0
		if err.StatusCode != nil {
			code = *err.StatusCode
		}
		typ := ""
		if err.Type != nil {
			typ = *err.Type
		} else if err.Error != nil && err.Error.Type != nil {
			typ = *err.Error.Type
		}
		return fmt.Sprintf("err %d %s %s", code, typ, msg)
	}
	if resp == nil || len(resp.Choices) == 0 ||
		resp.Choices[0].ChatNonStreamResponseChoice == nil ||
		resp.Choices[0].ChatNonStreamResponseChoice.Message == nil ||
		resp.Choices[0].ChatNonStreamResponseChoice.Message.Content == nil ||
		resp.Choices[0].ChatNonStreamResponseChoice.Message.Content.ContentStr == nil {
		return "empty"
	}
	return *resp.Choices[0].ChatNonStreamResponseChoice.Message.Content.ContentStr
}

func listen(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestUpstreamSkipsDeadPrimary(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := listen(t, closeHandler(&primaryHits))
	fallback := listen(t, okHandler(&fallbackHits, "from-fallback"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 5),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30), SlowThresholdSeconds: ptr(0)})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	for i := 0; i < 6; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		if got := outcome(resp, err); got != "from-fallback" {
			t.Fatalf("request %d = %s (primary hits %d)", i, got, primaryHits.Load())
		}
	}
	if got := primaryHits.Load(); got != 2 {
		t.Fatalf("dead primary hits = %d, want 2", got)
	}
	if got := fallbackHits.Load(); got != 6 {
		t.Fatalf("fallback hits = %d, want 6", got)
	}
}

func TestUpstreamSkipsHTTP500(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := listen(t, statusHandler(&primaryHits, 500, "internal error", "server_error"))
	fallback := listen(t, okHandler(&fallbackHits, "from-fallback"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 5),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30), SlowThresholdSeconds: ptr(0)})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	for i := 0; i < 6; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		if got := outcome(resp, err); got != "from-fallback" {
			t.Fatalf("request %d = %s", i, got)
		}
	}
	if got := primaryHits.Load(); got != 2 {
		t.Fatalf("500 primary hits = %d, want 2", got)
	}
}

func TestClientErrorStillReachesUpstream(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := listen(t, statusHandler(&primaryHits, 400, "bad request", "invalid_request_error"))
	fallback := listen(t, okHandler(&fallbackHits, "from-fallback"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 5),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{FailureThreshold: ptr(1), CooldownSeconds: ptr(30), SlowThresholdSeconds: ptr(0)})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	for i := 0; i < 4; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		if got := outcome(resp, err); got != "from-fallback" {
			t.Fatalf("request %d = %s", i, got)
		}
	}
	if got := primaryHits.Load(); got != 4 {
		t.Fatalf("client-error primary hits = %d, want every request to still try it", got)
	}
}

func TestHangingUpstreamShedsLaterRequests(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := listen(t, hangHandler(&primaryHits, 4*time.Second))
	fallback := listen(t, okHandler(&fallbackHits, "from-fallback"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 3),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{
		FailureThreshold:     ptr(5),
		CooldownSeconds:      ptr(30),
		SlowThresholdSeconds: ptr(1),
		MaxInFlight:          ptr(0),
	})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = g.call(schemas.OpenAI, "primary-model", fb, "")
	}()
	deadline := time.Now().Add(2 * time.Second)
	for primaryHits.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if primaryHits.Load() < 1 {
		t.Fatal("hanging request never reached the primary")
	}
	time.Sleep(1100 * time.Millisecond)
	for i := 0; i < 5; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		if got := outcome(resp, err); got != "from-fallback" {
			t.Fatalf("later request %d = %s", i, got)
		}
	}
	if got := primaryHits.Load(); got != 1 {
		t.Fatalf("primary hits while a call was already slow = %d, want 1", got)
	}
	<-done
}

func TestBurstIsCapped(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := listen(t, hangHandler(&primaryHits, 3*time.Second))
	fallback := listen(t, okHandler(&fallbackHits, "from-fallback"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 3),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{
		FailureThreshold:     ptr(50),
		SlowThresholdSeconds: ptr(0),
		MaxInFlight:          ptr(2),
	})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.call(schemas.OpenAI, "primary-model", fb, "")
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for primaryHits.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := primaryHits.Load(); got != 2 {
		t.Fatalf("burst primary hits = %d, want 2", got)
	}
	// Slots are still held by the hanging calls. A request that arrives now
	// must be served by the fallback.
	resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
	if got := outcome(resp, err); got != "from-fallback" {
		t.Fatalf("request while the burst is in flight = %s", got)
	}
	if got := primaryHits.Load(); got != 2 {
		t.Fatalf("primary hits while capped = %d, want 2", got)
	}
	wg.Wait()
}

func TestHalfOpenProbeRecovers(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	healthy := atomic.Bool{}
	primary := listen(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits.Add(1)
		if !healthy.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":{"message":"internal error","type":"server_error"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, chatOK, "from-primary")
	}))
	fallback := listen(t, okHandler(&fallbackHits, "from-fallback"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 5),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(1), SlowThresholdSeconds: ptr(0)})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	for i := 0; i < 2; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		if got := outcome(resp, err); got != "from-fallback" {
			t.Fatalf("opening request %d = %s", i, got)
		}
	}
	resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
	if got := outcome(resp, err); got != "from-fallback" {
		t.Fatalf("open-circuit request = %s", got)
	}
	if got := primaryHits.Load(); got != 2 {
		t.Fatalf("hits while open = %d, want 2", got)
	}
	time.Sleep(1100 * time.Millisecond)
	healthy.Store(true)
	resp, err = g.call(schemas.OpenAI, "primary-model", fb, "")
	if got := outcome(resp, err); got != "from-primary" {
		t.Fatalf("probe = %s, want the recovered primary", got)
	}
	resp, err = g.call(schemas.OpenAI, "primary-model", fb, "")
	if got := outcome(resp, err); got != "from-primary" {
		t.Fatalf("after probe = %s, want the primary to stay closed", got)
	}
	if got := primaryHits.Load(); got != 4 {
		t.Fatalf("primary hits = %d, want the two failures plus two recovered calls", got)
	}
}

func TestAllOpenReplaysUpstreamError(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := listen(t, statusHandler(&primaryHits, 500, "internal error", "server_error"))
	fallback := listen(t, statusHandler(&fallbackHits, 500, "internal error", "server_error"))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(primary.URL, 5),
			schemas.Groq:   providerConfig(fallback.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("p1", "sk-p1")},
			schemas.Groq:   {key("p2", "sk-p2")},
		},
	}
	g := startGateway(t, acc, &Config{FailureThreshold: ptr(2), CooldownSeconds: ptr(30), SlowThresholdSeconds: ptr(0)})
	fb := schemas.Fallback{Provider: schemas.Groq, Model: "fallback-model"}
	for i := 0; i < 2; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		got := outcome(resp, err)
		if err == nil || err.StatusCode == nil || *err.StatusCode != 500 || (err.Error != nil && err.Error.Message != "internal error") {
			t.Fatalf("opening request %d = %s", i, got)
		}
	}
	openedPrimary, openedFallback := primaryHits.Load(), fallbackHits.Load()
	for i := 0; i < 4; i++ {
		resp, err := g.call(schemas.OpenAI, "primary-model", fb, "")
		if err == nil || err.StatusCode == nil || *err.StatusCode != 500 {
			t.Fatalf("all-open request %d = %s", i, outcome(resp, err))
		}
		if err.Error == nil || err.Error.Message != "internal error" {
			t.Fatalf("all-open request %d message = %s", i, outcome(resp, err))
		}
		if err.Type != nil && *err.Type == "circuit_open" {
			t.Fatalf("all-open request %d returned circuit_open", i)
		}
		if err.Error.Type != nil && *err.Error.Type == "circuit_open" {
			t.Fatalf("all-open request %d error type circuit_open", i)
		}
	}
	if primaryHits.Load() != openedPrimary || fallbackHits.Load() != openedFallback {
		t.Fatalf("hits grew after both circuits opened: primary %d->%d fallback %d->%d",
			openedPrimary, primaryHits.Load(), openedFallback, fallbackHits.Load())
	}
}

func TestPerKeyIsolation(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	upstream := listen(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "Bearer sk-a" {
			hitsA.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}`)
			return
		}
		hitsB.Add(1)
		fmt.Fprintf(w, chatOK, "from-b")
	}))
	acc := &memAccount{
		configs: map[schemas.ModelProvider]*schemas.ProviderConfig{
			schemas.OpenAI: providerConfig(upstream.URL, 5),
		},
		keys: map[schemas.ModelProvider][]schemas.Key{
			schemas.OpenAI: {key("key-a", "sk-a"), key("key-b", "sk-b")},
		},
	}
	g := startGateway(t, acc, &Config{
		FailureThreshold:     ptr(2),
		CooldownSeconds:      ptr(30),
		SlowThresholdSeconds: ptr(0),
		CountRateLimit:       ptr(true),
	})
	for i := 0; i < 2; i++ {
		resp, err := g.call(schemas.OpenAI, "gpt-4o", schemas.Fallback{}, "key-a")
		if err == nil || err.StatusCode == nil || *err.StatusCode != 429 {
			t.Fatalf("pinned key-a request %d = %s", i, outcome(resp, err))
		}
	}
	resp, err := g.call(schemas.OpenAI, "gpt-4o", schemas.Fallback{}, "key-a")
	if err == nil || err.StatusCode == nil || *err.StatusCode != 429 {
		t.Fatalf("open key-a = %s", outcome(resp, err))
	}
	if got := hitsA.Load(); got != 2 {
		t.Fatalf("key-a hits after it opened = %d, want 2", got)
	}
	for i := 0; i < 5; i++ {
		resp, err = g.call(schemas.OpenAI, "gpt-4o", schemas.Fallback{}, "")
		if got := outcome(resp, err); got != "from-b" {
			t.Fatalf("unpinned request %d = %s", i, got)
		}
	}
	if got := hitsA.Load(); got != 2 {
		t.Fatalf("key-a hits = %d, want it to stay skipped", got)
	}
	if got := hitsB.Load(); got != 5 {
		t.Fatalf("key-b hits = %d, want 5", got)
	}
}
