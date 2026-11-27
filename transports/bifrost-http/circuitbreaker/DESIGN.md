# 上游熔断（circuit breaker）

面向 OSS Bifrost 的可选插件：跟踪上游是否还能用，并在失败达到阈值后跳过它，让请求走已经配置好的 fallback。插件默认不加载。不启用时，请求路径与原来一致（key 选择里多一次找不到值的 context 读取）。

企业版 `circuit_breaker_config` 不适合这件事。那套策略看的是响应头，并且自己指定 fallback 的 provider/model。这里要的是：连接被掐断、超时、5xx 时，别再把每次请求都打到已经挂掉的上游；429 可以按 key 隔离；调用方自己配的 fallback 链保持不变。所以配置写在 `plugins`，不读、不写 `circuit_breaker_config`。

## 状态机

每个熔断器有三种状态：

- **closed**：正常放行。连续失败次数达到 `failure_threshold` 后变为 open。
- **open**：在 `cooldown_seconds` 之内拒绝新的尝试，直接把**上一次真实上游错误**交回去（状态码、type、message），并允许 fallback。不会返回自己编的 `circuit_open`，除非还没有任何一次失败落袋（请求还挂在路上，见下面的并发）。
- **half-open**：冷却结束。provider 级和 model 级只放行 **1** 次探测（`max_probes` 只作用于 model 的 half-open 和“已经不健康”时的在途上限）。探测成功则关闭；探测再失败则立刻重新 open 一整段冷却。探测如果一直不返回，过一个 cooldown 后允许下一次探测。

成功会把对应熔断器的失败计数清零。取消请求不计数。插件自己回放的错误也不再计数，避免冷却被不断往后推。

## 粒度

| 失败 | 熔断范围 | 原因 |
| --- | --- | --- |
| 连接失败（`provider_connection_failed`，对端在第一个响应字节之前把连接关掉） | 整个 provider | 同一条上游链路，换 key 或换 model 也到不了 |
| 超时、5xx | provider + model | 这个模型的端点没有在时限内给出可用响应；同一 provider 上的其他 model 不受影响 |
| 429（仅当 `count_rate_limit: true`） | provider + model + key | 限流通常打在这一把 key 上，其他 key 继续用 |
| 4xx（参数、鉴权、配额、模型不存在等）、取消、无法归类的错误 | 不计数 | 不是上游宕机 |

Key 选择发生在 `PreLLMHook` 之后。插件在 pre-hook 里把已经 open 的 key id 写进 `bifrost-circuit-skip-key-ids`，core 的 key 选择会跳过它们。调用方显式 pin 的 key 不会被静默换成另一把：那把 key 自己 open 时，这次尝试直接回放它的 429（或上次的错误），再走 fallback 链。若配置里每一把还能服务该 model 的 key 都 open，则整次尝试回放其中一把的真实错误，而不是再去打上游。

## 并发

健康且没有在途“无响应字节”的调用时，不限制并发（`max_inflight` 默认为 0）。

下面两种情况把**新的**尝试限制到 `max_probes`（默认 1）次在途：

- 已经有过一次计数失败，但还没到打开阈值；
- 有调用超过 `slow_threshold_seconds`（默认 10 秒）仍没有任何响应字节。流式响应一旦出了第一个 chunk，就不再算挂起。

`max_inflight` 是硬上限，健康时也生效。一次故障刚开始、所有请求同时发出、失败还没返回、也还没越过 slow threshold 时，只有这个上限能挡住它们。不设置的话，这一波仍会打到上游——在失败返回之前，熔断器无从知道上游已经死了。后续在挂起期间到达的请求会被 slow threshold 挡下。

## 配置

```json
{
  "plugins": [
    {
      "name": "circuitbreaker",
      "enabled": true,
      "config": {
        "failure_threshold": 2,
        "cooldown_seconds": 15,
        "slow_threshold_seconds": 10,
        "max_inflight": 0,
        "max_probes": 1,
        "count_rate_limit": false
      }
    }
  ]
}
```

省略的字段用默认值。`slow_threshold_seconds: 0` 关闭慢调用判断。`max_inflight: 0` 表示不封顶。`count_rate_limit` 默认 false。重新加载插件会新建状态，已打开的熔断器会清空。

插件排在 governance、routing 和 semantic cache 之后的 builtin 组里。缓存命中不会被当成上游成功；未通过鉴权的请求会先被拒绝。被跳过的尝试不会打到上游。fallback 那一次会完整走过后面的插件。

## 限制

- 不提供 Prometheus 指标，只在打开、关闭和探测时打 info 日志。
- 不改 UI。企业版的 Circuit Breaker 页面是另一套基于响应头的策略。
- 半开探测：provider 级始终只有 1 个；key 级在冷却结束后重新进入选择池，不单飞。429 很快，即使冷却后有一小撮打到同一把 key，也会马上再次打开。
- 超时按 model 计，不按整个 provider 计。一个 model 挂了不会挡住同 provider 的其他 model；若整台机器都不响应，每个 model 要各自越过 slow threshold 或失败阈值。
- 连接失败按整个 provider 计。
- 跳过时回放的是打开熔断器的那次错误，冷却期间不会更新，直到探测。
- 在第一次失败还没返回、又没有 `max_inflight` 时，同时发出的那一波请求仍会到达上游。

## 请求路径上的成本

插件未启用：不在插件链上。key 选择多一次 context 查找，找不到就返回，没有分配。

插件启用且上游健康：pre-hook 拿一把互斥锁，查 map，在该 model 的在途列表上追加一条再放行。列表容量涨到并发高水位之后，稳态不再分配。没有 key 处于 open 时，不会去拉 key 列表。

---

# Upstream circuit breaker

Opt-in plugin for OSS Bifrost. It tracks whether an upstream is usable and, after enough failures, skips it so the request's own fallback chain runs. The plugin is not loaded unless configured. With it absent, the request path is unchanged aside from one context lookup in key selection that finds nothing.

The enterprise `circuit_breaker_config` does not fit. Those policies watch response headers and name their own fallback provider and model. This plugin is for connection drops, timeouts and 5xx, with optional per-key 429s, on the fallback chain the caller already configured. It is configured under `plugins` and does not read or write `circuit_breaker_config`.

## State machine

- **closed**. Requests pass. `failure_threshold` consecutive counted failures open it.
- **open**. For `cooldown_seconds`, new attempts are not sent. The breaker returns the **last real upstream error** (status, type, message) with fallbacks allowed. It does not invent a `circuit_open` error unless no failure has been stored yet (the calls are still in flight; see concurrency).
- **half-open**. After the cooldown, the provider-level and model-level circuits allow one probe (`max_probes` applies to the model half-open window and to the in-flight cap once the circuit is already degraded). Success closes the circuit. A failed probe opens it again for a full cooldown. A probe that never returns stops blocking new probes after one cooldown.

A success clears that circuit's failure count. Cancellation is not a failure. An error the breaker itself replayed is not counted again, so the cooldown is not pushed out by skips.

## Granularity

| Failure | Circuit | Why |
| --- | --- | --- |
| Connection failure (`provider_connection_failed`, peer closed before the first response byte) | whole provider | the same path is dead for every key and model |
| Timeout, 5xx | provider + model | this model's endpoint did not answer in time; other models on the provider are left alone |
| 429, only when `count_rate_limit` is true | provider + model + key | the limit is on that key; the others keep serving |
| 4xx (bad request, auth, quota, unknown model, …), cancellation, unrecognised errors | not counted | not an upstream outage |

Key selection runs after `PreLLMHook`. The plugin puts open key ids on `bifrost-circuit-skip-key-ids`, and core skips those keys. A key the caller pinned is not silently replaced: if that key's circuit is open, the attempt replays its last error and the fallback chain runs. When every configured key that can serve the model is open, the attempt replays one of those real errors and does not call the upstream.

## Concurrency

A healthy upstream with no byte-less call in flight has no concurrency cap (`max_inflight` defaults to 0).

New attempts are held to `max_probes` in flight (default 1) when either:

- a counted failure has been recorded but the threshold is not reached yet, or
- a call has produced no response byte for `slow_threshold_seconds` (default 10). A stream stops counting as hung once its first chunk arrives.

`max_inflight` is a hard cap that applies even while the upstream looks healthy. It is the only limit on a burst that starts at the same moment the outage starts, before any failure returns and before the slow threshold. Without it, that first wave still reaches the upstream: the breaker cannot know the upstream is dead until a call comes back. Requests that arrive while a call is already hung are shed by the slow threshold.

## Config

```json
{
  "plugins": [
    {
      "name": "circuitbreaker",
      "enabled": true,
      "config": {
        "failure_threshold": 2,
        "cooldown_seconds": 15,
        "slow_threshold_seconds": 10,
        "max_inflight": 0,
        "max_probes": 1,
        "count_rate_limit": false
      }
    }
  ]
}
```

Omitted fields use the defaults. `slow_threshold_seconds: 0` disables slow-call shedding. `max_inflight: 0` means no cap. `count_rate_limit` defaults to false. Reloading the plugin builds a new breaker and drops open state.

The plugin runs in the builtin group after governance, routing and the semantic cache. A cache hit is not an upstream success, and a request that fails auth is rejected first. A skipped attempt is not sent upstream. The fallback attempt runs the rest of the plugin pipeline.

## Limitations

- No Prometheus metrics. Open, close and probe are info logs.
- No UI. The enterprise Circuit Breaker page is the header-policy feature.
- Half-open probes: the provider circuit is always single-flight. A key circuit re-enters the pool when its cooldown ends and is not single-flight. A 429 is fast, so a small burst after cooldown reopens that key immediately.
- Timeouts are per model, not per provider. One hung model does not block the provider's other models. If the whole host stops answering, each model crosses the slow threshold or the failure threshold on its own.
- Connection failures cover the whole provider.
- A skip replays the error that opened the circuit. It is not refreshed until the next probe.
- Without `max_inflight`, a simultaneous burst that leaves before the first failure returns still reaches the upstream.

## Cost on the request path

Plugin disabled: not in the plugin pipeline. Key selection does one extra context lookup, finds nothing, and allocates nothing.

Plugin enabled, upstream healthy: the pre-hook takes one mutex, looks up the map, and appends one in-flight mark for that model. After the slice has grown to the concurrency high-water mark, the steady state does not allocate. The key list is not read unless some key circuit is open.
