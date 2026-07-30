---
'@workflow/core': minor
'@workflow/world': minor
'@workflow/world-vercel': minor
'@workflow/world-local': minor
---

Lazy hook resumption: `resumeHook()` now persists the `hook_received` event and publishes the workflow invocation concurrently when the target deployment supports it, cutting resume latency. A stable `resumeId` and payload digest tie the direct write to the queue consumer's idempotent re-ensure so both converge on one event; gated behind `supportsHookResumeInput`, with unsupported deployments keeping the sequential path.
