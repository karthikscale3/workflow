---
'@workflow/core': minor
'@workflow/world': minor
'@workflow/world-vercel': minor
'@workflow/world-local': minor
---

Lazy hook resumption: `resumeHook()` now persists the `hook_received` event and publishes the workflow invocation concurrently when the run supports it, cutting resume latency. A stable `resumeId` and payload digest tie the direct write to the queue consumer's idempotent re-ensure so both converge on exactly one event; the `resumeId` is persisted on the event so a consumer whose preload already contains the matching write skips the re-ensure entirely.

The fast path is taken only when two conditions are attested independently: the target consumer re-ensures from `hookInput` (a marker fixed per run — stamped at run start, and for a cross-deployment start carried over the health-check probe so an older target deployment fails closed), and the backend enforces the `(runId, resumeId)` constraint (re-evaluated fresh on every hook lookup). The Vercel World attests the backend per by-token lookup via a response-only `resumeCapabilities.hookResumeDedupVersion` (never persisted, never in `resumeContext`), so a server rollback or kill switch drops new resumes to the sequential path immediately with no stranded hooks; world-local keeps its static `hookResumeDedup` capability since its backend and adapter ship together. Runs or backends missing either attestation keep the sequential single-writer path.

Set `WORKFLOW_DISABLE_LAZY_HOOK_RESUME=1` on the SDK to force the sequential path. The chosen strategy is reported on the resume span as `workflow.hook.resume_strategy`, with `workflow.hook.resume_fallback_reason` naming why the sequential path was taken.
