import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  ThrottleError,
} from '@workflow/errors';
import {
  type Hook,
  SPEC_VERSION_CURRENT,
  SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT,
  type World,
} from '@workflow/world';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { dehydrateStepReturnValue } from '../serialization.js';
import { resumeHook } from './resume-hook.js';
import { setWorld } from './world.js';

vi.mock('@vercel/functions', () => ({ waitUntil: vi.fn() }));
vi.mock('../telemetry.js', () => ({
  linkToTraceCarrier: vi.fn(),
  trace: vi.fn((_name, fn) => fn(undefined)),
}));
// Return raw bytes from dehydration so `dehydratedPayload instanceof Uint8Array`
// is true and the parallel resume strategy activates. The sibling
// `resume-hook.fast-path.test.ts` returns a string and thus stays sequential;
// this file exercises the complementary parallel branch.
const PAYLOAD_BYTES = new Uint8Array([1, 2, 3, 4]);
vi.mock('../serialization.js', async (importActual) => {
  const actual = await importActual<typeof import('../serialization.js')>();
  return {
    ...actual,
    dehydrateStepReturnValue: vi.fn(async () => PAYLOAD_BYTES),
    hydrateStepArguments: vi.fn(async (value: unknown) => value),
  };
});

describe('resumeHook (parallel fast path)', () => {
  afterEach(() => setWorld(undefined));

  const baseHook = {
    runId: 'wrun_par',
    hookId: 'hook_par',
    token: 'order:par',
    ownerId: 'owner_1',
    projectId: 'project_1',
    environment: 'production',
    createdAt: new Date(),
  } satisfies Hook;

  // workflowCoreVersion 5.0.0 >= 5.0.0-beta.39 minVersion, so
  // supportsHookResumeInput is on. Combined with a CBOR-transport spec version
  // and raw-byte payload, resumeHook takes the parallel path.
  const parallelContext = {
    deploymentId: 'deployment_par',
    workflowName: 'processOrder',
    runSpecVersion: SPEC_VERSION_CURRENT,
    workflowCoreVersion: '5.0.0',
  };

  const makeWorld = (
    hook: Hook,
    overrides: Partial<Record<string, ReturnType<typeof vi.fn>>> = {}
  ) => {
    const createEvent = overrides.createEvent ?? vi.fn();
    const queue = overrides.queue ?? vi.fn();
    setWorld({
      specVersion: SPEC_VERSION_CURRENT,
      hooks: { getByToken: vi.fn().mockResolvedValue(hook) },
      runs: { get: vi.fn() },
      events: { create: createEvent },
      getEncryptionKeyForRun: vi.fn().mockResolvedValue(undefined),
      queue,
    } as unknown as World);
    return { createEvent, queue };
  };

  it('dispatches the event write and queue publish concurrently with a shared resumeId + digest', async () => {
    const hook = { ...baseHook, resumeContext: parallelContext } satisfies Hook;
    const { createEvent, queue } = makeWorld(hook);

    await resumeHook(hook.token, { foo: 'bar' });

    expect(createEvent).toHaveBeenCalledTimes(1);
    const [runIdArg, eventArg, optsArg] = createEvent.mock.calls[0];
    expect(runIdArg).toBe(hook.runId);
    expect(eventArg).toMatchObject({
      eventType: 'hook_received',
      correlationId: hook.hookId,
    });
    // Both writers must carry the same idempotency key and content digest.
    const resumeId = optsArg.resumeId as string;
    const digest = optsArg.resumePayloadDigest as string;
    expect(resumeId).toEqual(expect.any(String));
    expect(digest).toMatch(/^[0-9a-f]{64}$/);

    expect(queue).toHaveBeenCalledTimes(1);
    const [, payloadArg] = queue.mock.calls[0];
    expect(payloadArg.runId).toBe(hook.runId);
    expect(payloadArg.hookInput).toEqual({
      resumeId,
      hookId: hook.hookId,
      token: hook.token,
      payload: PAYLOAD_BYTES,
      payloadDigest: digest,
    });
  });

  it('always throws when the queue publish fails', async () => {
    const hook = { ...baseHook, resumeContext: parallelContext } satisfies Hook;
    const queueErr = new Error('queue unavailable');
    const { queue } = makeWorld(hook, {
      queue: vi.fn().mockRejectedValue(queueErr),
    });

    await expect(resumeHook(hook.token, { foo: 'bar' })).rejects.toBe(queueErr);
    expect(queue).toHaveBeenCalledTimes(1);
  });

  it('swallows a retryable event-write failure because the queue consumer re-ensures the event', async () => {
    // 429/5xx/transport on the direct write is resilient: the run WAS
    // re-triggered via the queue, whose consumer idempotently re-ensures the
    // hook_received event before replay. resumeHook must not fail the caller.
    const hook = { ...baseHook, resumeContext: parallelContext } satisfies Hook;
    const createEvent = vi
      .fn()
      .mockRejectedValue(new ThrottleError('slow down'));
    const queue = vi.fn().mockResolvedValue({ messageId: 'm_1' });
    makeWorld(hook, { createEvent, queue });

    await expect(resumeHook(hook.token, { foo: 'bar' })).resolves.toMatchObject(
      { hookId: hook.hookId }
    );
    expect(createEvent).toHaveBeenCalledTimes(1);
    expect(queue).toHaveBeenCalledTimes(1);
  });

  it('re-keys a terminal-run rejection from the event write to HookNotFoundError(token)', async () => {
    // The queue publish succeeds, but the run has genuinely ended: the direct
    // write rejects with a terminal "hook gone" error and resumeHook surfaces
    // the pre-fast-path contract (HookNotFoundError keyed on the token). The
    // queue consumer's re-ensure will also no-op against the terminal run.
    for (const err of [
      new HookNotFoundError(baseHook.hookId),
      new RunExpiredError('run has expired'),
    ]) {
      const hook = {
        ...baseHook,
        resumeContext: parallelContext,
      } satisfies Hook;
      const createEvent = vi.fn().mockRejectedValue(err);
      const queue = vi.fn().mockResolvedValue({ messageId: 'm_1' });
      makeWorld(hook, { createEvent, queue });

      await expect(resumeHook(hook.token, { foo: 'bar' })).rejects.toSatisfy(
        (e: unknown) =>
          HookNotFoundError.is(e) &&
          (e as HookNotFoundError).token === hook.token
      );
      setWorld(undefined);
    }
  });

  it('swallows an EntityConflict (409) from the event write on the parallel path', async () => {
    // Unlike the sequential path, a 409 here is NOT "hook gone": the parallel
    // write raced its own re-ensuring queue consumer (or a redrive) on the
    // shared resumeId. The run was re-triggered via the queue, whose consumer
    // converges on the single committed event, so resumeHook must resolve
    // rather than re-key to HookNotFoundError.
    const hook = { ...baseHook, resumeContext: parallelContext } satisfies Hook;
    const createEvent = vi
      .fn()
      .mockRejectedValue(new EntityConflictError('resumeId already claimed'));
    const queue = vi.fn().mockResolvedValue({ messageId: 'm_1' });
    makeWorld(hook, { createEvent, queue });

    await expect(resumeHook(hook.token, { foo: 'bar' })).resolves.toMatchObject(
      { hookId: hook.hookId }
    );
    expect(createEvent).toHaveBeenCalledTimes(1);
    expect(queue).toHaveBeenCalledTimes(1);
  });

  it('falls back to the sequential path when the payload exceeds the inline queue bound', async () => {
    // A payload larger than the queue's inline ceiling would fail the oversized
    // publish on the parallel path, persisting hook_received but never
    // re-triggering the run. The size gate must instead select the sequential
    // path, whose queue message carries only the run ID (no resumeId / no
    // hookInput) — the payload rides the event log.
    const oversized = new Uint8Array(256 * 1024).fill(7);
    vi.mocked(dehydrateStepReturnValue).mockResolvedValueOnce(oversized);
    const hook = { ...baseHook, resumeContext: parallelContext } satisfies Hook;
    const { createEvent, queue } = makeWorld(hook);

    await resumeHook(hook.token, { foo: 'bar' });

    expect(createEvent).toHaveBeenCalledTimes(1);
    const [, , optsArg] = createEvent.mock.calls[0];
    expect(optsArg.resumeId).toBeUndefined();
    expect(optsArg.resumePayloadDigest).toBeUndefined();

    expect(queue).toHaveBeenCalledTimes(1);
    const [, payloadArg] = queue.mock.calls[0];
    expect(payloadArg.hookInput).toBeUndefined();
  });

  it('falls back to the sequential path when the target deployment lacks hookInput support', async () => {
    // Older core version → supportsHookResumeInput is false. Even with raw-byte
    // payloads and CBOR transport, resumeHook writes then publishes and carries
    // neither resumeId nor hookInput.
    expect(SPEC_VERSION_CURRENT).toBeGreaterThanOrEqual(
      SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT
    );
    const hook = {
      ...baseHook,
      resumeContext: {
        ...parallelContext,
        workflowCoreVersion: '5.0.0-beta.38',
      },
    } satisfies Hook;
    const { createEvent, queue } = makeWorld(hook);

    await resumeHook(hook.token, { foo: 'bar' });

    expect(createEvent).toHaveBeenCalledTimes(1);
    const [, , optsArg] = createEvent.mock.calls[0];
    expect(optsArg.resumeId).toBeUndefined();
    expect(optsArg.resumePayloadDigest).toBeUndefined();

    expect(queue).toHaveBeenCalledTimes(1);
    const [, payloadArg] = queue.mock.calls[0];
    expect(payloadArg.hookInput).toBeUndefined();
  });
});
