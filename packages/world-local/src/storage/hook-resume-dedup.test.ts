import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { SPEC_VERSION_CURRENT, type Storage } from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createHook, createRun } from '../test-helpers.js';
import { createStorage } from './index.js';

// Once `supportsHookResumeInput` auto-enables on new clients, the parallel
// resume path has TWO writers of the same `hook_received`: the direct
// `events.create` and the queue consumer's re-ensure. Both carry the same
// `resumeId`. world-local (the dev backend) must converge them onto exactly
// one event — mirroring the server's `(runId, resumeId)` constraint — or a
// dev run would replay a duplicated hook_received.
describe('world-local hook_received resume dedup', () => {
  let testDir: string;
  let storage: Storage;

  beforeEach(async () => {
    testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'hook-resume-test-'));
    storage = createStorage(testDir);
  });

  afterEach(async () => {
    await fs.rm(testDir, { recursive: true, force: true });
  });

  async function setup() {
    const run = await createRun(storage, {
      deploymentId: 'dpl_test',
      workflowName: 'test-workflow',
      input: new Uint8Array(),
    });
    const hook = await createHook(storage, run.runId, {
      hookId: 'hook_1',
      token: 'order:1',
    });
    return { runId: run.runId, hook };
  }

  async function countHookReceived(runId: string): Promise<number> {
    const { data } = await storage.events.list({ runId });
    return data.filter((e) => e.eventType === 'hook_received').length;
  }

  it('collapses two resumeId-bearing writes onto a single event', async () => {
    const { runId, hook } = await setup();

    const first = await storage.events.create(
      runId,
      {
        eventType: 'hook_received',
        specVersion: SPEC_VERSION_CURRENT,
        correlationId: hook.hookId,
        eventData: { token: hook.token, payload: new Uint8Array([1, 2, 3]) },
      },
      { resumeId: 'resume_1', resumePayloadDigest: 'digest_1' }
    );

    const second = await storage.events.create(
      runId,
      {
        eventType: 'hook_received',
        specVersion: SPEC_VERSION_CURRENT,
        correlationId: hook.hookId,
        eventData: { token: hook.token, payload: new Uint8Array([1, 2, 3]) },
      },
      { resumeId: 'resume_2', resumePayloadDigest: 'digest_1' }
    );

    // The second write returns the already-stored event, and the log holds one.
    expect(second.event.eventId).toBe(first.event.eventId);
    expect(await countHookReceived(runId)).toBe(1);
  });
});
