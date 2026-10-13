import { assert, describe, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type AgentSessionImportSource,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const workspaceRoot = "/workspace/project";
const codexInstance = ProviderInstanceId.make("codex");
const codexDriver = ProviderDriverKind.make("codex");
const sessionId = "019a0000-0000-7000-8000-000000000001";
const importedThreadId = ThreadId.make(`import:${codexInstance}:${sessionId}`);
const importInput = {
  projectId,
  provider: "codex",
  providerInstanceId: codexInstance,
  providerSessionId: sessionId,
} as const;

const source: AgentSessionImportSource = {
  provider: "codex",
  providerInstanceId: codexInstance,
  providerSessionId: sessionId,
  filePath: `/home/.codex/sessions/2026/09/01/rollout-2026-09-01T10-00-00-${sessionId}.jsonl`,
  size: 100,
  mtimeMs: 2,
  device: 3,
  inode: 4,
  birthtimeMs: 1,
};

const thread: AgentSessionScanner.AgentSessionThread = {
  source: "codex",
  providerInstanceId: codexInstance,
  providerSessionId: sessionId,
  title: "Fix the flaky test",
  model: "gpt-5.4",
  createdAt: "2026-09-01T10:00:00.000Z",
  updatedAt: "2026-09-01T10:01:00.000Z",
  messages: [
    { role: "user", text: "Fix it", createdAt: "2026-09-01T10:00:00.000Z" },
    { role: "assistant", text: "Fixed", createdAt: "2026-09-01T10:01:00.000Z" },
  ],
};

function makeLayer(reads: { count: number }) {
  const scanner = AgentSessionScanner.AgentSessionScanner.of({
    scan: Effect.die("unused"),
    recentThreads: () => Stream.succeed({ _tag: "Importable", thread, source }),
    listSessions: () =>
      Effect.succeed({
        sessions: [
          {
            source: "codex",
            providerInstanceId: codexInstance,
            providerSessionId: sessionId,
            title: thread.title,
            model: thread.model,
            lastActiveAt: thread.updatedAt,
          },
        ],
        truncated: false,
      }),
    readSession: (input) =>
      Effect.sync(() => {
        reads.count += 1;
        return input.providerInstanceId === codexInstance && input.providerSessionId === sessionId
          ? { thread, source }
          : null;
      }),
  });
  const layerDatabase = SqlitePersistence.layerMemory;
  const layerStores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
    Layer.provideMerge(layerDatabase),
  );
  // The importer only reads thread records through the orchestrator.
  const layerOrchestrator = Layer.effect(
    Orchestrator.OrchestratorV2,
    Effect.map(
      ProjectionStore.ProjectionStoreV2,
      (store) =>
        ({
          getThreadRecords: store.getThreadRecords,
        }) as unknown as Orchestrator.OrchestratorV2["Service"],
    ),
  ).pipe(Layer.provide(layerStores));
  return AgentSessionImporter.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        layerStores,
        EventSink.layer.pipe(Layer.provide(layerStores)),
        layerOrchestrator,
        Layer.succeed(AgentSessionScanner.AgentSessionScanner, scanner),
        Layer.mock(ProjectService.ProjectService)({
          getById: () => Effect.succeed(Option.some({ id: projectId, workspaceRoot } as never)),
        }),
        IdAllocator.layer,
        ProviderSessionRuntime.layer.pipe(Layer.provide(layerDatabase)),
        ThreadCommandExecutor.layer,
      ),
    ),
  );
}

/** A thread bound to `nativeId` the way a T3-started thread is, with a non-derived row id. */
const seedThread = Effect.fn("seedThread")(function* (input: {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly nativeId: string;
  readonly deleted?: boolean;
  readonly legacyImport?: boolean;
}) {
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const at = DateTime.makeUnsafe("2026-09-02T10:00:00.000Z");
  const providerThreadId = idAllocator.derive.providerThread({
    driver: codexDriver,
    nativeThreadId: input.legacyImport === true ? input.nativeId : `pending:${input.threadId}`,
  });
  const appThread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: input.threadId,
    projectId,
    title: "Existing thread",
    providerInstanceId: input.providerInstanceId,
    modelSelection: { instanceId: input.providerInstanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    linkedPullRequest: null,
    branchPullRequest: null,
    activeProviderThreadId: providerThreadId,
    ...(input.legacyImport === true ? { historyOrigin: "v1_import" as const } : {}),
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: input.threadId },
    forkedFrom: null,
    createdAt: at,
    updatedAt: at,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    unsettledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    pinnedAt: null,
    pinOrderKey: null,
    activeOrderKey: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    driver: codexDriver,
    providerInstanceId: input.providerInstanceId,
    providerSessionId: null,
    appThreadId: input.threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver: codexDriver, nativeId: input.nativeId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: at,
    updatedAt: at,
  };
  yield* eventSink.write({
    events: [
      {
        id: EventId.make(`seed:${input.threadId}:created`),
        type: "thread.created",
        threadId: input.threadId,
        providerInstanceId: input.providerInstanceId,
        occurredAt: at,
        payload: appThread,
      },
      {
        id: EventId.make(`seed:${input.threadId}:provider-thread`),
        type: "provider-thread.updated",
        threadId: input.threadId,
        driver: codexDriver,
        providerInstanceId: input.providerInstanceId,
        occurredAt: at,
        payload: providerThread,
      },
      ...(input.deleted === true
        ? [
            {
              id: EventId.make(`seed:${input.threadId}:deleted`),
              type: "thread.deleted" as const,
              threadId: input.threadId,
              providerInstanceId: input.providerInstanceId,
              occurredAt: at,
              payload: { ...appThread, deletedAt: at },
            },
          ]
        : []),
    ],
  });
  return providerThread;
});

const activeProviderThread = Effect.fn("activeProviderThread")(function* (threadId: ThreadId) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const records = yield* store.getThreadRecords(threadId, ["providerThreads", "messages"]);
  const providerThread = records.providerThreads.find(
    (candidate) => candidate.id === records.thread.activeProviderThreadId,
  );
  assert.isDefined(providerThread);
  return { records, providerThread: providerThread! };
});

describe("AgentSessionImporter", () => {
  it.effect("imports messages once and preserves the provider native resume binding", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      assert.deepEqual(yield* importer.importRecentAgentThreads({ projectId }), {
        importedCount: 1,
        skippedCount: 0,
      });
      assert.deepEqual(yield* importer.importRecentAgentThreads({ projectId }), {
        importedCount: 1,
        skippedCount: 0,
      });

      const { records, providerThread } = yield* activeProviderThread(importedThreadId);
      assert.equal(records.thread.historyOrigin, "v1_import");
      assert.deepEqual(
        records.messages.map((message) => message.text),
        ["Fix it", "Fixed"],
      );
      assert.deepEqual(providerThread.nativeThreadRef, {
        driver: codexDriver,
        nativeId: sessionId,
        strength: "strong",
      });
      assert.equal(providerThread.nativeMetadata?.importedNativeId, sessionId);
      assert.equal(providerThread.lastRunOrdinal, null);
      const runtime = yield* runtimes.getByThreadId({ threadId: importedThreadId });
      assert.deepEqual(
        Option.map(runtime, (row) => row.resumeCursor),
        Option.some({ threadId: sessionId }),
      );
    }).pipe(Effect.provide(makeLayer(reads)));
  });

  it.effect("does not bulk import a session that a T3 thread already runs", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      yield* seedThread({
        threadId: ThreadId.make("thread-started-in-t3"),
        providerInstanceId: codexInstance,
        nativeId: sessionId,
      });
      assert.deepEqual(yield* importer.importRecentAgentThreads({ projectId }), {
        importedCount: 0,
        skippedCount: 1,
      });
      const store = yield* ProjectionStore.ProjectionStoreV2;
      assert.isNull(yield* store.getThreadShell(importedThreadId));
    }).pipe(Effect.provide(makeLayer(reads)));
  });

  it.effect("imports a selected session once and reopens it afterwards", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      assert.deepEqual(yield* importer.importSession(importInput), {
        threadId: importedThreadId,
        imported: true,
      });
      assert.deepEqual(yield* importer.importSession(importInput), {
        threadId: importedThreadId,
        imported: false,
      });
      assert.equal(reads.count, 1);
      const listed = yield* importer.listSessions({ projectId });
      assert.deepEqual(listed.sessions[0]?.thread, { id: importedThreadId, archived: false });
      const { providerThread } = yield* activeProviderThread(importedThreadId);
      // Scoped by instance so another account's import can never share the row.
      assert.equal(
        providerThread.id,
        (yield* IdAllocator.IdAllocatorV2).derive.providerThread({
          driver: codexDriver,
          providerInstanceId: codexInstance,
          nativeThreadId: sessionId,
        }),
      );
    }).pipe(Effect.provide(makeLayer(reads)));
  });

  it.effect("opens the T3 thread that already runs a session instead of importing it", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      const owner = ThreadId.make("thread-started-in-t3");
      yield* seedThread({
        threadId: owner,
        providerInstanceId: codexInstance,
        nativeId: sessionId,
      });

      const listed = yield* importer.listSessions({ projectId });
      assert.deepEqual(listed.sessions[0]?.thread, { id: owner, archived: false });
      assert.deepEqual(yield* importer.importSession(importInput), {
        threadId: owner,
        imported: false,
      });
      assert.equal(reads.count, 0);
    }).pipe(Effect.provide(makeLayer(reads)));
  });

  it.effect("refuses sessions whose thread was deleted or belongs to another account", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      yield* seedThread({
        threadId: ThreadId.make("thread-on-work-account"),
        providerInstanceId: ProviderInstanceId.make("codex-work"),
        nativeId: sessionId,
      });
      assert.deepEqual((yield* importer.listSessions({ projectId })).sessions, []);
      const otherAccount = yield* Effect.flip(importer.importSession(importInput));
      assert.equal(otherAccount._tag, "AgentSessionUnavailableError");
      assert.equal(
        otherAccount._tag === "AgentSessionUnavailableError" && otherAccount.reason,
        "other-account",
      );

      const deletedSessionId = "019a0000-0000-7000-8000-000000000002";
      yield* seedThread({
        threadId: ThreadId.make("thread-deleted"),
        providerInstanceId: codexInstance,
        nativeId: deletedSessionId,
        deleted: true,
      });
      const deleted = yield* Effect.flip(
        importer.importSession({ ...importInput, providerSessionId: deletedSessionId }),
      );
      assert.equal(deleted._tag === "AgentSessionUnavailableError" && deleted.reason, "deleted");
      assert.equal(reads.count, 0);
    }).pipe(Effect.provide(makeLayer(reads)));
  });

  it.effect("refuses an import thread that moved to a fresh native session", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      const eventSink = yield* EventSink.EventSinkV2;
      yield* importer.importSession(importInput);
      const { providerThread } = yield* activeProviderThread(importedThreadId);
      // What a failed native resume leaves: same row, different native session.
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("rebound-provider-thread"),
            type: "provider-thread.updated",
            threadId: importedThreadId,
            driver: codexDriver,
            providerInstanceId: codexInstance,
            occurredAt: providerThread.updatedAt,
            payload: {
              ...providerThread,
              nativeThreadRef: {
                driver: codexDriver,
                nativeId: "fresh-native",
                strength: "strong",
              },
            },
          },
        ],
      });

      const replaced = yield* Effect.flip(importer.importSession(importInput));
      assert.equal(replaced._tag === "AgentSessionUnavailableError" && replaced.reason, "replaced");
      assert.equal(reads.count, 1);
    }).pipe(Effect.provide(makeLayer(reads)));
  });

  it.effect("marks imports written before the marker existed when they are reopened", () => {
    const reads = { count: 0 };
    return Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const unverifiedSessionId = "019a0000-0000-7000-8000-000000000003";
      const unverifiedThreadId = ThreadId.make(`import:${codexInstance}:${unverifiedSessionId}`);
      for (const [threadId, nativeId] of [
        [importedThreadId, sessionId],
        [unverifiedThreadId, unverifiedSessionId],
      ] as const) {
        yield* seedThread({
          threadId,
          providerInstanceId: codexInstance,
          nativeId,
          legacyImport: true,
        });
        yield* runtimes.upsert({
          threadId,
          providerName: "codex",
          providerInstanceId: codexInstance,
          adapterKey: "codex",
          runtimeMode: "full-access",
          status: "stopped",
          lastSeenAt: thread.updatedAt,
          resumeCursor: { threadId: nativeId },
          runtimePayload: { cwd: workspaceRoot },
        });
      }
      // Only the first thread has the transcript record that proves its origin.
      yield* runtimes.recordImportedTranscript({ threadId: importedThreadId, source });

      assert.deepEqual(yield* importer.importSession(importInput), {
        threadId: importedThreadId,
        imported: false,
      });
      assert.deepEqual(
        yield* importer.importSession({ ...importInput, providerSessionId: unverifiedSessionId }),
        { threadId: unverifiedThreadId, imported: false },
      );

      const repaired = yield* activeProviderThread(importedThreadId);
      assert.equal(repaired.providerThread.nativeMetadata?.importedNativeId, sessionId);
      const unverified = yield* activeProviderThread(unverifiedThreadId);
      assert.isUndefined(unverified.providerThread.nativeMetadata?.importedNativeId);
    }).pipe(Effect.provide(makeLayer(reads)));
  });
});
