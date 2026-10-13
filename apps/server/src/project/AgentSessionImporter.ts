import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionImportSource,
  AgentSessionScanError,
  AgentSessionSource,
  AgentSessionUnavailableError,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type AgentSessionImportSessionInput,
  type AgentSessionListInput,
  type AgentSessionListResult,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const encodeIdList = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const decodeImportedTranscriptPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    cwd: Schema.optional(Schema.String),
    importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)),
  }),
);

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

/** The T3 thread bound to a native session. */
interface NativeSessionOwner {
  readonly threadId: ThreadId;
  readonly providerInstanceId: string;
  readonly archived: boolean;
  readonly deleted: boolean;
}

/**
 * Only an owner on the selected instance is reopened. A live owner on another
 * instance may be a different account's copy of the session, or share its
 * home, so it is neither reopened nor imported over.
 */
function sessionOwnership(
  owners: ReadonlyArray<NativeSessionOwner> | undefined,
  providerInstanceId: string,
):
  | { readonly _tag: "Free" }
  | { readonly _tag: "Owned"; readonly owner: NativeSessionOwner }
  | { readonly _tag: "OtherAccount" }
  | { readonly _tag: "Deleted" } {
  const live = owners?.filter((owner) => !owner.deleted) ?? [];
  const owner = live.find((candidate) => candidate.providerInstanceId === providerInstanceId);
  if (owner !== undefined) return { _tag: "Owned", owner };
  if (live.length > 0) return { _tag: "OtherAccount" };
  return owners === undefined || owners.length === 0 ? { _tag: "Free" } : { _tag: "Deleted" };
}

function importedThreadId(providerInstanceId: ProviderInstanceId, providerSessionId: string) {
  return ThreadId.make(`import:${providerInstanceId}:${providerSessionId}`);
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly index: number;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.index + 1;
  const suffix = String(input.index).padStart(6, "0");
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${input.threadId}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const sql = yield* SqlClient.SqlClient;
  const threadDispatch = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  // Imports from every client check ownership and write under one permit, so
  // two requests cannot give one native session two T3 threads.
  const importLock = yield* Semaphore.make(1);

  const resolveProject = Effect.fn("AgentSessionImporter.resolveProject")(function* (input: {
    readonly projectId: ProjectId;
    readonly expectedWorkspaceRoot?: string | undefined;
  }) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    return project;
  });

  /**
   * Threads bound to these native sessions through any provider thread, not
   * just the ones this importer created, and on any instance of the driver:
   * instances can share a home, and so a session. A live owner wins over an
   * archived one, and either wins over a deleted one.
   */
  const nativeSessionOwners = Effect.fn("AgentSessionImporter.nativeSessionOwners")(function* (
    source: AgentSessionSource,
    nativeIds: ReadonlyArray<string>,
  ) {
    const owners = new Map<string, Array<NativeSessionOwner>>();
    if (nativeIds.length === 0) return owners;
    const rows = yield* sql<{
      readonly nativeId: string;
      readonly threadId: string;
      readonly providerInstanceId: string;
      readonly archivedAt: string | null;
      readonly deletedAt: string | null;
    }>`
      SELECT
        json_extract(provider_threads.payload_json, '$.nativeThreadRef.nativeId') AS "nativeId",
        threads.thread_id AS "threadId",
        provider_threads.provider_instance_id AS "providerInstanceId",
        threads.archived_at AS "archivedAt",
        threads.deleted_at AS "deletedAt"
      FROM orchestration_v2_projection_provider_threads AS provider_threads
      JOIN orchestration_v2_projection_threads AS threads
        ON threads.thread_id = provider_threads.thread_id
      WHERE provider_threads.driver = ${source}
        AND json_extract(provider_threads.payload_json, '$.nativeThreadRef.nativeId')
          IN (SELECT value FROM json_each(${encodeIdList(nativeIds)}))
      ORDER BY
        threads.deleted_at IS NOT NULL,
        threads.archived_at IS NOT NULL,
        threads.updated_at DESC
    `.pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
    );
    for (const row of rows) {
      const owner = {
        threadId: ThreadId.make(row.threadId),
        providerInstanceId: row.providerInstanceId,
        archived: row.archivedAt !== null,
        deleted: row.deletedAt !== null,
      };
      const existing = owners.get(row.nativeId);
      if (existing === undefined) owners.set(row.nativeId, [owner]);
      else existing.push(owner);
    }
    return owners;
  });

  /** Persist one parsed session as a settled thread bound to its native session. */
  const writeImportedThread = Effect.fn("AgentSessionImporter.writeImportedThread")(function* (
    input: {
      readonly projectId: ProjectId;
      readonly workspaceRoot: string;
      readonly threadId: ThreadId;
    } & AgentSessionScanner.AgentSessionThreadWithSource,
  ) {
    const { thread, threadId } = input;
    if (
      thread.source === "claudeAgent" &&
      !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
    ) {
      return yield* new AgentSessionUnresumableSessionError({
        source: thread.source,
        providerSessionId: thread.providerSessionId,
      });
    }
    const driver = ProviderDriverKind.make(thread.source);
    const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
    // Scoped by instance: two accounts sharing a session id must not share a row.
    const providerThreadId = idAllocator.derive.providerThread({
      driver,
      providerInstanceId: thread.providerInstanceId,
      nativeThreadId: thread.providerSessionId,
    });
    const createdAt = dateTime(thread.createdAt);
    const updatedAt = dateTime(thread.updatedAt);
    const appThread: OrchestrationV2AppThread = {
      createdBy: "system",
      creationSource: "server",
      id: threadId,
      projectId: input.projectId,
      title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
      providerInstanceId: thread.providerInstanceId,
      modelSelection: { instanceId: thread.providerInstanceId, model },
      runtimeMode: DEFAULT_RUNTIME_MODE,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      linkedPullRequest: null,
      branchPullRequest: null,
      activeProviderThreadId: providerThreadId,
      historyOrigin: "v1_import",
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt,
      updatedAt,
      archivedAt: null,
      settledOverride: "settled",
      settledAt: updatedAt,
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
      driver,
      providerInstanceId: thread.providerInstanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: {
        driver,
        nativeId: thread.providerSessionId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      nativeMetadata: { importedNativeId: thread.providerSessionId },
      createdAt,
      updatedAt,
    };

    yield* runtimes.upsert(
      {
        threadId,
        providerName: driver,
        providerInstanceId: thread.providerInstanceId,
        adapterKey: driver,
        runtimeMode: DEFAULT_RUNTIME_MODE,
        status: "stopped",
        lastSeenAt: thread.updatedAt,
        resumeCursor:
          thread.source === "codex"
            ? { threadId: thread.providerSessionId }
            : { threadId, resume: thread.providerSessionId },
        runtimePayload: { cwd: input.workspaceRoot },
      },
      { onConflict: "ignore" },
    );
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
          type: "thread.created",
          threadId,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: createdAt,
          payload: appThread,
        },
        ...thread.messages.flatMap((message, index) => messageEvents({ threadId, index, message })),
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: updatedAt,
          payload: providerThread,
        },
      ],
    });
    yield* runtimes.recordImportedTranscript({ threadId, source: input.source });
  });

  /**
   * Imports written before `importedNativeId` existed lack the marker, so their
   * first prompt would replay the history into a session that already has it.
   * Add the marker only while no prompt has been sent and both the recorded
   * transcript and the provider thread still name this native session.
   */
  const repairImportedNativeMarker = Effect.fn("AgentSessionImporter.repairImportedNativeMarker")(
    function* (input: {
      readonly threadId: ThreadId;
      readonly source: Pick<
        AgentSessionImportSource,
        "provider" | "providerInstanceId" | "providerSessionId"
      >;
    }) {
      // The full-row write below must not race a dispatch. Every run starts
      // with a dispatch under this lock, and the row must have no run yet.
      return yield* threadDispatch.withLock(input.threadId, repairUnlocked(input));
    },
  );

  const repairUnlocked = Effect.fn("AgentSessionImporter.repairUnlocked")(function* (input: {
    readonly threadId: ThreadId;
    readonly source: Pick<
      AgentSessionImportSource,
      "provider" | "providerInstanceId" | "providerSessionId"
    >;
  }) {
    const records = yield* Effect.option(
      orchestrator.getThreadRecords(input.threadId, ["providerThreads"]),
    );
    if (Option.isNone(records)) return;
    const { thread, providerThreads } = records.value;
    const providerThread = providerThreads.find(
      (candidate) => candidate.id === thread.activeProviderThreadId,
    );
    if (
      thread.historyOrigin !== "v1_import" ||
      thread.deletedAt !== null ||
      providerThread === undefined ||
      providerThread.nativeMetadata?.importedNativeId !== undefined ||
      providerThread.lastRunOrdinal !== null ||
      providerThread.driver !== input.source.provider ||
      providerThread.providerInstanceId !== input.source.providerInstanceId ||
      providerThread.nativeThreadRef?.nativeId !== input.source.providerSessionId
    ) {
      return;
    }
    const runtime = yield* runtimes.getByThreadId({ threadId: input.threadId });
    const recorded = Option.flatMap(runtime, (row) =>
      decodeImportedTranscriptPayload(row.runtimePayload),
    ).pipe(
      Option.exists(
        (payload) =>
          payload.importedTranscripts?.some(
            (source) =>
              source.provider === input.source.provider &&
              source.providerInstanceId === input.source.providerInstanceId &&
              source.providerSessionId === input.source.providerSessionId,
          ) === true,
      ),
    );
    if (!recorded) return;
    // The projection copies this onto the thread, so never move it backwards.
    const updatedAt = DateTime.max(providerThread.updatedAt, thread.updatedAt);
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(
            `${IMPORT_EVENT_PREFIX}:provider-thread:${providerThread.id}:imported-native-id`,
          ),
          type: "provider-thread.updated",
          threadId: input.threadId,
          driver: providerThread.driver,
          providerInstanceId: providerThread.providerInstanceId,
          occurredAt: updatedAt,
          payload: {
            ...providerThread,
            nativeMetadata: {
              ...providerThread.nativeMetadata,
              importedNativeId: input.source.providerSessionId,
            },
            updatedAt,
          },
        },
      ],
    });
  });

  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* resolveProject(input);
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      ) {
        return [];
      }
      return payload.value.importedTranscripts ?? [];
    });
    const outcomes = scanner.recentThreads(project.workspaceRoot, completedSources);
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;

    yield* Stream.runForEach(outcomes, (outcome) =>
      Effect.gen(function* () {
        if (outcome._tag === "Skipped") {
          skippedCount += 1;
          return;
        }
        const source = outcome.source;
        const threadId = importedThreadId(source.providerInstanceId, source.providerSessionId);
        if (outcome._tag === "AlreadyImported") {
          yield* repairImportedNativeMarker({ threadId, source }).pipe(
            importLock.withPermits(1),
            Effect.ignore,
          );
          importedThreadIds.add(threadId);
          importedCount += 1;
          return;
        }
        if (outcome._tag === "Duplicate") {
          if (importedThreadIds.has(threadId)) {
            yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
          }
          return;
        }

        const imported = yield* Effect.gen(function* () {
          const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
          if (Option.isSome(existing)) {
            if (existing.value.thread.projectId !== input.projectId) {
              return yield* new AgentSessionThreadProjectConflictError({
                threadId,
                expectedProjectId: input.projectId,
                actualProjectId: existing.value.thread.projectId,
              });
            }
            if (existing.value.thread.historyOrigin !== "v1_import") {
              return yield* new AgentSessionThreadModifiedError({ threadId });
            }
            yield* runtimes.recordImportedTranscript({ threadId, source });
            yield* repairImportedNativeMarker({ threadId, source });
            return true;
          }
          // Another T3 thread already runs this session, possibly one T3 started.
          const owners = yield* nativeSessionOwners(source.provider, [source.providerSessionId]);
          if (owners.has(source.providerSessionId)) return false;
          yield* writeImportedThread({
            projectId: input.projectId,
            workspaceRoot: project.workspaceRoot,
            threadId,
            thread: outcome.thread,
            source,
          });
          return true;
        }).pipe(
          importLock.withPermits(1),
          Effect.catch((cause) =>
            Effect.logWarning("Could not import an agent session", {
              provider: outcome.thread.source,
              sessionId: outcome.thread.providerSessionId,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
        if (imported) {
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else {
          skippedCount += 1;
        }
      }),
    );

    return { importedCount, skippedCount } satisfies AgentSessionImportResult;
  });

  const listSessions = Effect.fn("AgentSessionImporter.listSessions")(function* (
    input: AgentSessionListInput,
  ) {
    const project = yield* resolveProject(input);
    const listed = yield* scanner.listSessions(project.workspaceRoot);
    const owners = new Map<string, ReadonlyArray<NativeSessionOwner>>();
    for (const source of ["claudeAgent", "codex"] as const) {
      const nativeIds = listed.sessions
        .filter((session) => session.source === source)
        .map((session) => session.providerSessionId);
      for (const [nativeId, sessionOwners] of yield* nativeSessionOwners(source, nativeIds)) {
        owners.set(`${source}\0${nativeId}`, sessionOwners);
      }
    }
    return {
      // Sessions that cannot be opened from here are left out: a deleted
      // thread cannot be restored, and another account's thread is not ours.
      sessions: listed.sessions.flatMap((session) => {
        const ownership = sessionOwnership(
          owners.get(`${session.source}\0${session.providerSessionId}`),
          session.providerInstanceId,
        );
        if (ownership._tag === "Deleted" || ownership._tag === "OtherAccount") return [];
        return [
          {
            provider: session.source,
            providerInstanceId: session.providerInstanceId,
            providerSessionId: session.providerSessionId,
            title: session.title,
            model: session.model,
            lastActiveAt: session.lastActiveAt,
            thread:
              ownership._tag === "Owned"
                ? { id: ownership.owner.threadId, archived: ownership.owner.archived }
                : null,
          },
        ];
      }),
      ...(listed.truncated ? { truncated: true } : {}),
    } satisfies AgentSessionListResult;
  });

  /**
   * Open the T3 thread for one native session, importing its history first
   * when no thread owns it yet. Importing never starts a turn.
   */
  const importSession = Effect.fn("AgentSessionImporter.importSession")(function* (
    input: AgentSessionImportSessionInput,
  ) {
    const project = yield* resolveProject(input);
    const unavailable = (reason: AgentSessionUnavailableError["reason"]) =>
      new AgentSessionUnavailableError({ providerSessionId: input.providerSessionId, reason });
    return yield* Effect.gen(function* () {
      const threadId = importedThreadId(input.providerInstanceId, input.providerSessionId);
      const ownership = sessionOwnership(
        (yield* nativeSessionOwners(input.provider, [input.providerSessionId])).get(
          input.providerSessionId,
        ),
        input.providerInstanceId,
      );
      if (ownership._tag === "Owned") {
        yield* repairImportedNativeMarker({
          threadId: ownership.owner.threadId,
          source: {
            provider: input.provider,
            providerInstanceId: input.providerInstanceId,
            providerSessionId: input.providerSessionId,
          },
        });
        return { threadId: ownership.owner.threadId, imported: false };
      }
      if (ownership._tag === "OtherAccount") return yield* unavailable("other-account");
      if (ownership._tag === "Deleted") return yield* unavailable("deleted");
      const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
      if (Option.isSome(existing)) {
        // The import thread exists but is no longer bound to this session: a
        // failed resume moved it to a fresh native session. Opening it would
        // continue a different conversation, and its id cannot be reused.
        return yield* unavailable(
          existing.value.thread.deletedAt === null ? "replaced" : "deleted",
        );
      }

      const session = yield* scanner.readSession({
        workspaceRoot: project.workspaceRoot,
        source: input.provider,
        providerInstanceId: input.providerInstanceId,
        providerSessionId: input.providerSessionId,
      });
      if (session === null) return yield* unavailable("not-found");
      yield* writeImportedThread({
        projectId: project.id,
        workspaceRoot: project.workspaceRoot,
        threadId,
        ...session,
      });
      return { threadId, imported: true };
    }).pipe(
      importLock.withPermits(1),
      Effect.mapError((cause) =>
        cause._tag === "AgentSessionUnresumableSessionError"
          ? unavailable("unresumable")
          : cause._tag === "AgentSessionScanError" || cause._tag === "AgentSessionUnavailableError"
            ? cause
            : new AgentSessionScanError({ operation: "import-session", cause }),
      ),
    );
  });

  return { importRecentAgentThreads, listSessions, importSession };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);
