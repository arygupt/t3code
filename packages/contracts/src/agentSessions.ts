import * as Schema from "effect/Schema";
import {
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

/** Coding agent home directories the scanner knows how to read. */
export const AgentSessionSource = Schema.Literals(["claudeAgent", "codex"]);
export type AgentSessionSource = typeof AgentSessionSource.Type;

/** File identity saved with an imported session so bounded retries can skip unchanged history. */
export const AgentSessionImportSource = Schema.Struct({
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
  filePath: TrimmedNonEmptyString,
  size: NonNegativeInt,
  mtimeMs: Schema.NullOr(Schema.Number),
  device: Schema.Number,
  inode: Schema.NullOr(Schema.Number),
  birthtimeMs: Schema.NullOr(Schema.Number),
});
export type AgentSessionImportSource = typeof AgentSessionImportSource.Type;

/**
 * Empty for now. Kept as a struct so future scan options (source filters,
 * explicit roots) can be added without a new method.
 */
export const AgentSessionScanInput = Schema.Struct({});
export type AgentSessionScanInput = typeof AgentSessionScanInput.Type;

/**
 * A directory that at least one agent CLI has run in, suitable for import as a
 * T3 Code project. `alreadyImported` marks candidates that already have an
 * active project rooted at the same path.
 */
/**
 * Git identity of a candidate directory, read from `.git/config` without
 * spawning git. `remoteKey` is the normalized origin URL, shared by every
 * clone of the same repository so the client can group them. `repository`
 * is the GitHub `owner/name` when the origin is on GitHub.
 */
export const AgentSessionProjectGit = Schema.Struct({
  remoteKey: Schema.NullOr(Schema.String),
  repository: Schema.NullOr(Schema.String),
});
export type AgentSessionProjectGit = typeof AgentSessionProjectGit.Type;

export const AgentSessionProjectCandidate = Schema.Struct({
  path: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  projectId: Schema.optional(ProjectId),
  sources: Schema.Array(AgentSessionSource),
  threadCount: NonNegativeInt,
  lastActiveAt: Schema.NullOr(IsoDateTime),
  alreadyImported: Schema.Boolean,
  /**
   * `null` when the directory is not the root of a git repository. Missing on
   * servers that predate the git scan, where the client cannot tell repositories
   * from plain folders and should treat every candidate as a standalone project.
   */
  git: Schema.optionalKey(Schema.NullOr(AgentSessionProjectGit)),
});
export type AgentSessionProjectCandidate = typeof AgentSessionProjectCandidate.Type;

export const AgentSessionScanResult = Schema.Struct({
  candidates: Schema.Array(AgentSessionProjectCandidate),
  scannedAt: IsoDateTime,
  truncated: Schema.optional(Schema.Boolean),
});
export type AgentSessionScanResult = typeof AgentSessionScanResult.Type;

export const AgentSessionImportInput = Schema.Struct({
  projectId: ProjectId,
  expectedWorkspaceRoot: Schema.optional(TrimmedNonEmptyString),
});
export type AgentSessionImportInput = typeof AgentSessionImportInput.Type;

export class AgentSessionImportProjectNotFoundError extends Schema.TaggedError<AgentSessionImportProjectNotFoundError>()(
  "AgentSessionImportProjectNotFoundError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project '${this.projectId}' does not exist.`;
  }
}

export class AgentSessionImportProjectChangedError extends Schema.TaggedError<AgentSessionImportProjectChangedError>()(
  "AgentSessionImportProjectChangedError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project '${this.projectId}' changed directories. Scan for projects again before importing history.`;
  }
}

export const AgentSessionImportResult = Schema.Struct({
  importedCount: NonNegativeInt,
  skippedCount: NonNegativeInt,
});
export type AgentSessionImportResult = typeof AgentSessionImportResult.Type;

/** Sessions recorded in the project's root directory, for choosing one to resume. */
export const AgentSessionListInput = Schema.Struct({
  projectId: ProjectId,
  expectedWorkspaceRoot: Schema.optional(TrimmedNonEmptyString),
});
export type AgentSessionListInput = typeof AgentSessionListInput.Type;

export const AgentSessionSummary = Schema.Struct({
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  model: Schema.NullOr(TrimmedNonEmptyString),
  lastActiveAt: IsoDateTime,
  /**
   * T3 thread already bound to this native session. Choosing the session opens
   * that thread instead of importing the session a second time.
   */
  thread: Schema.NullOr(Schema.Struct({ id: ThreadId, archived: Schema.Boolean })),
});
export type AgentSessionSummary = typeof AgentSessionSummary.Type;

export const AgentSessionListResult = Schema.Struct({
  sessions: Schema.Array(AgentSessionSummary),
  /** Older or very large transcripts were left out to keep the read bounded. */
  truncated: Schema.optional(Schema.Boolean),
});
export type AgentSessionListResult = typeof AgentSessionListResult.Type;

export const AgentSessionImportSessionInput = Schema.Struct({
  projectId: ProjectId,
  expectedWorkspaceRoot: Schema.optional(TrimmedNonEmptyString),
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
});
export type AgentSessionImportSessionInput = typeof AgentSessionImportSessionInput.Type;

/** `imported` is false when an existing T3 thread already owned the session. */
export const AgentSessionImportSessionResult = Schema.Struct({
  threadId: ThreadId,
  imported: Schema.Boolean,
});
export type AgentSessionImportSessionResult = typeof AgentSessionImportSessionResult.Type;

export class AgentSessionUnavailableError extends Schema.TaggedError<AgentSessionUnavailableError>()(
  "AgentSessionUnavailableError",
  {
    providerSessionId: Schema.String,
    reason: Schema.Literals(["not-found", "deleted", "other-account", "replaced", "unresumable"]),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "deleted":
        return "This session belongs to a deleted T3 thread.";
      case "other-account":
        return "A T3 thread on another provider account already uses this session.";
      case "replaced":
        return "This session's T3 thread now continues in a newer provider session.";
      case "unresumable":
        return "This session cannot be resumed.";
      case "not-found":
        return "This session is no longer in the project's history.";
    }
  }
}

export class AgentSessionScanError extends Schema.TaggedError<AgentSessionScanError>()(
  "AgentSessionScanError",
  {
    operation: Schema.Literals(["read-settings", "read-projects", "import-session"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to scan agent sessions during ${this.operation}.`;
  }
}
