import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/shell";
import type { AgentSessionSummary, ScopedProjectRef } from "@t3tools/contracts";
import { useRouter } from "@tanstack/react-router";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { RefreshCwIcon } from "lucide-react";
import { useContext, useMemo, useState } from "react";
import { create } from "zustand";

import { agentSessionImportSession, agentSessionList } from "~/state/agentSessions";
import { useProject } from "~/state/entities";
import { formatEnvironmentQueryError } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";
import { buildThreadRouteParams } from "~/threadRoutes";
import { formatRelativeTimeLabel } from "~/timestampFormat";
import { ClaudeAI, OpenAI } from "../Icons";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { DiscoveryList, DiscoveryListRow } from "../ui/discovery-list";
import { Input } from "../ui/input";
import { Spinner } from "../ui/spinner";
import { toastManager } from "../ui/toast";

const PROVIDER_LABELS = { claudeAgent: "Claude Code", codex: "Codex" } as const;

interface ResumeRequest {
  readonly id: number;
  readonly projectRef: ScopedProjectRef;
}

let nextRequestId = 0;
const useResumeRequest = create<{ request: ResumeRequest | null }>(() => ({ request: null }));

/**
 * Open the picker for a project's Claude Code and Codex sessions. The list is
 * read on the project's own environment, and only while the picker is open.
 */
export function openResumeSessionDialog(projectRef: ScopedProjectRef) {
  nextRequestId += 1;
  useResumeRequest.setState({ request: { id: nextRequestId, projectRef } });
}

/** Whether the picker opened as `requestId` is still the one on screen. */
function isCurrentRequest(requestId: number): boolean {
  return useResumeRequest.getState().request?.id === requestId;
}

function closeResumeSessionDialog(requestId: number) {
  if (isCurrentRequest(requestId)) useResumeRequest.setState({ request: null });
}

export function ResumeSessionDialogHost() {
  const request = useResumeRequest((state) => state.request);
  // Keyed so another project's picker starts with fresh search and state.
  return request ? <ResumeSessionDialog key={request.id} request={request} /> : null;
}

function sessionDescription(session: AgentSessionSummary): string {
  return [
    PROVIDER_LABELS[session.provider],
    // Name the account only when it is not the provider's default one.
    session.providerInstanceId === session.provider ? null : session.providerInstanceId,
    session.model,
    formatRelativeTimeLabel(session.lastActiveAt),
  ]
    .filter((part) => part !== null && part !== "")
    .join(" · ");
}

function ResumeSessionDialog({ request }: { readonly request: ResumeRequest }) {
  const project = useProject(request.projectRef);
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) closeResumeSessionDialog(request.id);
      }}
    >
      <DialogPopup className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>Resume a session</DialogTitle>
          <DialogDescription>
            Claude Code and Codex sessions that ran in this project's folder. Opening one shows its
            history here, and your next message continues the same session. Quit it in the CLI
            first, so the two do not write to it at once.
          </DialogDescription>
        </DialogHeader>
        {project === null ? (
          <DialogPanel>
            <p className="text-muted-foreground text-sm">This project is no longer available.</p>
          </DialogPanel>
        ) : (
          <ResumeSessionPicker request={request} project={project} />
        )}
      </DialogPopup>
    </Dialog>
  );
}

function ResumeSessionPicker({
  request,
  project,
}: {
  readonly request: ResumeRequest;
  readonly project: EnvironmentProject;
}) {
  const router = useRouter();
  const registry = useContext(RegistryContext);
  const { environmentId } = request.projectRef;
  const projectInput = useMemo(
    () => ({ projectId: project.id, expectedWorkspaceRoot: project.workspaceRoot }),
    [project.id, project.workspaceRoot],
  );
  const listAtom = useMemo(
    () => agentSessionList({ environmentId, input: projectInput }),
    [environmentId, projectInput],
  );
  const result = useAtomValue(listAtom);
  const listed = Option.getOrNull(AsyncResult.value(result));
  const loading = result.waiting || result._tag === "Initial";
  const error = result._tag === "Failure" ? formatEnvironmentQueryError(result.cause) : null;
  const canOpen = useAtomValue(agentSessionImportSession.permissionAtom(environmentId));
  const importSession = useAtomCommand(agentSessionImportSession, { reportFailure: false });
  const [query, setQuery] = useState("");
  const [openingSessionId, setOpeningSessionId] = useState<string | null>(null);

  const sessions = useMemo(() => {
    const terms = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
    return (listed?.sessions ?? []).filter((session) => {
      const haystack = `${session.title} ${sessionDescription(session)}`.toLowerCase();
      return terms.every((term) => haystack.includes(term));
    });
  }, [listed, query]);

  const openSession = async (session: AgentSessionSummary) => {
    if (openingSessionId !== null) return;
    setOpeningSessionId(session.providerSessionId);
    const opened = await importSession({
      environmentId,
      input: {
        ...projectInput,
        provider: session.provider,
        providerInstanceId: session.providerInstanceId,
        providerSessionId: session.providerSessionId,
      },
    });
    // Commands outlive the picker. A reply for a picker that was closed or
    // replaced must not close the new one, navigate, or toast.
    if (!isCurrentRequest(request.id)) return;
    setOpeningSessionId(null);
    if (opened._tag === "Success") {
      closeResumeSessionDialog(request.id);
      await router.navigate({
        to: "/$environmentId/$threadId",
        params: buildThreadRouteParams(scopeThreadRef(environmentId, opened.value.threadId)),
      });
      return;
    }
    if (isAtomCommandInterrupted(opened)) return;
    const failure = squashAtomCommandFailure(opened);
    toastManager.add({
      type: "error",
      title: "Could not open the session",
      description: failure instanceof Error ? failure.message : "The session could not be opened.",
    });
    // The session may have moved or been taken since the list was read.
    registry.refresh(listAtom);
  };

  return (
    <>
      <DialogPanel>
        <Input
          type="search"
          autoFocus
          aria-label="Search sessions"
          placeholder="Search sessions"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        {error !== null ? (
          <p role="alert" className="text-destructive-foreground text-sm">
            Could not read sessions: {error}
          </p>
        ) : null}
        {listed === null ? (
          error === null ? (
            <p className="flex items-center gap-2 text-muted-foreground text-sm">
              <Spinner size="md" />
              Looking for sessions…
            </p>
          ) : null
        ) : sessions.length === 0 ? (
          <p className="text-muted-foreground text-sm">
            {listed.sessions.length === 0
              ? "No Claude Code or Codex sessions have run in this project's folder."
              : "No sessions match your search."}
          </p>
        ) : (
          <DiscoveryList>
            {sessions.map((session) => (
              <DiscoveryListRow
                key={`${session.providerInstanceId}:${session.providerSessionId}`}
                disabled={!canOpen || openingSessionId !== null}
                onClick={() => void openSession(session)}
                icon={
                  session.provider === "claudeAgent" ? (
                    <ClaudeAI aria-hidden className="size-5 shrink-0" />
                  ) : (
                    <OpenAI aria-hidden className="size-5 shrink-0" />
                  )
                }
                title={session.title}
                description={sessionDescription(session)}
                action={
                  openingSessionId === session.providerSessionId ? (
                    <Spinner size="md" tone="muted" />
                  ) : session.thread !== null ? (
                    <span className="shrink-0 text-muted-foreground text-xs">
                      {session.thread.archived ? "Archived in T3" : "In T3"}
                    </span>
                  ) : null
                }
              />
            ))}
          </DiscoveryList>
        )}
      </DialogPanel>
      <DialogFooter>
        <p className="mr-auto self-center text-muted-foreground text-xs">
          {!canOpen
            ? "You can browse sessions but not open them on this environment."
            : listed?.truncated === true
              ? "Showing the most recent sessions."
              : null}
        </p>
        <Button variant="ghost" disabled={loading} onClick={() => registry.refresh(listAtom)}>
          {loading ? <Spinner /> : <RefreshCwIcon />}
          Refresh
        </Button>
      </DialogFooter>
    </>
  );
}
