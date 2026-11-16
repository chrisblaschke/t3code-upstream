import { TerminalPiSessionError, ThreadId, type TerminalOpenPiInput } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- identifiers are generated on the server.
import * as NodeCrypto from "node:crypto";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import * as TerminalManager from "./Manager.ts";

export const openPiTerminal = Effect.fn("terminal.openPi")(function* (input: TerminalOpenPiInput) {
  const refused = () => new TerminalPiSessionError({ threadId: input.threadId });
  const threads = yield* ThreadManagement.ThreadManagementService;
  const threadId = ThreadId.make(input.threadId);
  const thread = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(refused));
  if (!thread?.activeProviderThreadId) return yield* refused();
  const { providerThreads } = yield* threads
    .getThreadRecords(threadId, ["providerThreads"], {
      providerThreadIds: [thread.activeProviderThreadId],
    })
    .pipe(Effect.mapError(refused));
  const providerThread = providerThreads.find((p) => p.id === thread.activeProviderThreadId);
  const sessionFile = providerThread?.nativeThreadRef?.nativeId;
  if (providerThread?.driver !== "pi" || !sessionFile?.endsWith(".jsonl")) return yield* refused();
  const instances = yield* ProviderInstances.ProviderInstanceRegistry;
  const instance = yield* instances.getInstance(providerThread.providerInstanceId);
  if (instance?.driverKind !== "pi" || !instance.enabled || !instance.terminalLaunch)
    return yield* refused();
  const project = yield* (yield* ProjectStore.ProjectStoreV2)
    .get(thread.projectId)
    .pipe(Effect.mapError(refused));
  if (Option.isNone(project)) return yield* refused();
  const launch = yield* instance.terminalLaunch().pipe(Effect.mapError(refused));
  const terminalId = `pi-${NodeCrypto.randomUUID()}`;
  const terminal = yield* (yield* TerminalManager.TerminalManager)
    .openProcess(
      {
        threadId,
        terminalId,
        cwd: thread.worktreePath ?? project.value.workspaceRoot,
        worktreePath: thread.worktreePath,
      },
      { ...launch, args: [...launch.args, "--session", sessionFile] },
    )
    .pipe(Effect.mapError(refused));
  if (terminal.status === "error") return yield* refused();
  return terminal;
});
