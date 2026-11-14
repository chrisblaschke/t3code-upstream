/**
 * The Pi TUI command for a thread's Pi session, so a terminal (kitty on the
 * desktop host, the built-in terminal on mobile) can show the same session
 * live. Pi threads resume by session file path, which the provider thread
 * keeps as its native thread ref.
 */
import type {
  OrchestrationV2ThreadProjection,
  ProviderInstanceConfigMap,
} from "@t3tools/contracts";

export interface PiTuiLaunch {
  /** `pi --session <file>` as argv, for launchers that exec directly. */
  readonly argv: ReadonlyArray<string>;
  /** The same command quoted for a POSIX shell, for terminals that type it. */
  readonly shellCommand: string;
  readonly sessionFile: string;
}

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

/** Null when the thread is not a Pi thread or has no session file yet. */
export function resolvePiTuiLaunch(
  projection: {
    readonly thread: Pick<OrchestrationV2ThreadProjection["thread"], "activeProviderThreadId">;
    readonly providerThreads: OrchestrationV2ThreadProjection["providerThreads"];
  } | null,
  instances: ProviderInstanceConfigMap = {},
): PiTuiLaunch | null {
  if (projection === null) return null;
  const activeId = projection.thread.activeProviderThreadId;
  const providerThread =
    projection.providerThreads.find((candidate) => candidate.id === activeId) ?? null;
  if (providerThread === null || providerThread.driver !== "pi") return null;
  const sessionFile = providerThread.nativeThreadRef?.nativeId;
  if (sessionFile == null || !sessionFile.endsWith(".jsonl")) return null;
  const config = instances[providerThread.providerInstanceId]?.config;
  const binaryPath =
    config !== null &&
    typeof config === "object" &&
    "binaryPath" in config &&
    typeof config.binaryPath === "string" &&
    config.binaryPath.length > 0
      ? config.binaryPath
      : "pi";
  const argv = [binaryPath, "--session", sessionFile];
  return { argv, shellCommand: argv.map(shellQuote).join(" "), sessionFile };
}
