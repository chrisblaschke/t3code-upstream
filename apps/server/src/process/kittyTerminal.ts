/**
 * Opens a tab in the host's kitty, the user's own configured terminal, instead
 * of T3's embedded one. A running kitty with remote control enabled
 * (`allow_remote_control` + `listen_on unix:...`) gets a new tab in its
 * existing window; otherwise kitty is started with the command.
 */
import { ExternalTerminalLaunchError, type OpenExternalTerminalInput } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";

const KITTY_APP_CANDIDATES = (home: string) => [
  "/Applications/kitty.app",
  `${home}/Applications/kitty.app`,
];

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

export const findKittyApp = Effect.fn("kittyTerminal.findApp")(function* (home: string) {
  if ((yield* HostProcess.Platform) !== "darwin") return null;
  const fileSystem = yield* FileSystem.FileSystem;
  for (const app of KITTY_APP_CANDIDATES(home)) {
    if (
      yield* fileSystem
        .exists(`${app}/Contents/MacOS/kitten`)
        .pipe(Effect.orElseSucceed(() => false))
    ) {
      return app;
    }
  }
  return null;
});

/**
 * kitty appends `-<pid>` to a `listen_on unix:` path, so a crashed kitty
 * leaves stale sockets behind; take the newest one whose kitty still runs.
 */
const findKittySocket = Effect.fn("kittyTerminal.findSocket")(function* (home: string) {
  const override = (yield* HostProcess.Environment).KITTY_LISTEN_ON?.trim();
  if (override) return override;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const names = yield* fileSystem
    .readDirectory(home)
    .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
  const live = names.flatMap((name) => {
    const match = /^\.kitty\.sock-(\d+)$/.exec(name);
    const pid = match ? Number(match[1]) : NaN;
    return Number.isSafeInteger(pid) && isProcessAlive(pid) ? [{ name, pid }] : [];
  });
  const newest = live.toSorted((left, right) => right.pid - left.pid)[0];
  return newest === undefined ? null : `unix:${path.join(home, newest.name)}`;
});

export const openKittyTerminal = Effect.fn("kittyTerminal.open")(function* (
  input: OpenExternalTerminalInput,
) {
  // cwd reaches kitty's --cwd/--directory; accept only a real directory.
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const cwdInfo = path.isAbsolute(input.cwd)
    ? yield* fileSystem.stat(input.cwd).pipe(Effect.orElseSucceed(() => null))
    : null;
  if (cwdInfo?.type !== "Directory") {
    return yield* new ExternalTerminalLaunchError({
      detail: `Not an existing absolute directory: ${input.cwd}`,
    });
  }
  const env = yield* HostProcess.Environment;
  const home = env.HOME ?? "";
  const app = yield* findKittyApp(home);
  if (app === null) {
    return yield* new ExternalTerminalLaunchError({ detail: "kitty.app was not found." });
  }
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const shell = env.SHELL?.trim() || "/bin/zsh";
  // A login shell gets the user's PATH (kitty started from the Dock has a bare
  // one); dropping into a shell afterwards keeps the tab after the command ends.
  const program =
    input.command === undefined || input.command.length === 0
      ? [shell, "-l"]
      : [shell, "-lic", `${input.command.map(shellQuote).join(" ")}; exec ${shellQuote(shell)} -l`];

  const run = (command: string, args: ReadonlyArray<string>) =>
    spawner
      .exitCode(
        ChildProcess.make(command, args, { stdin: "ignore", stdout: "ignore", stderr: "ignore" }),
      )
      .pipe(
        Effect.mapError(
          (cause) => new ExternalTerminalLaunchError({ detail: `${command}: ${String(cause)}` }),
        ),
      );

  const socket = yield* findKittySocket(home);
  if (socket !== null) {
    const code = yield* run(`${app}/Contents/MacOS/kitten`, [
      "@",
      "--to",
      socket,
      "launch",
      "--type=tab",
      `--cwd=${input.cwd}`,
      ...(input.title ? [`--tab-title=${input.title}`] : []),
      ...program,
    ]);
    if (code === 0) {
      // `launch` focuses the tab inside kitty; bring kitty itself forward too.
      yield* run("open", ["-a", app]).pipe(Effect.ignore);
      return;
    }
  }
  // No reachable kitty: start one with the command. `-n` opens a new
  // instance even when kitty already runs (without remote control), which
  // `open -a` would otherwise just focus, dropping the arguments.
  const code = yield* run("open", [
    "-n",
    "-a",
    app,
    "--args",
    `--directory=${input.cwd}`,
    ...program,
  ]);
  if (code !== 0) {
    return yield* new ExternalTerminalLaunchError({ detail: `open exited with ${code}` });
  }
});

export const resolveExternalTerminal = Effect.fn("kittyTerminal.resolve")(function* () {
  const app = yield* findKittyApp((yield* HostProcess.Environment).HOME ?? "");
  return app === null ? undefined : ("kitty" as const);
});
