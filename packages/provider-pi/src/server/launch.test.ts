import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import { resolvePiTerminalLaunch } from "./launch.ts";

it.effect(
  "keeps inherited server values out of terminal overrides while preserving instance values",
  () =>
    Effect.gen(function* () {
      const config = { binaryPath: "pi", launchArgs: "", customModels: [], enabled: true };
      const launch = yield* resolvePiTerminalLaunch({
        config,
        environment: [
          { name: "PI_CODING_AGENT_DIR", value: "/work/profile", sensitive: false },
          { name: "API_KEY", value: "instance-credential", sensitive: true },
          { name: "PORT", value: "9000", sensitive: false },
        ],
      });
      expect(launch.env).toEqual({
        PI_CODING_AGENT_DIR: "/work/profile",
        API_KEY: "instance-credential",
        PORT: "9000",
      });
      const defaults = yield* resolvePiTerminalLaunch({ config, environment: undefined });
      expect(defaults.env).toEqual({});
    }).pipe(
      Effect.provideService(HostProcess.Environment, {
        T3CODE_DEV_AUTH_TOKEN: "server-only-token",
        ELECTRON_RUN_AS_NODE: "1",
        TMUX: "/stale/tmux",
        PORT: "3774",
        PATH: "/usr/bin",
        PI_CODING_AGENT_DIR: "/server/profile",
      }),
    ),
);
