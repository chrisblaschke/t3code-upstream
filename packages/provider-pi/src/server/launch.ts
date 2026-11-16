import {
  ProviderInstanceId,
  ProviderDriverKind,
  type ProviderInstanceEnvironment,
} from "@t3tools/contracts";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as Effect from "effect/Effect";
import type { PiSettings } from "../settings.ts";
import { resolvePiLaunchArgs } from "./mcpInjection.ts";

export function resolvePiCommand(config: PiSettings) {
  const parsed = resolvePiLaunchArgs(config.launchArgs);
  return parsed.ok ? { ...parsed, command: config.binaryPath || "pi" } : parsed;
}

export const resolvePiTerminalLaunch = Effect.fn(function* (input: {
  readonly config: PiSettings;
  readonly environment: ProviderInstanceEnvironment | undefined;
}) {
  const launch = resolvePiCommand(input.config);
  if (!launch.ok)
    return yield* new ProviderDriverError({
      driver: ProviderDriverKind.make("pi"),
      instanceId: ProviderInstanceId.make("pi"),
      detail: "Invalid Pi launch configuration.",
    });
  return {
    command: launch.command,
    args: launch.args,
    env: yield* mergeProviderInstanceEnvironment(input.environment, {}),
    redact: (input.environment ?? []).flatMap((value) =>
      value.sensitive && typeof value.value === "string" && value.value ? [value.value] : [],
    ),
  };
});
