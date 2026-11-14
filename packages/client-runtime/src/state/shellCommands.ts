import { WS_METHODS } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { createEnvironmentRpcCommand } from "./runtime.ts";
import type { EnvironmentRegistry } from "../connection/registry.ts";

export function createShellEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    openExternalTerminal: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:shell:open-external-terminal",
      tag: WS_METHODS.shellOpenExternalTerminal,
    }),
    openInEditor: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:shell:open-in-editor",
      tag: WS_METHODS.shellOpenInEditor,
    }),
  };
}
