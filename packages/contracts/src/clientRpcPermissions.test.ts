import { describe, expect, it } from "vite-plus/test";
import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import { WS_METHODS } from "./rpc.ts";

describe("fork RPC client permissions", () => {
  it("requires terminal operation for host terminals", () => {
    expect(clientRpcRequiredScopes(WS_METHODS.shellOpenExternalTerminal, undefined)).toEqual([
      "terminal:operate",
    ]);
  });
});
