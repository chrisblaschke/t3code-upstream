import { assert, it } from "@effect/vitest";
import { WS_METHODS, OpenExternalTerminalInput, ExternalLauncherError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { Headers } from "effect/http";
import { Rpc, RpcGroup, RpcMessage, RpcServer } from "effect/rpc";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import { layerPiFeatureRpc, WS_RPC_SERVER_OPTIONS } from "./ws.ts";

it.effect("routes Pi terminal launches", () =>
  Effect.gen(function* () {
    const launched: OpenExternalTerminalInput[] = [];
    const responses = yield* Queue.unbounded<Exit.Exit<unknown, unknown>>();
    const server = yield* RpcServer.makeNoSerialization(
      RpcGroup.make(
        Rpc.make(WS_METHODS.shellOpenExternalTerminal, {
          payload: OpenExternalTerminalInput,
          error: ExternalLauncherError,
        }),
      ),
      {
        ...WS_RPC_SERVER_OPTIONS,
        onFromServer: (response) =>
          response._tag === "Exit" ? Queue.offer(responses, response.exit) : Effect.void,
      },
    ).pipe(
      Effect.provide(
        layerPiFeatureRpc.pipe(
          Layer.provide(
            Layer.mock(ExternalLauncher.ExternalLauncher)({
              openExternalTerminal: (input) =>
                Effect.sync(() => {
                  launched.push(input);
                }),
            }),
          ),
        ),
      ),
    );
    const input = { cwd: "/workspace/project", command: ["pi", "--session", "/tmp/session.jsonl"] };
    yield* server.write(0, {
      _tag: "Request",
      id: RpcMessage.RequestId("terminal"),
      tag: WS_METHODS.shellOpenExternalTerminal,
      payload: input,
      headers: Headers.empty,
    });
    assert.isTrue(Exit.isSuccess(yield* Queue.take(responses)));
    assert.deepEqual(launched, [input]);
  }).pipe(Effect.scoped),
);
