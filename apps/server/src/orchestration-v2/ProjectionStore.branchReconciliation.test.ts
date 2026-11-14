import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const layerTest = ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory));

it.effect(
  "reconciles only the expected branch and workspace without changing thread activity",
  () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const later = DateTime.add(now, { minutes: 5 });
      const threadId = ThreadId.make("thread:branch-reconciliation");
      const providerInstanceId = ProviderInstanceId.make("codex");
      yield* store.apply({
        id: EventId.make("event:branch-reconciliation:create"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: {
          id: threadId,
          projectId: ProjectId.make("project:branch-reconciliation"),
          title: "Keep thread metadata",
          createdBy: "user",
          creationSource: "web",
          providerInstanceId,
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "original",
          worktreePath: "/copy",
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      });
      let memory = yield* store.getThreadProjection(threadId);
      for (const [id, expectedBranch, worktreePath, branch, expected] of [
        ["wrong-branch", "stale", "/copy", "wrong", "original"],
        ["wrong-workspace", "original", "/other", "wrong", "original"],
        ["rename", "original", "/copy", "renamed", "renamed"],
        ["stale-replay", "original", "/copy", "wrong", "renamed"],
      ] as const) {
        const event: OrchestrationV2DomainEvent = {
          id: EventId.make(`event:branch-reconciliation:${id}`),
          type: "thread.branch-reconciled",
          threadId,
          occurredAt: later,
          payload: { expectedBranch, worktreePath, branch },
        };
        memory = ProjectionStore.applyToProjection(memory, event);
        yield* store.apply(event);
        const persisted = yield* store.getThreadProjection(threadId);
        assert.equal(memory.thread.branch, expected);
        assert.equal(persisted.thread.branch, expected);
        assert.deepEqual(memory.thread.updatedAt, now);
        assert.deepEqual(persisted.thread.updatedAt, now);
        assert.equal(persisted.thread.worktreePath, "/copy");
        assert.equal(persisted.thread.title, "Keep thread metadata");
      }
    }).pipe(Effect.provide(layerTest)),
);
