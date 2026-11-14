import type { OrchestrationV2PlanStep } from "@t3tools/contracts";

/** The plan tool SoL-Pi registers; its steps render as T3's native task list. */
const PI_PLAN_TOOL = "update_plan";

const STEP_STATUS = {
  pending: "pending",
  in_progress: "running",
  completed: "completed",
} as const satisfies Record<string, OrchestrationV2PlanStep["status"]>;

const nonEmpty = (value: unknown): value is string =>
  typeof value === "string" && value.trim() !== "";

/**
 * `update_plan` arguments (`{ steps: [{ id, goal, status }] }`) as plan steps,
 * or null when the call is some other tool or its arguments don't fit, so the
 * caller keeps showing it as an ordinary tool row.
 */
export function piPlanSteps(
  toolName: string,
  args: unknown,
): ReadonlyArray<OrchestrationV2PlanStep> | null {
  if (toolName !== PI_PLAN_TOOL || typeof args !== "object" || args === null) return null;
  const steps = (args as { readonly steps?: unknown }).steps;
  if (!Array.isArray(steps) || steps.length === 0) return null;
  const mapped: Array<OrchestrationV2PlanStep> = [];
  for (const step of steps) {
    if (typeof step !== "object" || step === null) return null;
    const { id, goal, status } = step as Record<string, unknown>;
    if (!nonEmpty(id) || !nonEmpty(goal) || typeof status !== "string") return null;
    const mappedStatus = STEP_STATUS[status as keyof typeof STEP_STATUS];
    if (mappedStatus === undefined) return null;
    mapped.push({ id: id.trim(), text: goal.trim(), status: mappedStatus });
  }
  return mapped;
}
