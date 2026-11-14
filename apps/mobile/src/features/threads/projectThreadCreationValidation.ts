export function resolveProjectThreadCreationBranch(input: {
  readonly workspaceMode: "local" | "worktree" | "rift";
  readonly selectedBranch: string | null;
  readonly currentCheckoutBranch: string | null;
}): string | null {
  if (input.selectedBranch !== null) {
    return input.selectedBranch;
  }
  return input.workspaceMode === "local" ? input.currentCheckoutBranch : null;
}
