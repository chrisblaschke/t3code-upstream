import {
  contextUsageBudget,
  latestContextUsage,
} from "@t3tools/client-runtime/state/composer-meters";
import { pickUsageGauge } from "@t3tools/client-runtime/state/thread-usage-limits";
import type {
  EnvironmentId,
  ProviderDriverKind,
  ThreadId,
  UsageLimitsReport,
} from "@t3tools/contracts";
import { memo, useMemo } from "react";
import { Pressable, View } from "react-native";
import { Circle, Svg } from "react-native-svg";
import { withUniwind } from "uniwind";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { useThreadProjection } from "../../state/use-thread-detail";

const ThemedCircle = withUniwind(Circle);

const RING_SIZE = 14;
const RING_STROKE = 2;
const RING_RADIUS = (RING_SIZE - RING_STROKE) / 2;
const RING_CIRCUMFERENCE = 2 * Math.PI * RING_RADIUS;

function formatPercent(value: number): string {
  return value < 10 ? `${value.toFixed(1).replace(/\.0$/, "")}%` : `${Math.round(value)}%`;
}

function ContextRing(props: { readonly percent: number; readonly colorClassName: string }) {
  const filled = Math.max(0, Math.min(100, props.percent));
  return (
    <Svg width={RING_SIZE} height={RING_SIZE} accessible={false}>
      <ThemedCircle
        cx={RING_SIZE / 2}
        cy={RING_SIZE / 2}
        r={RING_RADIUS}
        colorClassName="accent-border"
        stroke="currentColor"
        strokeWidth={RING_STROKE}
        fill="none"
      />
      <ThemedCircle
        cx={RING_SIZE / 2}
        cy={RING_SIZE / 2}
        r={RING_RADIUS}
        colorClassName={props.colorClassName}
        stroke="currentColor"
        strokeWidth={RING_STROKE}
        strokeLinecap="round"
        strokeDasharray={RING_CIRCUMFERENCE}
        strokeDashoffset={RING_CIRCUMFERENCE * (1 - filled / 100)}
        fill="none"
        transform={`rotate(-90 ${RING_SIZE / 2} ${RING_SIZE / 2})`}
      />
    </Svg>
  );
}

/**
 * The desktop composer's context meter and usage gauge, side by side: how full
 * the context is (toward the point the agent compacts, for Pi) and the
 * thread model's tightest usage-limit window. Only this row re-renders as the
 * thread streams; tapping it opens the full usage limits when offered.
 */
export const ComposerMeters = memo(function ComposerMeters(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly model: string | null;
  readonly driver: ProviderDriverKind | undefined;
  readonly usageLimits: UsageLimitsReport | null;
  readonly onPress?: (() => void) | undefined;
}) {
  const thread = useThreadProjection({
    environmentId: props.environmentId,
    threadId: props.threadId,
  });
  const projection = thread?.projection ?? null;
  const providerTurns = projection?.providerTurns;
  const providerThreads = projection?.providerThreads;
  const activeProviderThreadId = projection?.thread.activeProviderThreadId;
  const contextPercent = useMemo(() => {
    if (!providerTurns || !providerThreads) return null;
    const usage = latestContextUsage({
      providerTurns,
      providerThreads,
      activeProviderThreadId: activeProviderThreadId ?? null,
    });
    return usage === null ? null : contextUsageBudget(usage).usedPercentage;
  }, [providerTurns, providerThreads, activeProviderThreadId]);
  const gauge = useMemo(
    () => pickUsageGauge(props.usageLimits, props.model, props.driver),
    [props.usageLimits, props.model, props.driver],
  );
  if (contextPercent === null && gauge === null) return null;

  const contextFull = contextPercent !== null && contextPercent > 90;
  const gaugeUsed = gauge === null ? null : Math.round(gauge.window.usedPercent);
  const gaugeTone =
    gaugeUsed === null || gaugeUsed < 80
      ? "text-foreground-muted"
      : gaugeUsed >= 95
        ? "text-danger-foreground"
        : "text-warning-foreground";
  const labels = [
    contextPercent !== null ? `Context ${formatPercent(contextPercent)} used` : null,
    gauge !== null && gaugeUsed !== null
      ? `${gauge.account.label} ${gauge.window.label} limit ${gaugeUsed}% used`
      : null,
  ].filter(Boolean);

  return (
    <Pressable
      accessibilityRole={props.onPress ? "button" : undefined}
      accessibilityLabel={labels.join(", ")}
      accessibilityHint={props.onPress ? "Shows usage limits" : undefined}
      className="h-11 shrink-0 flex-row items-center gap-2.5 rounded-xl px-1.5 active:bg-subtle"
      disabled={!props.onPress}
      onPress={props.onPress}
    >
      {contextPercent !== null ? (
        <View className="flex-row items-center gap-1">
          <ContextRing
            percent={contextPercent}
            colorClassName={contextFull ? "accent-danger-foreground" : "accent-icon-muted"}
          />
          <Text
            className={
              contextFull
                ? "text-xs tabular-nums text-danger-foreground"
                : "text-xs tabular-nums text-foreground-muted"
            }
          >
            {formatPercent(contextPercent)}
          </Text>
        </View>
      ) : null}
      {gauge !== null && gaugeUsed !== null ? (
        <View className="flex-row items-center gap-1">
          <SymbolView
            name="chart.bar.xaxis"
            size={13}
            tintColorClassName={
              gaugeUsed >= 95
                ? "accent-danger-foreground"
                : gaugeUsed >= 80
                  ? "accent-warning-foreground"
                  : "accent-icon-muted"
            }
            type="monochrome"
          />
          <Text className={`text-xs tabular-nums ${gaugeTone}`}>
            {`${gauge.window.label} ${gaugeUsed}%`}
          </Text>
        </View>
      ) : null}
    </Pressable>
  );
});
