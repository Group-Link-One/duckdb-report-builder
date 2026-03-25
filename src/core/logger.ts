/**
 * Report Builder Logger
 *
 * Pluggable observability layer. All lifecycle events flow through this interface,
 * allowing consumers to hook into logging, metrics, tracing, or any custom sink.
 *
 * Default: silent (no-op). Use `consoleLogger()` for console output.
 *
 * @example
 * ```typescript
 * // Console logging
 * new ReportWithContext()
 *     .logger(consoleLogger())
 *     .context({ ... })
 *
 * // Custom logger (e.g., structured JSON)
 * new ReportWithContext()
 *     .logger({
 *         onInit: (ms) => metrics.histogram('report.init_ms', ms),
 *         onSourceLoad: (e) => metrics.histogram('report.source_ms', e.durationMs, { source: e.alias }),
 *         onBuildComplete: (e) => metrics.histogram('report.total_ms', e.totalMs, { rows: e.rows }),
 *     })
 *
 * // Selective override — only log slow sources
 * new ReportWithContext()
 *     .logger({
 *         onSourceLoad: (e) => {
 *             if (e.durationMs > 1000) console.warn(`Slow source: ${e.alias} took ${e.durationMs}ms`);
 *         },
 *     })
 * ```
 */

// ── Event payloads ──────────────────────────────────────────────

export interface InitEvent {
    durationMs: number;
}

export interface SourceLoadEvent {
    alias: string;
    provider: string;
    durationMs: number;
}

export interface BuildCompleteEvent {
    prepareMs: number;
    executeMs: number;
    totalMs: number;
    rows: number;
    strategy: 'cte' | 'temp_tables';
}

export interface ProviderEvent {
    provider: string;
    tableName: string;
    message: string;
    durationMs?: number;
    meta?: Record<string, unknown>;
}

export type { ProfileCompleteEvent } from './profiling';

// ── Logger interface ────────────────────────────────────────────

/**
 * All methods are optional — implement only the events you care about.
 */
export interface ReportLogger {
    /** DuckDB instance initialized */
    onInit?(event: InitEvent): void;

    /** A data source finished loading into DuckDB */
    onSourceLoad?(event: SourceLoadEvent): void;

    /** Report build() completed (prepare + execute) */
    onBuildComplete?(event: BuildCompleteEvent): void;

    /** Generic provider-level event (e.g., CSV load timing, timeline generation) */
    onProviderEvent?(event: ProviderEvent): void;

    /** Profiling data collected (only fires when profiling is enabled) */
    onProfileComplete?(event: import('./profiling').ProfileCompleteEvent): void;
}

// ── Built-in loggers ────────────────────────────────────────────

/** No-op logger — the default when none is configured. */
export const silentLogger: ReportLogger = Object.freeze({});

/**
 * Console logger with optional prefix.
 *
 * @param prefix - Label prepended to each line (default: "report-builder")
 */
export function consoleLogger(prefix = 'report-builder'): Required<ReportLogger> {
    return {
        onInit({ durationMs }) {
            console.log(`[${prefix}] init: ${durationMs}ms`);
        },
        onSourceLoad({ alias, provider, durationMs }) {
            console.log(`[${prefix}] load "${alias}" (${provider}): ${durationMs}ms`);
        },
        onBuildComplete({ prepareMs, executeMs, totalMs, rows, strategy }) {
            console.log(
                `[${prefix}] prepare: ${prepareMs}ms | duckdb-exec: ${executeMs}ms | total: ${totalMs}ms | rows: ${rows} | strategy: ${strategy}`,
            );
        },
        onProviderEvent({ provider, tableName, message }) {
            console.log(`[${prefix}:${provider}] ${tableName}: ${message}`);
        },
        onProfileComplete({ profile }) {
            const peakMB = (profile.memoryPeakBytes / 1024 / 1024).toFixed(2);
            console.log(
                `[${prefix}] profile: strategy=${profile.strategy} | peak=${peakMB}MB | rows=${profile.totalRows} | total=${profile.totalDurationMs}ms`,
            );
            if (profile.steps) {
                for (const step of profile.steps) {
                    const deltaMB = (step.memoryDeltaBytes / 1024 / 1024).toFixed(2);
                    console.log(
                        `[${prefix}]   step ${step.stepNumber}: ${step.name} | ${step.durationMs}ms | rows=${step.rowCount} | mem-delta=${deltaMB}MB`,
                    );
                }
            }
            if (profile.queryProfile) {
                const deltaMB = (profile.queryProfile.memoryDeltaBytes / 1024 / 1024).toFixed(2);
                console.log(`[${prefix}]   cte mem-delta=${deltaMB}MB`);
                console.log(`[${prefix}]   explain-analyze:\n${profile.queryProfile.explainAnalyze}`);
            }
        },
    };
}
