/**
 * Profiling — Intermediate DuckDB Stats Collection
 *
 * Collects memory snapshots, timing, and row counts during report execution.
 * Works with both CTE and temp_tables strategies to enable data-driven
 * comparison between execution modes.
 *
 * Zero overhead when disabled (profiling: false is the default).
 */

import { DuckDBConnection } from '@duckdb/node-api';
import { StepInfo } from './execution-strategy';

// ── Memory snapshot (maps to duckdb_memory() output) ──────────

export interface MemorySnapshot {
    tag: string;
    memoryUsageBytes: number;
    temporaryMemoryBytes: number;
}

// ── Per-step profile (temp_tables mode) ───────────────────────

export interface StepProfile {
    name: string;
    tableName: string;
    stepNumber: number;
    durationMs: number;
    rowCount: number;
    memoryBefore: MemorySnapshot[];
    memoryAfter: MemorySnapshot[];
    memoryDeltaBytes: number;
    sql?: string;
}

// ── CTE query profile ────────────────────────────────────────

export interface CTEQueryProfile {
    explainAnalyze: string;
    totalDurationMs: number;
    rowCount: number;
    memoryBefore: MemorySnapshot[];
    memoryAfter: MemorySnapshot[];
    memoryDeltaBytes: number;
}

// ── Top-level profile result ─────────────────────────────────

export interface ProfileResult {
    strategy: 'cte' | 'temp_tables';
    totalDurationMs: number;
    totalRows: number;
    memoryPeakBytes: number;
    /** Populated only for temp_tables mode */
    steps?: StepProfile[];
    /** Populated only for CTE mode */
    queryProfile?: CTEQueryProfile;
}

// ── Logger event ─────────────────────────────────────────────

export interface ProfileCompleteEvent {
    profile: ProfileResult;
}

// ── Helpers ──────────────────────────────────────────────────

/**
 * Query duckdb_memory() and return parsed snapshots.
 */
export async function queryMemorySnapshot(conn: DuckDBConnection): Promise<MemorySnapshot[]> {
    const result = await conn.run('SELECT * FROM duckdb_memory()');
    const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));
    const snapshots: MemorySnapshot[] = [];

    for (let chunkIndex = 0; chunkIndex < result.chunkCount; chunkIndex++) {
        const chunk = result.getChunk(chunkIndex);
        const rows = chunk.getRowObjects(columnNames);
        for (const row of rows) {
            snapshots.push({
                tag: String(row.tag ?? row.Tag ?? ''),
                memoryUsageBytes: Number(row.memory_usage_bytes ?? row.memory_usage ?? 0),
                temporaryMemoryBytes: Number(row.temporary_memory_bytes ?? row.temporary_memory ?? 0),
            });
        }
    }

    return snapshots;
}

/**
 * Sum total memory usage across all tags.
 */
export function sumMemoryBytes(snapshots: MemorySnapshot[]): number {
    return snapshots.reduce((acc, s) => acc + s.memoryUsageBytes, 0);
}

/**
 * Query row count from a table.
 */
export async function queryRowCount(conn: DuckDBConnection, tableName: string): Promise<number> {
    const result = await conn.run(`SELECT COUNT(*) as cnt FROM ${tableName}`);
    const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));
    const chunk = result.getChunk(0);
    const rows = chunk.getRowObjects(columnNames);
    return Number(rows[0]?.cnt ?? 0);
}

/**
 * Run EXPLAIN ANALYZE and return the output as a string.
 */
export async function runExplainAnalyze(conn: DuckDBConnection, sql: string): Promise<string> {
    const result = await conn.run(`EXPLAIN ANALYZE ${sql}`);
    const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));
    const lines: string[] = [];

    for (let chunkIndex = 0; chunkIndex < result.chunkCount; chunkIndex++) {
        const chunk = result.getChunk(chunkIndex);
        const rows = chunk.getRowObjects(columnNames);
        for (const row of rows) {
            // EXPLAIN ANALYZE typically returns rows with explain_key/explain_value or similar
            const values = Object.values(row);
            lines.push(values.join('\t'));
        }
    }

    return lines.join('\n');
}

// ── Factory for manual temp_tables profiling ─────────────────

/**
 * Create a profiling callback pair for temp_tables mode.
 *
 * Returns `beforeExecute` (call before the step loop) and `onStep`
 * (compose with your own onStep). After build, call `getProfile()`
 * to retrieve per-step profiling data.
 *
 * @example
 * ```typescript
 * const profiler = createProfilingCallback();
 * const result = await report.build({
 *     strategy: 'temp_tables',
 *     beforeExecute: profiler.beforeExecute,
 *     onStep: composeCallbacks(profiler.onStep, myCallback),
 * });
 * const steps = profiler.getStepProfiles();
 * ```
 */
export function createProfilingCallback(): {
    beforeExecute: (conn: DuckDBConnection) => Promise<void>;
    onStep: (info: StepInfo, conn: DuckDBConnection) => Promise<void>;
    getStepProfiles: () => StepProfile[];
} {
    const stepProfiles: StepProfile[] = [];
    let previousMemory: MemorySnapshot[] = [];
    let previousTime: number = 0;

    const beforeExecute = async (conn: DuckDBConnection) => {
        previousMemory = await queryMemorySnapshot(conn);
        previousTime = Date.now();
    };

    const onStep = async (info: StepInfo, conn: DuckDBConnection) => {
        const now = Date.now();
        const memoryAfter = await queryMemorySnapshot(conn);
        const rowCount = await queryRowCount(conn, info.tableName);

        stepProfiles.push({
            name: info.name,
            tableName: info.tableName,
            stepNumber: info.stepNumber,
            durationMs: now - previousTime,
            rowCount,
            memoryBefore: previousMemory,
            memoryAfter,
            memoryDeltaBytes: sumMemoryBytes(memoryAfter) - sumMemoryBytes(previousMemory),
            sql: info.sql,
        });

        previousMemory = memoryAfter;
        previousTime = now;
    };

    return {
        beforeExecute,
        onStep,
        getStepProfiles: () => [...stepProfiles],
    };
}
