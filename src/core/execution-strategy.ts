/**
 * Execution Strategy for Report Building
 *
 * Defines how transforms are executed: as a single CTE chain or as
 * individual temp tables with callbacks between each step.
 */

import { DuckDBConnection } from '@duckdb/node-api';

export type ExecutionStrategy = 'cte' | 'temp_tables';
export type CallbackPosition = 'after_source_load' | 'between_transforms' | 'before_output';

/**
 * Step information passed to callbacks
 */
export interface StepInfo {
    /** Step name (e.g., 'source:readings', 'transform:applyEnrichment') */
    name: string;

    /** Current table name */
    tableName: string;

    /** Step number in the pipeline (0-indexed) */
    stepNumber: number;

    /** Total number of steps */
    totalSteps: number;

    /** Position in the pipeline */
    position: CallbackPosition;

    /** SQL that was executed to create this step (if temp table mode) */
    sql?: string;
}

/**
 * Execution options for building reports
 */
export interface ExecutionOptions {
    /** Strategy to use: CTE (single query) or temp tables (step-by-step) */
    strategy: ExecutionStrategy;

    /**
     * Callback that fires IN BETWEEN each step (not after all!)
     * Use for progress tracking, validation, or debugging.
     *
     * @param stepInfo Information about the current step
     * @param connection DuckDB connection (can query the current table)
     */
    onStep?: (stepInfo: StepInfo, connection: DuckDBConnection) => Promise<void>;

    /**
     * Inject custom SQL IN BETWEEN transforms.
     * This allows you to modify data or add computed columns between pipeline steps.
     *
     * @param stepInfo Information about the current step
     * @param connection DuckDB connection
     * @returns New table name if you modified data, or void to continue with existing table
     *
     * @example
     * ```typescript
     * injectSQL: async (info, conn) => {
     *   if (info.name === 'transform:applyEnrichment') {
     *     const newTable = `${info.tableName}_with_cost`;
     *     await conn.run(`
     *       CREATE TEMP TABLE ${newTable} AS
     *       SELECT *, adjusted_consumption * 5.5 AS cost
     *       FROM ${info.tableName}
     *     `);
     *     return newTable; // Pipeline continues with this table
     *   }
     * }
     * ```
     */
    injectSQL?: (stepInfo: StepInfo, connection: DuckDBConnection) => Promise<string | void>;

    /**
     * Keep temporary tables after execution for debugging.
     * Only applicable in temp_tables mode.
     */
    keepTempTables?: boolean;
}

/**
 * Compose multiple step callbacks into a single callback
 *
 * @example
 * ```typescript
 * const combined = composeCallbacks(
 *   createProgressCallback('WaterReport'),
 *   createNullValidationCallback({ 'applyEnrichment': ['adjusted_consumption'] })
 * );
 * ```
 */
export function composeCallbacks(
    ...callbacks: Array<(info: StepInfo, conn: DuckDBConnection) => Promise<void>>
): (info: StepInfo, conn: DuckDBConnection) => Promise<void> {
    return async (info: StepInfo, conn: DuckDBConnection) => {
        for (const callback of callbacks) {
            await callback(info, conn);
        }
    };
}

/**
 * Create a progress logging callback
 */
export function createProgressCallback(reportName: string) {
    return async (info: StepInfo, _conn: DuckDBConnection) => {
        const progress = Math.round(((info.stepNumber + 1) / info.totalSteps) * 100);
        console.log(`[${reportName}] ${progress}% - ${info.name} (${info.tableName})`);
    };
}

/**
 * Create a null validation callback
 * Validates that specified columns don't contain NULL values at specific steps
 */
export function createNullValidationCallback(
    validations: Record<string, string[]>
): (info: StepInfo, conn: DuckDBConnection) => Promise<void> {
    return async (info: StepInfo, conn: DuckDBConnection) => {
        const columns = validations[info.name];
        if (!columns || columns.length === 0) return;

        for (const column of columns) {
            const result = await conn.run(`
                SELECT COUNT(*) as null_count
                FROM ${info.tableName}
                WHERE ${column} IS NULL
            `);
            const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));
            const chunk = result.getChunk(0);
            const rows = chunk.getRowObjects(columnNames);
            const nullCount = Number(rows[0]?.null_count ?? 0);
            if (nullCount > 0) {
                throw new Error(
                    `Validation failed at step '${info.name}': ` +
                        `Column '${column}' has ${nullCount} NULL values in table '${info.tableName}'`
                );
            }
        }
    };
}

/**
 * Create a row count logging callback
 */
export function createRowCountCallback() {
    return async (info: StepInfo, conn: DuckDBConnection) => {
        const result = await conn.run(`SELECT COUNT(*) as row_count FROM ${info.tableName}`);
        const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));
        const chunk = result.getChunk(0);
        const rows = chunk.getRowObjects(columnNames);
        const rowCount = Number(rows[0]?.row_count ?? 0);
        console.log(`[${info.name}] Row count: ${rowCount.toLocaleString()}`);
    };
}

/**
 * Create a sample data logging callback (useful for debugging)
 */
export function createSampleDataCallback(limit: number = 3) {
    return async (info: StepInfo, conn: DuckDBConnection) => {
        const result = await conn.run(`SELECT * FROM ${info.tableName} LIMIT ${limit}`);
        const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));
        const chunk = result.getChunk(0);
        const rows = chunk.getRowObjects(columnNames);
        console.log(`[${info.name}] Sample data (${limit} rows):`, rows);
    };
}
