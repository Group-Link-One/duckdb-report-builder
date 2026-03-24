/**
 * ReportWithContext - Fluent API for building multi-source reports
 *
 * This is the user-facing API that constructs a QueryPlan and executes it.
 *
 * Example usage:
 * ```typescript
 * const report = new ReportWithContext()
 *     .context({ from: new Date('2024-01-01'), until: new Date('2024-01-31') }, 'UTC')
 *     .load('readings', readingsProvider)
 *     .load('dc', deviceContextProvider)
 *     .pivot('readings', {
 *         on: 'serie_id',
 *         val: 'raw_value',
 *         cols: [
 *             { id: 2, alias: 'energy_in' },
 *             { id: 3, alias: 'energy_out' },
 *         ]
 *     })
 *     .join('readings', 'dc', { device_id: 'device_id', channel: 'channel' })
 *     .select(['timestamp', 'dc.device_id', 'energy_in', 'energy_out'])
 *     .filter('energy_in IS NOT NULL');
 *
 * const result = await report.build<EnergyReportRow>();
 * ```
 */

import { IDataSourceProvider, LoadContext } from '../providers/i-data-source-provider';
import {
    AggregationStrategy, ApplyEnrichmentTransform, CoarsenTransform, EnrichmentFormula, FilterSpec, JoinTransform, LocfTransform, OutputColumn, PivotTransform, PlanContext, QueryPlan, SourceSpec, TimeGranularity, TimezoneTransform, TransformSpec, validateQueryPlan, WindowFunctionSpec, WindowTransform
} from '../query-plan/query-plan';
import { FormatConfig, generateFormatCTE } from '../sql-generator/format-generator';
import { SQLGenerator } from '../sql-generator/sql-generator';
import { DuckDBQueryExecutor } from './duckdb-query-executor';
import { ExecutionOptions, StepInfo } from './execution-strategy';
import { ReportLogger, silentLogger } from './logger';

/**
 * Pivot configuration for fluent API
 */
export interface PivotConfig {
    on: string; // Column to pivot on (e.g., 'serie_id')
    val: string; // Column with values (e.g., 'raw_value')
    cols: Array<{
        id: number | string; // Value to match (e.g., serie_id=2)
        alias: string; // Output column (e.g., 'energy_in')
        locf?: number | null; // Optional LOCF lookback seconds
    }>;
    groupBy?: string[]; // Columns to group by (e.g., ['device_id', 'timestamp'])
    as?: string; // Optional output table rename
}

/**
 * LOCF configuration for fluent API
 */
export interface LocfConfig {
    baseTimeline?: string; // Timeline source alias; omit for in-place LOCF
    joinKeys: string[]; // Join keys (e.g., ['device_id', 'channel'])
    columns: string[]; // Columns to carry forward
    maxLookbackSeconds?: number | null;
    as?: string; // Optional output table rename
}

/**
 * Coarsen configuration for fluent API
 */
export interface CoarsenConfig {
    from: TimeGranularity; // Original granularity
    to: TimeGranularity; // Target granularity
    strategy: Record<string, AggregationStrategy>; // Column -> aggregation strategy
    timestampColumn?: string; // Default: 'timestamp'
    groupBy?: string[]; // Additional columns to group by (e.g., device_id, channel)
    as?: string; // Optional output table rename
}

/**
 * Apply enrichment configuration for fluent API
 */
export interface ApplyEnrichmentConfig {
    lookupSource: string; // Lookup/context source alias
    joinOn: string[]; // Keys to join on (e.g., ['device_id', 'channel'])
    formulas: Record<string, EnrichmentFormula>; // Column name -> enrichment formula
    as?: string; // Optional output table rename
}

/**
 * Timezone configuration for fluent API
 */
export interface TimezoneConfig {
    timestampColumns: string[]; // Columns to convert (e.g., ['timestamp', 'event_time'])
    timezone: string; // Target timezone (e.g., 'America/Sao_Paulo')
    as?: string; // Optional output table rename
}

/**
 * Window function configuration for fluent API
 */
export interface WindowConfig {
    partitionBy: string[]; // PARTITION BY columns (e.g., ['device_id', 'channel'])
    orderBy: Array<{ column: string; direction: 'ASC' | 'DESC' }>; // ORDER BY for window
    windowFunctions: WindowFunctionSpec[]; // Window functions to apply
    qualify?: string; // QUALIFY condition (e.g., 'rn = 1')
    as?: string; // Optional output table rename
}

/**
 * Result of building a report
 */
export interface ReportResult<T = any> {
    data: T[];
    sql: string;
    executionTimeMs: number;
}

/**
 * ReportWithContext - Fluent API builder
 */
export class ReportWithContext {
    private planContext?: PlanContext;
    private sources: SourceSpec[] = [];
    private transforms: TransformSpec[] = [];
    private outputColumns: OutputColumn[] = [];
    private outputFilters: FilterSpec[] = [];
    private outputOrderBy: Array<{ column: string; direction: 'ASC' | 'DESC' }> = [];
    private outputGroupBy: string[] = [];
    private formatConfig?: FormatConfig;
    private executor: DuckDBQueryExecutor;
    private _logger: ReportLogger = silentLogger;

    constructor() {
        this.executor = new DuckDBQueryExecutor();
    }

    /**
     * Set a logger for observability (timing, source loads, build completion).
     * Default is silent (no-op). Use `consoleLogger()` for console output.
     *
     * @example
     * ```typescript
     * import { consoleLogger } from 'duckdb-report-builder';
     *
     * new ReportWithContext()
     *     .logger(consoleLogger())
     *     .context({ ... })
     *     .load('readings', provider)
     *     .build();
     * ```
     */
    logger(logger: ReportLogger): this {
        this._logger = logger;
        return this;
    }

    /** Access the current logger (for providers that need it). */
    getLogger(): ReportLogger {
        return this._logger;
    }

    /**
     * Set the execution context (period, timezone, and arbitrary parameters)
     *
     * @param contextParams - Object with from, until, timezone, and any additional params
     * @returns this (for chaining)
     *
     * @example
     * ```typescript
     * new ReportWithContext()
     *     .context({
     *         from: new Date('2024-01-01'),
     *         until: new Date('2024-01-31'),
     *         timezone: 'America/Sao_Paulo',
     *         deviceIds: [100n, 200n],
     *         organizationId: 42
     *     })
     * ```
     */
    context(contextParams: { from: Date; until: Date; timezone?: string; [key: string]: any }): this {
        const { from, until, timezone = 'UTC', ...params } = contextParams;
        this.planContext = {
            period: { from, until },
            timezone,
            params,
        };
        return this;
    }

    /**
     * Set the time period and timezone (sugar for .context())
     *
     * @deprecated Use .context() instead for more flexibility
     */
    period(from: Date, until: Date, timezone: string = 'UTC'): this {
        return this.context({ from, until, timezone });
    }

    /**
     * Load a data source
     *
     * @param alias - Unique alias for this source
     * @param provider - Provider instance to load data from
     * @param filters - Optional source-specific filters
     * @returns this (for chaining)
     *
     * @example
     * ```typescript
     * new ReportWithContext()
     *     .context({ from, until, timezone: 'UTC', deviceIds: [100n, 200n] })
     *     .load('readings', new ClickHouseProvider(config))
     *     .load('contexts', new DeviceContextProvider())
     * ```
     */
    load(alias: string, provider: IDataSourceProvider, filters?: FilterSpec[]): this {
        this.sources.push({
            alias,
            provider,
            filters,
        });
        return this;
    }

    /**
     * Register data sources
     *
     * @param sources - Array of source specifications with provider and alias
     * @deprecated Use .load() instead for better ergonomics
     */
    source(sources: Array<{ provider: IDataSourceProvider; alias: string; filters?: FilterSpec[] }>): this {
        for (const src of sources) {
            this.sources.push({
                alias: src.alias,
                provider: src.provider,
                filters: src.filters,
            });
        }
        return this;
    }

    /**
     * Add a pivot transform
     *
     * @param sourceAlias - Source to pivot
     * @param config - Pivot configuration
     */
    pivot(sourceAlias: string, config: PivotConfig): this {
        const pivotTransform: PivotTransform = {
            type: 'pivot',
            sourceAlias,
            pivotColumn: config.on,
            valueColumn: config.val,
            columns: config.cols.map((col) => ({
                pivotValue: col.id,
                outputAlias: col.alias,
                locf:
                    col.locf !== undefined
                        ? {
                              enabled: true,
                              maxLookbackSeconds: col.locf,
                          }
                        : undefined,
            })),
            groupBy: config.groupBy,
            as: config.as,
        };
        this.transforms.push(pivotTransform);
        return this;
    }

    /**
     * Add an LOCF transform
     *
     * @param sourceAlias - Source to apply LOCF to
     * @param config - LOCF configuration
     */
    locf(sourceAlias: string, config: LocfConfig): this {
        const locfTransform: LocfTransform = {
            type: 'locf',
            sourceAlias,
            baseTimelineAlias: config.baseTimeline,
            joinKeys: config.joinKeys,
            columns: config.columns,
            maxLookbackSeconds: config.maxLookbackSeconds ?? null,
            as: config.as,
        };
        this.transforms.push(locfTransform);
        return this;
    }

    /**
     * Add a join transform
     *
     * @param leftAlias - Left source alias
     * @param rightAlias - Right source alias
     * @param onConditions - Join conditions (object with left column as key, right column as value)
     * @param joinType - Type of join (default: LEFT)
     */
    join(
        leftAlias: string,
        rightAlias: string,
        onConditions: Record<string, string>,
        joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' = 'LEFT'
    ): this {
        const joinTransform: JoinTransform = {
            type: 'join',
            leftAlias,
            rightAlias,
            joinType,
            onConditions: Object.entries(onConditions).map(([left, right]) => ({ left, right })),
        };
        this.transforms.push(joinTransform);
        return this;
    }

    /**
     * Add a coarsen transform
     *
     * Aggregates data from fine to coarse time granularity (e.g., minute -> hour).
     *
     * @param sourceAlias - Source to coarsen
     * @param config - Coarsen configuration
     *
     * @example
     * ```typescript
     * .coarsen('battery', {
     *     from: 'minute',
     *     to: 'hour',
     *     strategy: {
     *         voltage: 'avg',
     *         current: 'avg',
     *         soc: 'last'
     *     }
     * })
     * ```
     */
    coarsen(sourceAlias: string, config: CoarsenConfig): this {
        const coarsenTransform: CoarsenTransform = {
            type: 'coarsen',
            sourceAlias,
            from: config.from,
            to: config.to,
            strategy: config.strategy,
            timestampColumn: config.timestampColumn,
            groupBy: config.groupBy,
            as: config.as,
        };
        this.transforms.push(coarsenTransform);
        return this;
    }

    /**
     * Add an apply enrichment transform
     *
     * Joins a lookup table and applies enrichment formulas to compute derived columns.
     *
     * @param sourceAlias - Source to apply enrichment to
     * @param config - Enrichment configuration
     *
     * @example
     * ```typescript
     * .applyEnrichment('raw_readings', {
     *     lookupSource: 'device_contexts',
     *     joinOn: ['device_id', 'channel'],
     *     formulas: {
     *         adjusted_value: {
     *             formula: '(r.raw_value - c.offset) * c.multiplier'
     *         },
     *         consumption: {
     *             formula: 'r.daily_raw * c.multiplier'
     *         }
     *     }
     * })
     * ```
     */
    applyEnrichment(sourceAlias: string, config: ApplyEnrichmentConfig): this {
        const applyEnrichmentTransform: ApplyEnrichmentTransform = {
            type: 'apply_enrichment',
            sourceAlias,
            lookupSourceAlias: config.lookupSource,
            joinOn: config.joinOn,
            formulas: config.formulas,
            as: config.as,
        };
        this.transforms.push(applyEnrichmentTransform);
        return this;
    }

    /**
     * Add a timezone conversion transform
     *
     * Converts timestamp columns from UTC to a specific timezone.
     *
     * @param sourceAlias - Source to apply timezone conversion to
     * @param config - Timezone configuration
     *
     * @example
     * ```typescript
     * .timezone('raw_readings', {
     *     timestampColumns: ['timestamp', 'event_time'],
     *     timezone: 'America/Sao_Paulo'
     * })
     * ```
     */
    timezone(sourceAlias: string, config: TimezoneConfig): this {
        const timezoneTransform: TimezoneTransform = {
            type: 'timezone',
            sourceAlias,
            timestampColumns: config.timestampColumns,
            timezone: config.timezone,
            as: config.as,
        };
        this.transforms.push(timezoneTransform);
        return this;
    }

    /**
     * Add a window function transform
     *
     * Applies window functions (LAG, ROW_NUMBER, ARRAY_AGG, etc.) for advanced analytics.
     *
     * @param sourceAlias - Source to apply window functions to
     * @param config - Window configuration
     *
     * @example
     * ```typescript
     * // Calculate delta using LAG
     * .window('readings', {
     *     partitionBy: ['device_id', 'channel'],
     *     orderBy: [{ column: 'date', direction: 'ASC' }],
     *     windowFunctions: [{
     *         function: 'LAG',
     *         column: 'value',
     *         offset: 1,
     *         defaultValue: 0,
     *         outputAlias: 'prev_value'
     *     }]
     * })
     *
     * // Get latest row per device using ROW_NUMBER + QUALIFY
     * .window('readings', {
     *     partitionBy: ['device_id'],
     *     orderBy: [{ column: 'timestamp', direction: 'DESC' }],
     *     windowFunctions: [{
     *         function: 'ROW_NUMBER',
     *         outputAlias: 'rn'
     *     }],
     *     qualify: 'rn = 1'
     * })
     * ```
     */
    window(sourceAlias: string, config: WindowConfig): this {
        const windowTransform: WindowTransform = {
            type: 'window',
            sourceAlias,
            partitionBy: config.partitionBy,
            orderBy: config.orderBy,
            windowFunctions: config.windowFunctions,
            qualify: config.qualify,
            as: config.as,
        };
        this.transforms.push(windowTransform);
        return this;
    }

    /**
     * Select output columns
     *
     * @param columns - Array of column references or [expression, alias] tuples
     *   - string: column reference with optional dot notation (e.g., 'dc.device_name')
     *   - [string, string]: raw SQL expression with explicit alias (e.g., ['SUM(value)', 'total'])
     */
    select(columns: Array<string | [string, string]>): this {
        for (const col of columns) {
            if (Array.isArray(col)) {
                const [expression, alias] = col;
                this.outputColumns.push({
                    sourceAlias: '',
                    sourceColumn: expression,
                    outputAlias: alias,
                    isRawExpression: true,
                });
            } else {
                const { sourceAlias, sourceColumn, outputAlias } = this.parseColumnReference(col);
                this.outputColumns.push({ sourceAlias, sourceColumn, outputAlias });
            }
        }
        return this;
    }

    /**
     * Add GROUP BY clause
     *
     * @param columns - Columns to group by
     */
    groupBy(columns: string[]): this {
        this.outputGroupBy = columns;
        return this;
    }

    /**
     * Add a filter condition
     *
     * @param condition - SQL WHERE condition
     */
    filter(condition: string): this {
        this.outputFilters.push({ condition });
        return this;
    }

    /**
     * Add ORDER BY clause
     *
     * @param column - Column to order by
     * @param direction - Sort direction (default: ASC)
     */
    orderBy(column: string, direction: 'ASC' | 'DESC' = 'ASC'): this {
        this.outputOrderBy.push({ column, direction });
        return this;
    }

    /**
     * Apply formatting to output columns
     *
     * Formatting is applied as the final step before returning results.
     * Uses DuckDB's format() function for locale-specific number/date formatting.
     *
     * @param config - Format configuration with locale and column-specific rules
     * @returns this (for chaining)
     *
     * @example
     * ```typescript
     * new ReportWithContext([100n])
     *     .period(from, until, 'UTC')
     *     .source([...])
     *     .select(['device_id', 'consumption', 'cost', 'timestamp'])
     *     .format({
     *         locale: 'pt-BR',
     *         columns: {
     *             consumption: { decimalPlaces: 3, unit: 'm³' },
     *             cost: { decimalPlaces: 2, currency: 'R$' },
     *             timestamp: { dateFormat: '%d/%m/%Y %H:%M:%S' }
     *         }
     *     })
     *     .build();
     * ```
     */
    format(config: FormatConfig): this {
        this.formatConfig = config;
        return this;
    }

    /**
     * Build the query plan and execute it
     *
     * @param options - Execution options (strategy, callbacks, etc.)
     * @returns Result with data, SQL, and execution time
     */
    async build<T = any>(options?: ExecutionOptions): Promise<ReportResult<T>> {
        const startTime = Date.now();
        const strategy = options?.strategy || 'cte';
        const { plan, sourceTableMap } = await this.preparePlan();
        const prepareMs = Date.now() - startTime;

        let data: any[];
        let sql: string;

        const tExec = Date.now();
        if (strategy === 'temp_tables') {
            const result = await this.executeTempTableMode(plan, sourceTableMap, options);
            data = result.data;
            sql = result.sql;
        } else {
            // CTE mode (default)
            const generator = new SQLGenerator({ formatted: true });
            const generated = generator.generate(plan, sourceTableMap);
            sql = generated.sql;

            // Apply formatting if configured
            if (this.formatConfig) {
                // Create temp table from main query result
                const mainResultTable = 'temp_report_result';
                await this.executor.getConnection().run(`CREATE TEMP TABLE ${mainResultTable} AS ${sql}`);

                // Generate formatting SQL
                const formatSQL = generateFormatCTE(mainResultTable, this.formatConfig);
                data = await this.executor.runQuery(formatSQL);

                // Cleanup
                await this.executor.getConnection().run(`DROP TABLE IF EXISTS ${mainResultTable}`);

                // Update SQL for debugging (show both queries)
                sql = `-- Main query:\n${sql}\n\n-- Formatting:\n${formatSQL}`;
            } else {
                data = await this.executor.runQuery(sql);
            }
        }
        const execMs = Date.now() - tExec;

        const executionTimeMs = Date.now() - startTime;
        this._logger.onBuildComplete?.({
            prepareMs,
            executeMs: execMs,
            totalMs: executionTimeMs,
            rows: data.length,
            strategy,
        });

        return {
            data: data as T[],
            sql,
            executionTimeMs,
        };
    }

    private async preparePlan(): Promise<{ plan: QueryPlan; sourceTableMap: Map<string, string> }> {
        if (!this.planContext) {
            throw new Error('Context must be set before building report (use .context() or .period())');
        }

        const plan: QueryPlan = {
            context: this.planContext,
            sources: this.sources,
            transforms: this.transforms,
            output: {
                columns: this.outputColumns,
                filters: this.outputFilters.length > 0 ? this.outputFilters : undefined,
                orderBy: this.outputOrderBy.length > 0 ? this.outputOrderBy : undefined,
                groupBy: this.outputGroupBy.length > 0 ? this.outputGroupBy : undefined,
            },
        };

        validateQueryPlan(plan);

        const t0 = Date.now();
        await this.executor.init();
        const initMs = Date.now() - t0;

        const sourceTableMap = new Map<string, string>();
        const timings: Array<{ alias: string; provider: string; ms: number }> = [];

        for (const source of this.sources) {
            const loadContext: LoadContext = {
                connection: this.executor.getConnection(),
                period: this.planContext.period,
                timezone: this.planContext.timezone,
                params: this.planContext.params,
                tables: new Map(sourceTableMap),
                logger: this._logger,
            };
            const tSrc = Date.now();
            const tableName = await source.provider.load(loadContext);
            const srcMs = Date.now() - tSrc;
            timings.push({ alias: source.alias, provider: source.provider.name ?? source.provider.constructor.name, ms: srcMs });
            sourceTableMap.set(source.alias, tableName);
        }

        // Emit lifecycle events
        this._logger.onInit?.({ durationMs: initMs });
        for (const t of timings) {
            this._logger.onSourceLoad?.({ alias: t.alias, provider: t.provider, durationMs: t.ms });
        }

        return { plan, sourceTableMap };
    }

    /**
     * Execute in temp table mode with callbacks
     */
    private async executeTempTableMode(
        plan: QueryPlan,
        sourceTableMap: Map<string, string>,
        options?: ExecutionOptions
    ): Promise<{ data: any[]; sql: string }> {
        const generator = new SQLGenerator({ formatted: true });

        // Create a working copy of the table map
        const workingTableMap = new Map<string, string>(sourceTableMap);

        const tempTablePlan = generator.generateTempTablePlan(plan, workingTableMap);
        const connection = this.executor.getConnection();
        const executedSQL: string[] = [];

        try {
            // Execute each step with callbacks
            for (let i = 0; i < tempTablePlan.steps.length; i++) {
                const step = tempTablePlan.steps[i];

                // Execute the step
                await connection.run(step.sql);
                executedSQL.push(step.sql);

                // Update table map to use the new temp table
                // Temp tables use same names as CTEs so no prefix mapping needed
                workingTableMap.set(step.tableName, step.tableName);

                // Fire onStep callback
                if (options?.onStep) {
                    const stepInfo: StepInfo = {
                        name: step.name,
                        tableName: step.tableName,
                        stepNumber: i,
                        totalSteps: tempTablePlan.steps.length,
                        position: 'between_transforms',
                        sql: step.sql,
                    };
                    await options.onStep(stepInfo, connection);
                }

                // Handle SQL injection
                if (options?.injectSQL) {
                    const stepInfo: StepInfo = {
                        name: step.name,
                        tableName: step.tableName,
                        stepNumber: i,
                        totalSteps: tempTablePlan.steps.length,
                        position: 'between_transforms',
                        sql: step.sql,
                    };
                    const newTableName = await options.injectSQL(stepInfo, connection);
                    if (newTableName) {
                        // Collect all aliases pointing to the old table
                        const aliasesToUpdate: string[] = [];
                        for (const [alias, tableName] of workingTableMap.entries()) {
                            if (tableName === step.tableName) {
                                aliasesToUpdate.push(alias);
                            }
                        }

                        // Update all collected aliases to point to the new table
                        for (const alias of aliasesToUpdate) {
                            workingTableMap.set(alias, newTableName);
                        }
                    }
                }
            }

            // Build a list of logical CTE names (not the physical temp table names)
            // This is used for determining the base table in final SELECT
            // The workingTableMap then provides the mapping to actual tables
            const logicalCTENames: string[] = tempTablePlan.tempTables.map((name) => name);

            // Regenerate final SELECT with the updated table map
            const sqlGen = new SQLGenerator({ formatted: true });
            const finalSelectOnly = sqlGen.generateFinalSelect(plan, workingTableMap, logicalCTENames);

            executedSQL.push(finalSelectOnly);

            // Apply formatting if configured
            let data: any[];
            let formattedResultTable: string | null = null;

            if (this.formatConfig) {
                // Create temp table from final SELECT
                formattedResultTable = 'temp_formatted_result';
                const createFormattedSQL = `CREATE TEMP TABLE ${formattedResultTable} AS ${finalSelectOnly}`;
                await connection.run(createFormattedSQL);
                executedSQL.push(createFormattedSQL);

                // Generate and execute formatting SQL
                const formatSQL = generateFormatCTE(formattedResultTable, this.formatConfig);
                executedSQL.push(formatSQL);
                data = await this.executor.runQuery(formatSQL);

                // Add formatted table to cleanup list (will be handled in finally)
                tempTablePlan.tempTables.push(formattedResultTable);
            } else {
                data = await this.executor.runQuery(finalSelectOnly);
            }

            // Return combined SQL for debugging
            const combinedSQL = executedSQL.join(';\n\n');

            return { data, sql: combinedSQL };
        } finally {
            // Cleanup temp tables even if an error occurred
            if (!options?.keepTempTables) {
                for (const tableName of tempTablePlan.tempTables) {
                    try {
                        await connection.run(`DROP TABLE IF EXISTS ${tableName}`);
                    } catch {
                        // Ignore cleanup errors
                    }
                }
            }
        }
    }

    /**
     * Generate SQL without executing
     *
     * @returns Generated SQL string
     */
    async toSQL(): Promise<string> {
        const { plan, sourceTableMap } = await this.preparePlan();
        const generator = new SQLGenerator({ formatted: true });
        const { sql } = generator.generate(plan, sourceTableMap);
        return sql;
    }

    /**
     * Close the DuckDB connection
     */
    async close(): Promise<void> {
        await this.executor.close();
    }

    /**
     * Parse column reference into source alias, source column, and output alias
     *
     * Supports:
     * - Simple column: 'timestamp' -> { sourceAlias: 'readings', sourceColumn: 'timestamp', outputAlias: 'timestamp' }
     * - Qualified column: 'dc.device_name' -> { sourceAlias: 'dc', sourceColumn: 'device_name', outputAlias: 'device_name' }
     */
    private parseColumnReference(ref: string): {
        sourceAlias: string;
        sourceColumn: string;
        outputAlias: string;
    } {
        const parts = ref.split('.');

        if (parts.length === 1) {
            // Simple column - use the last transform's output, or first source as default
            let sourceAlias: string;

            if (this.transforms.length > 0) {
                // Use the output of the last transform
                const lastTransform = this.transforms[this.transforms.length - 1];
                sourceAlias = this.getTransformOutputAlias(lastTransform);
            } else {
                // No transforms - use first source
                sourceAlias = this.sources.length > 0 ? this.sources[0].alias : 'unknown';
            }

            return {
                sourceAlias,
                sourceColumn: parts[0],
                outputAlias: parts[0],
            };
        } else if (parts.length === 2) {
            // Qualified column
            return {
                sourceAlias: parts[0],
                sourceColumn: parts[1],
                outputAlias: parts[1],
            };
        } else {
            throw new Error(`Invalid column reference: ${ref}`);
        }
    }

    /**
     * Get the output alias for a transform
     *
     * Returns the CTE name that will be generated for this transform.
     */
    private getTransformOutputAlias(transform: TransformSpec): string {
        switch (transform.type) {
            case 'pivot':
                return transform.as || `${transform.sourceAlias}_pivoted`;
            case 'locf':
                return transform.as || `${transform.sourceAlias}_locf`;
            case 'coarsen':
                return transform.as || `${transform.sourceAlias}_coarsened`;
            case 'apply_enrichment':
                return transform.as || `${transform.sourceAlias}_enriched`;
            case 'timezone':
                return transform.as || `${transform.sourceAlias}_tz`;
            case 'window':
                return transform.as || `${transform.sourceAlias}_windowed`;
            case 'join':
                // Joins don't create a new CTE, they reference both sides
                return transform.leftAlias;
            case 'filter':
                // Filters don't create a new CTE
                return transform.sourceAlias;
            default:
                return 'unknown';
        }
    }
}
