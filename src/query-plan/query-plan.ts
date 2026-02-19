/**
 * Query Plan IR (Intermediate Representation)
 *
 * This module defines the core IR for the report builder system.
 * The IR represents the complete specification of a multi-source report
 * as a data structure (not SQL), decoupling "what to do" from "how to execute."
 */

import { IDataSourceProvider } from '../providers/i-data-source-provider';

/**
 * Plan context - defines the query execution context
 */
export interface PlanContext {
    period: {
        from: Date;
        until: Date;
    };
    timezone: string;
    params: Record<string, any>;
}

/**
 * Data source registration
 */
export interface SourceSpec {
    alias: string; // Unique name for this source
    provider: IDataSourceProvider; // Provider instance
    filters?: FilterSpec[]; // Source-specific filters
}

/**
 * Filter specification
 */
export interface FilterSpec {
    condition: string; // SQL WHERE condition (e.g., "energy_in IS NOT NULL")
}

/**
 * Pivot transform - converts rows to columns
 */
export interface PivotTransform {
    type: 'pivot';
    sourceAlias: string;
    pivotColumn: string; // Column to pivot on (e.g., 'serie_id')
    valueColumn: string; // Column with values (e.g., 'raw_value')
    columns: Array<{
        pivotValue: number | string; // Value to match (e.g., serie_id=2)
        outputAlias: string; // Output column (e.g., 'energy_in')
        locf?: {
            enabled: boolean;
            maxLookbackSeconds: number | null;
        };
    }>;
    groupBy?: string[]; // Columns to group by (e.g., ['device_id', 'timestamp'])
    as?: string; // Optional output table rename
}

/**
 * LOCF (Last Observation Carried Forward) transform
 * Fills gaps in data by carrying forward the last observed value
 */
export interface LocfTransform {
    type: 'locf';
    sourceAlias: string; // Source to apply LOCF to
    baseTimelineAlias: string; // Timeline to fill (e.g., generated 24-hour timeline)
    joinKeys: string[]; // Join keys (e.g., ['device_id', 'channel'])
    columns: string[]; // Columns to carry forward
    maxLookbackSeconds: number | null; // Maximum time to look back (null = unlimited)
}

/**
 * Join transform - joins two sources
 */
export interface JoinTransform {
    type: 'join';
    leftAlias: string;
    rightAlias: string;
    joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
    onConditions: Array<{ left: string; right: string }>;
}

/**
 * Filter transform - filters rows
 */
export interface FilterTransform {
    type: 'filter';
    sourceAlias: string;
    condition: string;
}

/**
 * Time granularity options for coarsening
 */
export type TimeGranularity = 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month';

/**
 * Aggregation strategy for coarsening
 */
export type AggregationStrategy = 'sum' | 'avg' | 'min' | 'max' | 'first' | 'last' | 'count';

/**
 * Coarsen transform - aggregates data from fine to coarse time granularity
 * Example: minute -> hour, hour -> day
 */
export interface CoarsenTransform {
    type: 'coarsen';
    sourceAlias: string; // Source table to coarsen
    from: TimeGranularity; // Original granularity
    to: TimeGranularity; // Target granularity
    strategy: Record<string, AggregationStrategy>; // Column -> aggregation strategy
    timestampColumn?: string; // Default: 'timestamp'
    groupBy?: string[]; // Additional columns to group by (e.g., device_id, channel)
    as?: string; // Optional output table rename
}

/**
 * Enrichment formula for apply_enrichment transform
 */
export interface EnrichmentFormula {
    formula: string; // SQL formula using lookup table columns
}

/**
 * Apply enrichment transform - joins a lookup table and applies enrichment formulas
 * Applies context-specific calculations to source data (e.g., offsets, multipliers, derived columns)
 */
export interface ApplyEnrichmentTransform {
    type: 'apply_enrichment';
    sourceAlias: string; // Source table with raw data
    lookupSourceAlias: string; // Lookup/context table
    joinOn: string[]; // Keys to join on (e.g., ['device_id', 'channel'])
    formulas: Record<string, EnrichmentFormula>; // Column name -> enrichment formula
    as?: string; // Optional output table rename
}

/**
 * Timezone transform - converts timestamp columns to a specific timezone
 * Used to convert UTC timestamps to local time (e.g., "America/Sao_Paulo")
 */
export interface TimezoneTransform {
    type: 'timezone';
    sourceAlias: string; // Source table to apply timezone conversion
    timestampColumns: string[]; // Columns to convert (e.g., ['timestamp', 'event_time'])
    timezone: string; // Target timezone (e.g., 'America/Sao_Paulo')
    as?: string; // Optional output table rename
}

/**
 * Window function specification
 */
export interface WindowFunctionSpec {
    function: 'LAG' | 'LEAD' | 'ROW_NUMBER' | 'RANK' | 'FIRST_VALUE' | 'LAST_VALUE' | 'ARRAY_AGG';
    column?: string; // Column to operate on (not needed for ROW_NUMBER)
    offset?: number; // Offset for LAG/LEAD (default: 1)
    defaultValue?: any; // Default value for LAG/LEAD when no previous row
    outputAlias: string; // Output column name
    orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>; // ORDER BY for ARRAY_AGG
}

/**
 * Window transform - applies window functions (LAG, ROW_NUMBER, ARRAY_AGG, etc.)
 * Used for delta calculations, latest value selection, and aggregations
 *
 * Example: Calculate daily delta using LAG
 *   LAG(value, 1, 0) OVER (PARTITION BY device_id ORDER BY date)
 */
export interface WindowTransform {
    type: 'window';
    sourceAlias: string; // Source table to apply window functions
    partitionBy: string[]; // PARTITION BY columns (e.g., ['device_id', 'channel'])
    orderBy: Array<{ column: string; direction: 'ASC' | 'DESC' }>; // ORDER BY for window
    windowFunctions: WindowFunctionSpec[]; // Window functions to apply
    qualify?: string; // QUALIFY condition (e.g., 'rn = 1' for latest row)
    as?: string; // Optional output table rename
}

/**
 * Union of all transform types
 */
export type TransformSpec =
    | PivotTransform
    | LocfTransform
    | JoinTransform
    | FilterTransform
    | CoarsenTransform
    | ApplyEnrichmentTransform
    | TimezoneTransform
    | WindowTransform;

/**
 * Output column specification
 */
export interface OutputColumn {
    sourceAlias: string; // Source table/CTE alias
    sourceColumn: string; // Column name in source
    outputAlias: string; // Output column name
    isRawExpression?: boolean; // If true, sourceColumn is a raw SQL expression (not quoted)
}

/**
 * Output specification - defines the final result structure
 */
export interface OutputSpec {
    columns: OutputColumn[];
    filters?: FilterSpec[];
    orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
    groupBy?: string[];
}

/**
 * The complete query specification
 * This is the root IR object that represents an entire multi-source report
 */
export interface QueryPlan {
    context: PlanContext;
    sources: SourceSpec[];
    transforms: TransformSpec[];
    output: OutputSpec;
}

/**
 * Type guard for PivotTransform
 */
export function isPivotTransform(transform: TransformSpec): transform is PivotTransform {
    return transform.type === 'pivot';
}

/**
 * Type guard for LocfTransform
 */
export function isLocfTransform(transform: TransformSpec): transform is LocfTransform {
    return transform.type === 'locf';
}

/**
 * Type guard for JoinTransform
 */
export function isJoinTransform(transform: TransformSpec): transform is JoinTransform {
    return transform.type === 'join';
}

/**
 * Type guard for FilterTransform
 */
export function isFilterTransform(transform: TransformSpec): transform is FilterTransform {
    return transform.type === 'filter';
}

/**
 * Type guard for CoarsenTransform
 */
export function isCoarsenTransform(transform: TransformSpec): transform is CoarsenTransform {
    return transform.type === 'coarsen';
}

/**
 * Type guard for ApplyEnrichmentTransform
 */
export function isApplyEnrichmentTransform(transform: TransformSpec): transform is ApplyEnrichmentTransform {
    return transform.type === 'apply_enrichment';
}

/**
 * Type guard for TimezoneTransform
 */
export function isTimezoneTransform(transform: TransformSpec): transform is TimezoneTransform {
    return transform.type === 'timezone';
}

/**
 * Type guard for WindowTransform
 */
export function isWindowTransform(transform: TransformSpec): transform is WindowTransform {
    return transform.type === 'window';
}

/**
 * Validates a QueryPlan
 * @throws Error if the plan is invalid
 */
export function validateQueryPlan(plan: QueryPlan): void {
    // Validate context
    if (plan.context.period.from >= plan.context.period.until) {
        throw new Error('QueryPlan period "from" must be before "until"');
    }

    // Validate sources
    if (!plan.sources || plan.sources.length === 0) {
        throw new Error('QueryPlan must have at least one source');
    }

    const sourceAliases = new Set<string>();
    for (const source of plan.sources) {
        if (!source.alias) {
            throw new Error('Source must have an alias');
        }
        if (sourceAliases.has(source.alias)) {
            throw new Error(`Duplicate source alias: ${source.alias}`);
        }
        sourceAliases.add(source.alias);
    }

    // Validate transforms reference valid sources
    // Build set of available aliases (sources + transformed aliases)
    const availableAliases = new Set<string>(sourceAliases);

    for (const transform of plan.transforms) {
        if (isPivotTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`Pivot transform references unknown source: ${transform.sourceAlias}`);
            }
            // Add the pivoted alias to available aliases
            availableAliases.add(`${transform.sourceAlias}_pivoted`);
        } else if (isLocfTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`LOCF transform references unknown source: ${transform.sourceAlias}`);
            }
            if (!availableAliases.has(transform.baseTimelineAlias)) {
                throw new Error(`LOCF transform references unknown timeline source: ${transform.baseTimelineAlias}`);
            }
            // Add the LOCF alias to available aliases
            availableAliases.add(`${transform.sourceAlias}_locf`);
        } else if (isCoarsenTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`Coarsen transform references unknown source: ${transform.sourceAlias}`);
            }
            // Add the coarsened alias (either custom 'as' or default '_coarsened')
            const coarsenedAlias = transform.as || `${transform.sourceAlias}_coarsened`;
            availableAliases.add(coarsenedAlias);
        } else if (isApplyEnrichmentTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`ApplyEnrichment transform references unknown source: ${transform.sourceAlias}`);
            }
            const lookupAlias = (transform as any).lookupSourceAlias || (transform as any).contextSourceAlias;
            if (!availableAliases.has(lookupAlias)) {
                throw new Error(`ApplyEnrichment transform references unknown lookup source: ${lookupAlias}`);
            }
            // Add the enriched alias (either custom 'as' or default '_enriched')
            const enrichedAlias = transform.as || `${transform.sourceAlias}_enriched`;
            availableAliases.add(enrichedAlias);
        } else if (isTimezoneTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`Timezone transform references unknown source: ${transform.sourceAlias}`);
            }
            // Add the timezone-converted alias (either custom 'as' or default '_tz')
            const tzAlias = transform.as || `${transform.sourceAlias}_tz`;
            availableAliases.add(tzAlias);
        } else if (isWindowTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`Window transform references unknown source: ${transform.sourceAlias}`);
            }
            // Add the window alias (either custom 'as' or default '_windowed')
            const windowAlias = transform.as || `${transform.sourceAlias}_windowed`;
            availableAliases.add(windowAlias);
        } else if (isJoinTransform(transform)) {
            if (!availableAliases.has(transform.leftAlias)) {
                throw new Error(`Join transform references unknown left source: ${transform.leftAlias}`);
            }
            if (!availableAliases.has(transform.rightAlias)) {
                throw new Error(`Join transform references unknown right source: ${transform.rightAlias}`);
            }
        } else if (isFilterTransform(transform)) {
            if (!availableAliases.has(transform.sourceAlias)) {
                throw new Error(`Filter transform references unknown source: ${transform.sourceAlias}`);
            }
        }
    }

    // Validate output columns
    if (!plan.output.columns || plan.output.columns.length === 0) {
        throw new Error('QueryPlan output must have at least one column');
    }
}
