/**
 * SQL Generator
 *
 * Consumes QueryPlan IR and generates DuckDB SQL with CTEs.
 *
 * Strategy:
 * 1. Load sources (done by providers)
 * 2. Generate CTEs for each transform
 * 3. Generate final SELECT with joins
 * 4. Apply filters and ordering
 */

import {
    isApplyEnrichmentTransform, isCoarsenTransform, isFilterTransform, isJoinTransform, isLocfTransform, isPivotTransform, isTimezoneTransform,
    isWindowTransform,
    JoinTransform, QueryPlan,
    TransformSpec
} from '../query-plan/query-plan';
import { generateCoarsenRawSQL, generateCoarsenSQL, getCoarsenCTEName } from './coarsen-generator';
import { quoteIdentifier } from './cte-builder';
import {
    generateApplyEnrichmentRawSQL, generateApplyEnrichmentSQL, getApplyEnrichmentCTEName
} from './enrichment-generator';
import { buildJoinTree, generateFromClauseWithJoins } from './join-generator';
import { generateLocfRawSQL, generateLocfSQL, getLocfCTEName } from './locf-generator';
import { generatePivotRawSQL, generatePivotSQL, getPivotCTEName } from './pivot-generator';
import { generateTimezoneRawSQL, generateTimezoneSQL, getTimezoneCTEName } from './timezone-generator';
import { generateWindowRawSQL, generateWindowSQL, getWindowCTEName } from './window-generator';

/**
 * SQL Generator Options
 */
export interface SQLGeneratorOptions {
    /**
     * If true, generate formatted SQL with indentation
     */
    formatted?: boolean;

    /**
     * If true, include comments in generated SQL
     */
    includeComments?: boolean;
}

/**
 * Generated SQL result
 */
export interface GeneratedSQL {
    /**
     * The complete SQL query string
     */
    sql: string;

    /**
     * Table names created by providers (for cleanup)
     */
    tempTables: string[];

    /**
     * CTE names generated (for debugging)
     */
    cteNames: string[];
}

/**
 * Temp table execution step
 */
export interface TempTableStep {
    /** Step name (e.g., 'source:readings', 'transform:applyEnrichment') */
    name: string;

    /** Temp table name to create */
    tableName: string;

    /** SQL to execute to create this temp table */
    sql: string;

    /** Names of tables this step depends on */
    dependsOn: string[];

    /** Transform index in the plan (if applicable) */
    transformIndex?: number;
}

/**
 * Temp table execution plan
 */
export interface GeneratedTempTablePlan {
    /** Ordered steps to execute */
    steps: TempTableStep[];

    /** Final SELECT statement (uses last temp table) */
    finalSelect: string;

    /** All temp table names (for cleanup) */
    tempTables: string[];
}

/**
 * SQL Generator
 *
 * Generates DuckDB SQL from a QueryPlan IR
 */
export class SQLGenerator {
    private options: Required<SQLGeneratorOptions>;

    constructor(options: SQLGeneratorOptions = {}) {
        this.options = {
            formatted: options.formatted ?? true,
            includeComments: options.includeComments ?? false,
        };
    }

    /**
     * Generate SQL from a QueryPlan
     *
     * @param plan - The query plan to generate SQL for
     * @param sourceTableMap - Map of source alias to table name (provided by providers)
     * @returns Generated SQL result
     */
    generate(plan: QueryPlan, sourceTableMap: Map<string, string>): GeneratedSQL {
        const ctes: string[] = [];
        const cteNames: string[] = [];

        // Generate CTEs for each transform
        for (const transform of plan.transforms) {
            const { cte, cteName } = this.generateTransformCTE(transform, sourceTableMap, cteNames);
            if (cte) {
                ctes.push(cte);
                cteNames.push(cteName);
            }
        }

        // Generate final SELECT
        const finalSelect = this.generateFinalSelect(plan, sourceTableMap, cteNames);

        // Combine everything
        let sql = '';
        if (ctes.length > 0) {
            sql = 'WITH\n' + ctes.join(',\n') + '\n' + finalSelect;
        } else {
            sql = finalSelect;
        }

        return {
            sql,
            tempTables: Array.from(sourceTableMap.values()),
            cteNames,
        };
    }

    /**
     * Generate a CTE for a transform
     */
    private generateTransformCTE(
        transform: TransformSpec,
        sourceTableMap: Map<string, string>,
        existingCTEs: string[]
    ): { cte: string; cteName: string } {
        if (isPivotTransform(transform)) {
            // Get source table (could be from provider or previous CTE)
            const sourceTable = this.resolveTableName(transform.sourceAlias, sourceTableMap, existingCTEs);
            const cte = generatePivotSQL(transform, sourceTable);
            const cteName = getPivotCTEName(transform);
            return { cte, cteName };
        } else if (isLocfTransform(transform)) {
            // Get source and timeline tables
            const sourceTable = this.resolveTableName(transform.sourceAlias, sourceTableMap, existingCTEs);
            const timelineTable = this.resolveTableName(transform.baseTimelineAlias, sourceTableMap, existingCTEs);
            const cte = generateLocfSQL(transform, sourceTable, timelineTable);
            const cteName = getLocfCTEName(transform);
            return { cte, cteName };
        } else if (isCoarsenTransform(transform)) {
            // Get source table
            const sourceTable = this.resolveTableName(transform.sourceAlias, sourceTableMap, existingCTEs);
            const cte = generateCoarsenSQL(transform, sourceTable);
            const cteName = getCoarsenCTEName(transform);
            return { cte, cteName };
        } else if (isApplyEnrichmentTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, sourceTableMap, existingCTEs);
            const lookupAlias = transform.lookupSourceAlias;
            const lookupTable = this.resolveTableName(lookupAlias, sourceTableMap, existingCTEs);
            const cte = generateApplyEnrichmentSQL(transform, sourceTable, lookupTable);
            const cteName = getApplyEnrichmentCTEName(transform);
            return { cte, cteName };
        } else if (isTimezoneTransform(transform)) {
            // Get source table
            const sourceTable = this.resolveTableName(transform.sourceAlias, sourceTableMap, existingCTEs);
            const cte = generateTimezoneSQL(transform, sourceTable);
            const cteName = getTimezoneCTEName(transform);
            return { cte, cteName };
        } else if (isWindowTransform(transform)) {
            // Get source table
            const sourceTable = this.resolveTableName(transform.sourceAlias, sourceTableMap, existingCTEs);
            const cte = generateWindowSQL(transform, sourceTable);
            const cteName = getWindowCTEName(transform);
            return { cte, cteName };
        } else if (isJoinTransform(transform)) {
            // Join transforms are handled in the final SELECT, not as CTEs
            return { cte: '', cteName: '' };
        } else if (isFilterTransform(transform)) {
            // Filter transforms are handled in WHERE clauses
            return { cte: '', cteName: '' };
        }

        return { cte: '', cteName: '' };
    }

    /**
     * Resolve table name from alias
     *
     * Checks if the alias refers to a provider table or a CTE
     * Priority: CTE suffixes > sourceTableMap (for injected tables) > CTE names > sourceTableMap (for sources) > Default
     */
    private resolveTableName(alias: string, sourceTableMap: Map<string, string>, existingCTEs: string[]): string {
        // FIRST: Check if it's a CTE name with standard suffix for the given alias
        // This handles the case where we have transforms on a source (e.g., readings -> readings_pivoted)
        const suffixes = ['_pivoted', '_coarsened', '_enriched', '_locf', '_tz', '_windowed'];
        for (const suffix of suffixes) {
            const cteName = `${alias}${suffix}`;
            if (existingCTEs.includes(cteName)) {
                return cteName;
            }
        }

        // SECOND: Check if the alias itself is already a CTE name (handles custom 'as' names)
        if (existingCTEs.includes(alias)) {
            // If this CTE is also in sourceTableMap with a DIFFERENT value (injected table),
            // use the injected table name (for temp table mode with SQL injection)
            if (sourceTableMap.has(alias)) {
                const mappedValue = sourceTableMap.get(alias)!;
                if (mappedValue !== alias) {
                    return mappedValue;
                }
            }
            return alias;
        }

        // THIRD: Check sourceTableMap for the provider table mapping
        // This is used for source aliases like 'readings' -> 'clickhouse_readings_xxx'
        if (sourceTableMap.has(alias)) {
            return sourceTableMap.get(alias)!;
        }

        // Default to alias itself
        return alias;
    }

    /**
     * Generate the final SELECT statement
     */
    public generateFinalSelect(plan: QueryPlan, sourceTableMap: Map<string, string>, cteNames: string[]): string {
        // Build SELECT columns
        const columns = plan.output.columns
            .map((col) => {
                if (col.isRawExpression) {
                    const alias = quoteIdentifier(col.outputAlias);
                    return `${col.sourceColumn} AS ${alias}`;
                }
                const sourceRef = quoteIdentifier(col.sourceAlias);
                const colRef = quoteIdentifier(col.sourceColumn);
                const alias = quoteIdentifier(col.outputAlias);
                return `${sourceRef}.${colRef} AS ${alias}`;
            })
            .join(',\n    ');

        // Determine base table for FROM clause
        // If there are joins, use the first join's left table as base
        // Otherwise, use the last CTE (if any), or the first source
        let baseTable: string;
        let baseAlias: string;

        const joins = plan.transforms.filter(isJoinTransform) as JoinTransform[];

        if (joins.length > 0) {
            // Use the first join's left table as the base
            baseAlias = joins[0].leftAlias;
        } else if (cteNames.length > 0) {
            // No joins, but we have CTEs - use the last CTE as the base
            baseAlias = cteNames[cteNames.length - 1];
        } else if (plan.sources.length > 0) {
            // No joins, no CTEs - use first source
            baseAlias = plan.sources[0].alias;
        } else {
            throw new Error('QueryPlan must have at least one source');
        }

        // Build FROM clause with joins
        const orderedJoins = buildJoinTree(joins);

        // Create table map for join resolution
        const tableMap = new Map<string, string>();
        for (const source of plan.sources) {
            tableMap.set(source.alias, this.resolveTableName(source.alias, sourceTableMap, cteNames));
        }
        // Add CTE names
        // Process in order so later CTEs for the same base alias take precedence
        for (const cteName of cteNames) {
            // Extract base alias from CTE name (e.g., 'readings_pivoted' -> 'readings')
            const extractedBaseAlias = cteName.replace(/_pivoted$|_locf$|_coarsened$|_enriched$|_tz$|_windowed$/, '');
            // Use resolveTableName to get the actual table (handles SQL injection)
            const actualTable = this.resolveTableName(cteName, sourceTableMap, cteNames);
            tableMap.set(cteName, actualTable);
            // Always map the base alias to the current CTE - later CTEs take precedence
            // This ensures that if we have readings_pivoted then readings_locf,
            // the 'readings' alias points to readings_locf (the final transformed version)
            tableMap.set(extractedBaseAlias, actualTable);
        }

        // Resolve baseAlias to actual table name
        baseTable = tableMap.get(baseAlias) || this.resolveTableName(baseAlias, sourceTableMap, cteNames);

        let fromClause: string;
        if (orderedJoins.length > 0) {
            fromClause = generateFromClauseWithJoins(baseTable, baseAlias, orderedJoins, tableMap);
        } else {
            fromClause = `FROM ${quoteIdentifier(baseTable)} AS ${quoteIdentifier(baseAlias)}`;
        }

        // Build WHERE clause from filters
        const filters = plan.output.filters || [];
        const whereConditions = filters.map((f) => f.condition);
        const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

        // Build GROUP BY clause
        const groupBy = plan.output.groupBy || [];
        const groupByClause = groupBy.length > 0 ? `GROUP BY ${groupBy.join(', ')}` : '';

        // Build ORDER BY clause
        const orderBy = plan.output.orderBy || [];
        const orderByClause =
            orderBy.length > 0
                ? `ORDER BY ${orderBy.map((o) => `${quoteIdentifier(o.column)} ${o.direction}`).join(', ')}`
                : '';

        // Combine all parts
        const parts = [`SELECT`, `    ${columns}`, fromClause, whereClause, groupByClause, orderByClause].filter(
            (p) => p.trim() !== ''
        );

        return parts.join('\n');
    }

    /**
     * Generate a temp table execution plan
     *
     * Instead of generating a single CTE query, this generates a plan with
     * individual steps that create temporary tables. This allows callbacks
     * to fire between transforms and SQL to be injected.
     *
     * @param plan - The query plan to generate SQL for
     * @param sourceTableMap - Map of source alias to table name (provided by providers)
     * @returns Temp table execution plan
     */
    generateTempTablePlan(plan: QueryPlan, sourceTableMap: Map<string, string>): GeneratedTempTablePlan {
        const steps: TempTableStep[] = [];
        const tempTableNames: string[] = [];
        let currentTableMap = new Map<string, string>(sourceTableMap);

        // Generate a step for each transform
        for (let i = 0; i < plan.transforms.length; i++) {
            const transform = plan.transforms[i];
            const step = this.generateTransformStep(transform, i, currentTableMap, tempTableNames);

            if (step) {
                steps.push(step);
                tempTableNames.push(step.tableName);

                // Update table map so next transform can reference this one
                // Map both the step name and any relevant aliases
                currentTableMap.set(step.tableName, step.tableName);

                // For transforms, also map the source alias to this new table
                if (isPivotTransform(transform)) {
                    currentTableMap.set(transform.sourceAlias, step.tableName);
                } else if (isCoarsenTransform(transform)) {
                    currentTableMap.set(transform.sourceAlias, step.tableName);
                } else if (isApplyEnrichmentTransform(transform)) {
                    currentTableMap.set(transform.sourceAlias, step.tableName);
                } else if (isTimezoneTransform(transform)) {
                    currentTableMap.set(transform.sourceAlias, step.tableName);
                } else if (isWindowTransform(transform)) {
                    currentTableMap.set(transform.sourceAlias, step.tableName);
                } else if (isLocfTransform(transform)) {
                    currentTableMap.set(transform.sourceAlias, step.tableName);
                }
            }
        }

        // Generate final SELECT
        const finalSelect = this.generateFinalSelect(plan, currentTableMap, tempTableNames);

        return {
            steps,
            finalSelect,
            tempTables: tempTableNames,
        };
    }

    /**
     * Generate a temp table step for a transform
     */
    private generateTransformStep(
        transform: TransformSpec,
        index: number,
        tableMap: Map<string, string>,
        existingTables: string[]
    ): TempTableStep | null {
        if (isPivotTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, tableMap, existingTables);
            const tableName = getPivotCTEName(transform);
            const rawSQL = generatePivotRawSQL(transform, sourceTable);

            return {
                name: `transform:pivot`,
                tableName,
                sql: `CREATE TEMP TABLE ${tableName} AS ${rawSQL}`,
                dependsOn: [sourceTable],
                transformIndex: index,
            };
        } else if (isLocfTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, tableMap, existingTables);
            const timelineTable = this.resolveTableName(transform.baseTimelineAlias, tableMap, existingTables);
            const tableName = getLocfCTEName(transform);
            const rawSQL = generateLocfRawSQL(transform, sourceTable, timelineTable);

            return {
                name: `transform:locf`,
                tableName,
                sql: `CREATE TEMP TABLE ${tableName} AS ${rawSQL}`,
                dependsOn: [sourceTable, timelineTable],
                transformIndex: index,
            };
        } else if (isCoarsenTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, tableMap, existingTables);
            const tableName = getCoarsenCTEName(transform);
            const rawSQL = generateCoarsenRawSQL(transform, sourceTable);

            return {
                name: `transform:coarsen`,
                tableName,
                sql: `CREATE TEMP TABLE ${tableName} AS ${rawSQL}`,
                dependsOn: [sourceTable],
                transformIndex: index,
            };
        } else if (isApplyEnrichmentTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, tableMap, existingTables);
            const lookupAlias = transform.lookupSourceAlias;
            const lookupTable = this.resolveTableName(lookupAlias, tableMap, existingTables);
            const tableName = getApplyEnrichmentCTEName(transform);
            const rawSQL = generateApplyEnrichmentRawSQL(transform, sourceTable, lookupTable);

            return {
                name: `transform:applyEnrichment`,
                tableName,
                sql: `CREATE TEMP TABLE ${tableName} AS ${rawSQL}`,
                dependsOn: [sourceTable, lookupTable],
                transformIndex: index,
            };
        } else if (isTimezoneTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, tableMap, existingTables);
            const tableName = getTimezoneCTEName(transform);
            const rawSQL = generateTimezoneRawSQL(transform, sourceTable);

            return {
                name: `transform:timezone`,
                tableName,
                sql: `CREATE TEMP TABLE ${tableName} AS ${rawSQL}`,
                dependsOn: [sourceTable],
                transformIndex: index,
            };
        } else if (isWindowTransform(transform)) {
            const sourceTable = this.resolveTableName(transform.sourceAlias, tableMap, existingTables);
            const tableName = getWindowCTEName(transform);
            const rawSQL = generateWindowRawSQL(transform, sourceTable);

            return {
                name: `transform:window`,
                tableName,
                sql: `CREATE TEMP TABLE ${tableName} AS ${rawSQL}`,
                dependsOn: [sourceTable],
                transformIndex: index,
            };
        } else if (isJoinTransform(transform) || isFilterTransform(transform)) {
            // Joins and filters are handled in final SELECT, not as separate steps
            return null;
        }

        return null;
    }

}
