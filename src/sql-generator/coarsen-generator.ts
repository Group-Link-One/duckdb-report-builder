/**
 * Coarsen SQL Generator
 *
 * Generates SQL for time granularity coarsening transformations.
 * Aggregates data from fine to coarse time granularity (e.g., minute -> hour, hour -> day).
 *
 * Example:
 *   Input (minute):   timestamp           | voltage
 *                     2024-01-01 10:01:00 | 12.5
 *                     2024-01-01 10:15:00 | 12.7
 *                     2024-01-01 10:45:00 | 12.3
 *   Output (hour):    timestamp           | voltage
 *                     2024-01-01 10:00:00 | 12.5  (AVG)
 */

import { AggregationStrategy, CoarsenTransform, TimeGranularity } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

/**
 * Granularity hierarchy (for validation)
 */
const GRANULARITY_HIERARCHY: TimeGranularity[] = ['second', 'minute', 'hour', 'day', 'week', 'month'];

/**
 * Map time granularity to DuckDB DATE_TRUNC format
 */
function mapGranularityToDateTrunc(granularity: TimeGranularity): string {
    const mapping: Record<TimeGranularity, string> = {
        second: 'second',
        minute: 'minute',
        hour: 'hour',
        day: 'day',
        week: 'week',
        month: 'month',
    };
    return mapping[granularity];
}

/**
 * Generate SQL aggregation expression based on strategy
 *
 * @param strategy - Aggregation strategy
 * @param column - Column name
 * @param timestampColumn - Timestamp column for ordering (used in FIRST/LAST)
 * @returns SQL aggregation expression
 */
function generateAggregationExpression(strategy: AggregationStrategy, column: string, timestampColumn: string): string {
    const quotedColumn = quoteIdentifier(column);
    const quotedTimestamp = quoteIdentifier(timestampColumn);

    switch (strategy) {
        case 'sum':
            return `SUM(${quotedColumn})`;
        case 'avg':
            return `AVG(${quotedColumn})`;
        case 'min':
            return `MIN(${quotedColumn})`;
        case 'max':
            return `MAX(${quotedColumn})`;
        case 'first':
            return `FIRST(${quotedColumn} ORDER BY ${quotedTimestamp})`;
        case 'last':
            return `LAST(${quotedColumn} ORDER BY ${quotedTimestamp})`;
        case 'count':
            return `COUNT(${quotedColumn})`;
        default:
            throw new Error(`Unknown aggregation strategy: ${strategy}`);
    }
}

/**
 * Validate coarsen transform
 *
 * @param transform - Coarsen transform specification
 * @throws Error if transform is invalid
 */
export function validateCoarsenTransform(transform: CoarsenTransform): void {
    const fromIndex = GRANULARITY_HIERARCHY.indexOf(transform.from);
    const toIndex = GRANULARITY_HIERARCHY.indexOf(transform.to);

    if (fromIndex === -1) {
        throw new Error(`Invalid "from" granularity: ${transform.from}`);
    }
    if (toIndex === -1) {
        throw new Error(`Invalid "to" granularity: ${transform.to}`);
    }
    if (fromIndex >= toIndex) {
        throw new Error(
            `Cannot coarsen from "${transform.from}" to "${transform.to}". ` +
                `Target granularity must be coarser than source granularity.`
        );
    }
    if (Object.keys(transform.strategy).length === 0) {
        throw new Error('Coarsen transform must specify at least one column strategy');
    }
}

/**
 * Generate raw SQL SELECT statement for a coarsen transform (without CTE wrapper)
 *
 * @param transform - Coarsen transform specification
 * @param sourceTable - Source table or CTE name
 * @returns Raw SELECT statement
 */
export function generateCoarsenRawSQL(transform: CoarsenTransform, sourceTable: string): string {
    validateCoarsenTransform(transform);

    const timestampColumn = transform.timestampColumn || 'timestamp';
    const quotedTimestamp = quoteIdentifier(timestampColumn);
    const quotedSourceTable = quoteIdentifier(sourceTable);
    const dateTruncFormat = mapGranularityToDateTrunc(transform.to);

    // Build aggregation columns
    const aggregationColumns = Object.entries(transform.strategy).map(([column, strategy]) => {
        const aggExpr = generateAggregationExpression(strategy, column, timestampColumn);
        return `${aggExpr} AS ${quoteIdentifier(column)}`;
    });

    // Handle group-by columns if specified
    const groupByColumns = transform.groupBy && transform.groupBy.length > 0 ? transform.groupBy : [];
    const groupByExprs = [
        `DATE_TRUNC('${dateTruncFormat}', ${quotedTimestamp})`,
        ...groupByColumns.map(quoteIdentifier),
    ];
    const groupByClause = groupByExprs.join(', ');
    const selectGroupBy =
        groupByColumns.length > 0 ? groupByColumns.map((col) => quoteIdentifier(col)).join(',\n    ') + ',\n    ' : '';
    const orderByGroupBy = groupByColumns.length > 0 ? ', ' + groupByColumns.map(quoteIdentifier).join(', ') : '';

    return `
SELECT
    DATE_TRUNC('${dateTruncFormat}', ${quotedTimestamp}) AS ${quotedTimestamp},
    ${selectGroupBy}${aggregationColumns.join(',\n    ')}
FROM ${quotedSourceTable}
GROUP BY ${groupByClause}
ORDER BY ${quotedTimestamp}${orderByGroupBy}
    `.trim();
}

/**
 * Generate SQL for a coarsen transform (wrapped in CTE)
 *
 * @param transform - Coarsen transform specification
 * @param sourceTable - Source table or CTE name
 * @returns CTE SQL for the coarsened result
 */
export function generateCoarsenSQL(transform: CoarsenTransform, sourceTable: string): string {
    const cteName = transform.as || `${transform.sourceAlias}_coarsened`;
    const rawSQL = generateCoarsenRawSQL(transform, sourceTable);
    return buildCTE(cteName, rawSQL);
}

/**
 * Get the CTE name for a coarsen transform
 *
 * @param transform - Coarsen transform specification
 * @returns CTE name
 */
export function getCoarsenCTEName(transform: CoarsenTransform): string {
    return transform.as || `${transform.sourceAlias}_coarsened`;
}
