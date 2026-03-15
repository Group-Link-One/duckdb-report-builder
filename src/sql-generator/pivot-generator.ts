/**
 * Pivot SQL Generator
 *
 * Generates SQL for pivot transformations using MAX(CASE WHEN...) pattern.
 *
 * Example:
 *   Input (unpivoted): serie_id | value
 *                      2        | 666
 *                      3        | 0
 *   Output (pivoted):  energy_in | energy_out
 *                      666       | 0
 */

import { PivotTransform } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

/**
 * Generate raw SQL SELECT statement for a pivot transform (without CTE wrapper)
 *
 * @param transform - Pivot transform specification
 * @param sourceTable - Source table or CTE name
 * @returns Raw SELECT statement
 */
export function generatePivotRawSQL(transform: PivotTransform, sourceTable: string): string {
    // groupBy is required for pivot transforms
    if (!transform.groupBy || transform.groupBy.length === 0) {
        throw new Error(
            `Pivot transform on '${transform.sourceAlias}' must specify groupBy columns. ` +
                `Example: groupBy: ['entity_id', 'timestamp'] or groupBy: ['id', 'date']`
        );
    }
    const groupByColumns = transform.groupBy;

    // Build CASE WHEN expressions for each pivot column
    const pivotCases = transform.columns.map((col) => {
        const pivotValue = typeof col.pivotValue === 'string' ? `'${col.pivotValue}'` : col.pivotValue;

        return `MAX(CASE WHEN ${quoteIdentifier(transform.pivotColumn)} = ${pivotValue} THEN ${quoteIdentifier(transform.valueColumn)} END) AS ${quoteIdentifier(col.outputAlias)}`;
    });

    const groupByClause = groupByColumns.map(quoteIdentifier).join(', ');
    const selectGroupBy = groupByColumns.map((col) => quoteIdentifier(col)).join(', ');

    return `
SELECT
    ${selectGroupBy},
    ${pivotCases.join(',\n    ')}
FROM ${quoteIdentifier(sourceTable)}
GROUP BY ${groupByClause}
    `.trim();
}

/**
 * Generate SQL for a pivot transform (wrapped in CTE)
 *
 * @param transform - Pivot transform specification
 * @param sourceTable - Source table or CTE name
 * @returns CTE SQL for the pivoted result
 */
export function generatePivotSQL(transform: PivotTransform, sourceTable: string): string {
    const cteName = transform.as || `${transform.sourceAlias}_pivoted`;
    const rawSQL = generatePivotRawSQL(transform, sourceTable);
    return buildCTE(cteName, rawSQL);
}

/**
 * Infer group-by columns from source schema
 *
 * This function determines which columns should be in the GROUP BY clause
 * by excluding the pivot column and value column.
 *
 * @param sourceColumns - All columns in the source table
 * @param pivotColumn - Column being pivoted on
 * @param valueColumn - Column containing values
 * @returns Array of column names to group by
 */
export function inferGroupByColumns(sourceColumns: string[], pivotColumn: string, valueColumn: string): string[] {
    return sourceColumns.filter((col) => col !== pivotColumn && col !== valueColumn);
}

export function getPivotCTEName(transform: PivotTransform): string {
    return transform.as || `${transform.sourceAlias}_pivoted`;
}
