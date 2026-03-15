/**
 * Window Function SQL Generator
 *
 * Generates SQL for window function transformations.
 * Supports LAG, LEAD, ROW_NUMBER, RANK, FIRST_VALUE, LAST_VALUE, ARRAY_AGG with QUALIFY.
 *
 * Example:
 *   Input:   device_id | date       | value
 *            101       | 2024-01-01 | 100
 *            101       | 2024-01-02 | 150
 *            101       | 2024-01-03 | 200
 *
 *   LAG:     device_id | date       | value | prev_value
 *            101       | 2024-01-01 | 100   | 0
 *            101       | 2024-01-02 | 150   | 100
 *            101       | 2024-01-03 | 200   | 150
 *
 *   QUALIFY: device_id | date       | value | rn
 *            101       | 2024-01-03 | 200   | 1  (latest row only)
 */

import { WindowFunctionSpec, WindowTransform } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

/**
 * Validate window transform
 *
 * @param transform - Window transform specification
 * @throws Error if transform is invalid
 */
export function validateWindowTransform(transform: WindowTransform): void {
    if (!transform.partitionBy || transform.partitionBy.length === 0) {
        throw new Error('Window transform must specify at least one PARTITION BY column');
    }
    if (!transform.orderBy || transform.orderBy.length === 0) {
        throw new Error('Window transform must specify at least one ORDER BY column');
    }
    if (!transform.windowFunctions || transform.windowFunctions.length === 0) {
        throw new Error('Window transform must specify at least one window function');
    }

    // Validate each window function
    for (const func of transform.windowFunctions) {
        validateWindowFunction(func);
    }
}

/**
 * Validate a single window function specification
 *
 * @param func - Window function specification
 * @throws Error if function is invalid
 */
function validateWindowFunction(func: WindowFunctionSpec): void {
    // ROW_NUMBER, RANK don't need a column
    const needsColumn = !['ROW_NUMBER', 'RANK'].includes(func.function);

    if (needsColumn && !func.column) {
        throw new Error(`Window function ${func.function} requires a column`);
    }

    if (!func.outputAlias || func.outputAlias.trim() === '') {
        throw new Error(`Window function ${func.function} must have an output alias`);
    }
}

/**
 * Generate SQL for a single window function
 *
 * @param func - Window function specification
 * @param partitionBy - PARTITION BY columns
 * @param orderBy - ORDER BY columns
 * @returns SQL window function expression
 */
function generateWindowFunction(
    func: WindowFunctionSpec,
    partitionBy: string[],
    orderBy: Array<{ column: string; direction: 'ASC' | 'DESC' }>
): string {
    const quotedOutputAlias = quoteIdentifier(func.outputAlias);

    // Build PARTITION BY clause
    const partitionClause = partitionBy.map(quoteIdentifier).join(', ');

    // Build ORDER BY clause - use function-specific orderBy if provided, else use transform orderBy
    const orderByToUse = func.orderBy && func.orderBy.length > 0 ? func.orderBy : orderBy;
    const orderByClause = orderByToUse.map((ob) => `${quoteIdentifier(ob.column)} ${ob.direction}`).join(', ');

    let windowExpr: string;

    switch (func.function) {
        case 'LAG': {
            const column = quoteIdentifier(func.column!);
            const offset = func.offset || 1;
            const defaultValue =
                func.defaultValue !== undefined
                    ? typeof func.defaultValue === 'string'
                        ? `'${func.defaultValue}'`
                        : func.defaultValue
                    : 'NULL';
            windowExpr = `LAG(${column}, ${offset}, ${defaultValue}) OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause})`;
            break;
        }

        case 'LEAD': {
            const column = quoteIdentifier(func.column!);
            const offset = func.offset || 1;
            const defaultValue =
                func.defaultValue !== undefined
                    ? typeof func.defaultValue === 'string'
                        ? `'${func.defaultValue}'`
                        : func.defaultValue
                    : 'NULL';
            windowExpr = `LEAD(${column}, ${offset}, ${defaultValue}) OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause})`;
            break;
        }

        case 'ROW_NUMBER': {
            windowExpr = `ROW_NUMBER() OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause})`;
            break;
        }

        case 'RANK': {
            windowExpr = `RANK() OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause})`;
            break;
        }

        case 'FIRST_VALUE': {
            const column = quoteIdentifier(func.column!);
            windowExpr = `FIRST_VALUE(${column}) OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause})`;
            break;
        }

        case 'LAST_VALUE': {
            const column = quoteIdentifier(func.column!);
            // LAST_VALUE requires ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            windowExpr = `LAST_VALUE(${column}) OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`;
            break;
        }

        case 'ARRAY_AGG': {
            const column = quoteIdentifier(func.column!);
            windowExpr = `ARRAY_AGG(${column}) OVER (PARTITION BY ${partitionClause} ORDER BY ${orderByClause})`;
            break;
        }

        default:
            throw new Error(`Unsupported window function: ${func.function}`);
    }

    return `${windowExpr} AS ${quotedOutputAlias}`;
}

/**
 * Generate raw SQL SELECT statement for a window transform (without CTE wrapper)
 *
 * @param transform - Window transform specification
 * @param sourceTable - Source table or CTE name
 * @returns Raw SELECT statement
 */
export function generateWindowRawSQL(transform: WindowTransform, sourceTable: string): string {
    validateWindowTransform(transform);

    const quotedSourceTable = quoteIdentifier(sourceTable);

    // Generate window function columns
    const windowColumns = transform.windowFunctions.map((func) =>
        generateWindowFunction(func, transform.partitionBy, transform.orderBy)
    );

    // Build the SELECT statement
    let sql = `
SELECT
    *,
    ${windowColumns.join(',\n    ')}
FROM ${quotedSourceTable}
    `.trim();

    // Add QUALIFY clause if specified
    if (transform.qualify) {
        sql += `\nQUALIFY ${transform.qualify}`;
    }

    return sql;
}

/**
 * Generate SQL for a window transform (wrapped in CTE)
 *
 * @param transform - Window transform specification
 * @param sourceTable - Source table or CTE name
 * @returns CTE SQL for the windowed result
 */
export function generateWindowSQL(transform: WindowTransform, sourceTable: string): string {
    const cteName = transform.as || `${transform.sourceAlias}_windowed`;
    const rawSQL = generateWindowRawSQL(transform, sourceTable);
    return buildCTE(cteName, rawSQL);
}

export function getWindowCTEName(transform: WindowTransform): string {
    return transform.as || `${transform.sourceAlias}_windowed`;
}

/**
 * Helper function to create a LAG window function specification
 *
 * @param column - Column to apply LAG to
 * @param outputAlias - Output column name
 * @param offset - Number of rows to look back (default: 1)
 * @param defaultValue - Default value when no previous row (default: 0)
 * @returns WindowFunctionSpec for LAG
 */
export function createLagFunction(
    column: string,
    outputAlias: string,
    offset: number = 1,
    defaultValue: string | number | null = 0
): WindowFunctionSpec {
    return {
        function: 'LAG',
        column,
        offset,
        defaultValue,
        outputAlias,
    };
}

/**
 * Helper function to create a ROW_NUMBER window function specification
 *
 * @param outputAlias - Output column name (default: 'rn')
 * @returns WindowFunctionSpec for ROW_NUMBER
 */
export function createRowNumberFunction(outputAlias: string = 'rn'): WindowFunctionSpec {
    return {
        function: 'ROW_NUMBER',
        outputAlias,
    };
}

/**
 * Helper function to create an ARRAY_AGG window function specification
 *
 * @param column - Column to aggregate
 * @param outputAlias - Output column name
 * @param orderBy - Optional ORDER BY for array elements
 * @returns WindowFunctionSpec for ARRAY_AGG
 */
export function createArrayAggFunction(
    column: string,
    outputAlias: string,
    orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>
): WindowFunctionSpec {
    return {
        function: 'ARRAY_AGG',
        column,
        outputAlias,
        orderBy,
    };
}
