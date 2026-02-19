/**
 * Timezone SQL Generator
 *
 * Generates SQL for timezone conversion transformations.
 * Converts UTC timestamps to local time (e.g., "America/Sao_Paulo").
 *
 * Example:
 *   Input (UTC):       timestamp               | device_id
 *                      2024-01-01 13:00:00 UTC | 101
 *   Output (BRT):      timestamp               | device_id
 *                      2024-01-01 10:00:00     | 101  (UTC-3)
 *
 * DuckDB Timezone Conversion:
 *   - AT TIME ZONE 'timezone' converts to the specified timezone
 *   - timezone(tz, timestamp) is an alternative syntax
 */

import { TimezoneTransform } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

/**
 * Validate timezone transform
 *
 * @param transform - Timezone transform specification
 * @throws Error if transform is invalid
 */
export function validateTimezoneTransform(transform: TimezoneTransform): void {
    if (!transform.timestampColumns || transform.timestampColumns.length === 0) {
        throw new Error('Timezone transform must specify at least one timestamp column');
    }
    if (!transform.timezone || transform.timezone.trim() === '') {
        throw new Error('Timezone transform must specify a timezone');
    }
    // Validate timezone format (basic check for IANA timezone database format)
    if (!/^[A-Za-z_]+\/[A-Za-z_]+$/.test(transform.timezone)) {
        throw new Error(
            `Invalid timezone format: ${transform.timezone}. Expected format: "Region/City" (e.g., "America/Sao_Paulo")`
        );
    }
}

/**
 * Generate raw SQL SELECT statement for a timezone transform (without CTE wrapper)
 *
 * @param transform - Timezone transform specification
 * @param sourceTable - Source table or CTE name
 * @param selectColumns - Optional columns to select (if not provided, uses *)
 * @returns Raw SELECT statement
 */
export function generateTimezoneRawSQL(
    transform: TimezoneTransform,
    sourceTable: string,
    selectColumns?: string[]
): string {
    validateTimezoneTransform(transform);

    const quotedSourceTable = quoteIdentifier(sourceTable);

    // Build timezone conversion for each timestamp column
    const conversions = transform.timestampColumns.map((col) => {
        const quotedCol = quoteIdentifier(col);
        return `(${quotedCol} AT TIME ZONE '${transform.timezone}') AS ${quotedCol}`;
    });

    // Handle column selection
    let selectClause: string;
    if (selectColumns && selectColumns.length > 0) {
        // Filter out timestamp columns from selectColumns to avoid duplication
        const nonTimestampColumns = selectColumns
            .filter((col) => !transform.timestampColumns.includes(col))
            .map(quoteIdentifier);
        const allColumns = [...nonTimestampColumns, ...conversions];
        selectClause = allColumns.join(',\n    ');
    } else {
        selectClause = `*,\n    ${conversions.join(',\n    ')}`;
    }

    return `
SELECT
    ${selectClause}
FROM ${quotedSourceTable}
    `.trim();
}

/**
 * Generate SQL for a timezone transform (wrapped in CTE)
 *
 * @param transform - Timezone transform specification
 * @param sourceTable - Source table or CTE name
 * @returns CTE SQL for the timezone-converted result
 */
export function generateTimezoneSQL(transform: TimezoneTransform, sourceTable: string): string {
    const cteName = transform.as || `${transform.sourceAlias}_tz`;
    const rawSQL = generateTimezoneRawSQL(transform, sourceTable);
    return buildCTE(cteName, rawSQL);
}

/**
 * Generate SQL with explicit column selection (wrapped in CTE)
 *
 * @param transform - Timezone transform specification
 * @param sourceTable - Source table or CTE name
 * @param selectColumns - Columns to select (non-timestamp columns)
 * @returns CTE SQL for the timezone-converted result
 */
export function generateTimezoneSQLWithColumns(
    transform: TimezoneTransform,
    sourceTable: string,
    selectColumns: string[]
): string {
    const cteName = transform.as || `${transform.sourceAlias}_tz`;
    const rawSQL = generateTimezoneRawSQL(transform, sourceTable, selectColumns);
    return buildCTE(cteName, rawSQL);
}

/**
 * Get the CTE name for a timezone transform
 *
 * @param transform - Timezone transform specification
 * @returns CTE name
 */
export function getTimezoneCTEName(transform: TimezoneTransform): string {
    return transform.as || `${transform.sourceAlias}_tz`;
}

/**
 * Helper function to generate a single timezone conversion expression
 *
 * @param columnName - Column to convert
 * @param timezone - Target timezone (e.g., 'America/Sao_Paulo')
 * @returns SQL expression for timezone conversion
 */
export function generateTimezoneExpression(columnName: string, timezone: string): string {
    const quotedCol = quoteIdentifier(columnName);
    return `(${quotedCol} AT TIME ZONE '${timezone}')`;
}
