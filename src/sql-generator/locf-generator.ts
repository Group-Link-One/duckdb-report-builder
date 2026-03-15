/**
 * LOCF (Last Observation Carried Forward) SQL Generator
 *
 * Generates SQL for LOCF transformations using window functions.
 * LOCF fills gaps in sparse data by carrying forward the last observed value.
 *
 * Example:
 *   Input:  timestamp | device_id | serial_number
 *           T1        | 100       | 2949494
 *           T2        | 100       | NULL
 *           T3        | 100       | NULL
 *
 *   Output: timestamp | device_id | serial_number
 *           T1        | 100       | 2949494
 *           T2        | 100       | 2949494 (carried forward)
 *           T3        | 100       | 2949494 (carried forward)
 */

import { LocfTransform } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

/**
 * Generate raw SQL SELECT statement for an LOCF transform (without CTE wrapper)
 *
 * @param transform - LOCF transform specification
 * @param sourceTable - Source table or CTE name
 * @param baseTimelineTable - Base timeline table for gap filling
 * @returns Raw SELECT statement
 */
export function generateLocfRawSQL(transform: LocfTransform, sourceTable: string, baseTimelineTable: string): string {
    // Build LOCF expressions for each column using LAST_VALUE window function
    const locfExpressions = transform.columns.map((col) => {
        const quotedCol = quoteIdentifier(col);

        if (transform.maxLookbackSeconds !== null) {
            return `
    COALESCE(
        source.${quotedCol},
        (
            SELECT ${quotedCol}
            FROM ${quoteIdentifier(sourceTable)} prev
            WHERE ${transform.joinKeys.map((key) => `prev.${quoteIdentifier(key)} = base.${quoteIdentifier(key)}`).join(' AND ')}
                AND prev.timestamp <= base.timestamp
                AND prev.timestamp >= base.timestamp - INTERVAL '${transform.maxLookbackSeconds} seconds'
                AND prev.${quotedCol} IS NOT NULL
            ORDER BY prev.timestamp DESC
            LIMIT 1
        )
    ) AS ${quotedCol}`.trim();
        } else {
            return `
    COALESCE(
        source.${quotedCol},
        (
            SELECT ${quotedCol}
            FROM ${quoteIdentifier(sourceTable)} prev
            WHERE ${transform.joinKeys.map((key) => `prev.${quoteIdentifier(key)} = base.${quoteIdentifier(key)}`).join(' AND ')}
                AND prev.timestamp <= base.timestamp
                AND prev.${quotedCol} IS NOT NULL
            ORDER BY prev.timestamp DESC
            LIMIT 1
        )
    ) AS ${quotedCol}`.trim();
        }
    });

    // Build join conditions
    const joinConditions = transform.joinKeys
        .map((key) => `base.${quoteIdentifier(key)} = source.${quoteIdentifier(key)}`)
        .join(' AND ');

    // Select all columns from base timeline plus LOCF columns
    const baseColumns = transform.joinKeys.map((key) => `base.${quoteIdentifier(key)}`).join(',\n    ');

    return `
SELECT
    base.timestamp,
    ${baseColumns},
    ${locfExpressions.join(',\n    ')}
FROM ${quoteIdentifier(baseTimelineTable)} base
LEFT JOIN ${quoteIdentifier(sourceTable)} source
    ON ${joinConditions}
    AND base.timestamp = source.timestamp
    `.trim();
}

/**
 * Generate SQL for an LOCF transform (wrapped in CTE)
 *
 * @param transform - LOCF transform specification
 * @param sourceTable - Source table or CTE name
 * @param baseTimelineTable - Base timeline table for gap filling
 * @returns CTE SQL for the LOCF result
 */
export function generateLocfSQL(transform: LocfTransform, sourceTable: string, baseTimelineTable: string): string {
    const cteName = `${transform.sourceAlias}_locf`;
    const rawSQL = generateLocfRawSQL(transform, sourceTable, baseTimelineTable);
    return buildCTE(cteName, rawSQL);
}

export function getLocfCTEName(transform: LocfTransform): string {
    return `${transform.sourceAlias}_locf`;
}
