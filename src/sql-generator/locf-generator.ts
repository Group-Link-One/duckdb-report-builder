/**
 * LOCF (Last Observation Carried Forward) SQL Generator
 *
 * Generates SQL for LOCF transformations using correlated subqueries.
 * LOCF fills gaps in sparse data by carrying forward the last observed value.
 *
 * Two modes:
 *   Timeline-join: LEFT JOIN source onto a complete timeline, fill gaps via subquery.
 *     Guarantees one output row per timeline slot. Requires a TimelineProvider.
 *   In-place: Fill NULL columns within existing rows via subquery against the same table.
 *     Faster — no timeline overhead. Use when rows exist but columns are sparse.
 */

import { LocfTransform } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

function buildLocfExpression(
    col: string,
    sourceTable: string,
    joinKeys: string[],
    rowAlias: string,
    maxLookbackSeconds: number | null
): string {
    const quotedCol = quoteIdentifier(col);
    const keyPredicates = joinKeys
        .map((key) => `prev.${quoteIdentifier(key)} = ${rowAlias}.${quoteIdentifier(key)}`)
        .join(' AND ');

    const lookbackClause = maxLookbackSeconds !== null
        ? `\n                AND prev.timestamp >= ${rowAlias}.timestamp - INTERVAL '${maxLookbackSeconds} seconds'`
        : '';

    return `COALESCE(
        ${rowAlias === 'source' ? `source.${quotedCol}` : quotedCol},
        (
            SELECT ${quotedCol}
            FROM ${quoteIdentifier(sourceTable)} prev
            WHERE ${keyPredicates}
                AND prev.timestamp <= ${rowAlias}.timestamp${lookbackClause}
                AND prev.${quotedCol} IS NOT NULL
            ORDER BY prev.timestamp DESC
            LIMIT 1
        )
    ) AS ${quotedCol}`;
}

/**
 * Generate in-place LOCF SQL (no timeline join).
 * Fills NULL columns within existing rows using correlated subqueries.
 */
function generateInPlaceLocfRawSQL(transform: LocfTransform, sourceTable: string): string {
    const locfExpressions = transform.columns.map((col) =>
        buildLocfExpression(col, sourceTable, transform.joinKeys, 'curr', transform.maxLookbackSeconds)
    );

    const excludeClause = transform.columns.map(quoteIdentifier).join(', ');

    return `
SELECT
    * EXCLUDE (${excludeClause}),
    ${locfExpressions.join(',\n    ')}
FROM ${quoteIdentifier(sourceTable)} curr
    `.trim();
}

/**
 * Generate timeline-join LOCF SQL.
 * LEFT JOINs source onto a complete timeline, filling gaps via correlated subqueries.
 */
function generateTimelineJoinLocfRawSQL(
    transform: LocfTransform,
    sourceTable: string,
    baseTimelineTable: string
): string {
    const locfExpressions = transform.columns.map((col) =>
        buildLocfExpression(col, sourceTable, transform.joinKeys, 'base', transform.maxLookbackSeconds)
    );

    const joinConditions = transform.joinKeys
        .map((key) => `base.${quoteIdentifier(key)} = source.${quoteIdentifier(key)}`)
        .join(' AND ');

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
 * Generate raw LOCF SQL. Dispatches to in-place or timeline-join based on
 * whether baseTimelineTable is provided.
 */
export function generateLocfRawSQL(
    transform: LocfTransform,
    sourceTable: string,
    baseTimelineTable?: string
): string {
    if (baseTimelineTable) {
        return generateTimelineJoinLocfRawSQL(transform, sourceTable, baseTimelineTable);
    }
    return generateInPlaceLocfRawSQL(transform, sourceTable);
}

/**
 * Generate LOCF SQL wrapped in a CTE.
 */
export function generateLocfSQL(
    transform: LocfTransform,
    sourceTable: string,
    baseTimelineTable?: string
): string {
    const cteName = getLocfCTEName(transform);
    const rawSQL = generateLocfRawSQL(transform, sourceTable, baseTimelineTable);
    return buildCTE(cteName, rawSQL);
}

export function getLocfCTEName(transform: LocfTransform): string {
    return transform.as || `${transform.sourceAlias}_locf`;
}
