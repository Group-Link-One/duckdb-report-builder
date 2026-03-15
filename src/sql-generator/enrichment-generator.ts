/**
 * Enrichment SQL Generator
 *
 * Generates SQL for applying enrichment formulas using a lookup table.
 * Joins source data with a lookup/context table and applies enrichment formulas.
 *
 * Example:
 *   Input (source):    entity_id | channel | daily_raw | latest_raw
 *                      101       | 1       | 1000      | 50000
 *   Lookup table:      entity_id | channel | multiplier | raw_offset | result_offset
 *                      101       | 1       | 1.2        | 100        | 0
 *   Output (enriched): entity_id | channel | adjusted_consumption | adjusted_absolute
 *                      101       | 1       | 1200 (1000 * 1.2)    | 59880 ((50000-100)*1.2+0)
 */

import { ApplyEnrichmentTransform } from '../query-plan/query-plan';
import { buildCTE, quoteIdentifier } from './cte-builder';

/**
 * Validate apply enrichment transform
 *
 * @param transform - ApplyEnrichment transform specification
 * @throws Error if transform is invalid
 */
export function validateApplyEnrichmentTransform(transform: ApplyEnrichmentTransform): void {
    if (!transform.lookupSourceAlias) {
        throw new Error('ApplyEnrichment transform must specify lookupSourceAlias');
    }
    if (transform.joinOn.length === 0) {
        throw new Error('ApplyEnrichment transform must specify at least one join key');
    }
    if (!transform.formulas || Object.keys(transform.formulas).length === 0) {
        throw new Error('ApplyEnrichment transform must specify at least one formula');
    }
    for (const [columnName, enrichment] of Object.entries(transform.formulas)) {
        const formula = enrichment.formula;
        if (!formula || formula.trim() === '') {
            throw new Error(`Enrichment formula for column "${columnName}" must have a non-empty formula`);
        }
    }
}

/**
 * Generate raw SQL SELECT statement for an apply enrichment transform (without CTE wrapper)
 *
 * @param transform - ApplyEnrichment transform specification
 * @param sourceTable - Source table or CTE name (raw data)
 * @param lookupTable - Lookup table name (context/enrichment data)
 * @param selectColumns - Optional columns to select from source (if not provided, uses r.*)
 * @returns Raw SELECT statement
 */
export function generateApplyEnrichmentRawSQL(
    transform: ApplyEnrichmentTransform,
    sourceTable: string,
    lookupTable: string,
    selectColumns?: string[]
): string {
    validateApplyEnrichmentTransform(transform);

    const quotedSourceTable = quoteIdentifier(sourceTable);
    const quotedLookupTable = quoteIdentifier(lookupTable);

    // Build JOIN ON conditions
    const joinConditions = transform.joinOn.map((key) => {
        const quotedKey = quoteIdentifier(key);
        return `r.${quotedKey} = c.${quotedKey}`;
    });
    const joinClause = joinConditions.join(' AND ');

    const enrichmentColumns = Object.entries(transform.formulas).map(([columnName, enrichment]) => {
        const quotedColumn = quoteIdentifier(columnName);
        const formula = enrichment.formula;
        return `${formula} AS ${quotedColumn}`;
    });

    // Build SELECT clause
    const selectClause =
        selectColumns && selectColumns.length > 0
            ? selectColumns.map((col) => `r.${quoteIdentifier(col)}`).join(',\n    ')
            : 'r.*';

    return `
SELECT
    ${selectClause},
    ${enrichmentColumns.join(',\n    ')}
FROM ${quotedSourceTable} AS r
LEFT JOIN ${quotedLookupTable} AS c
    ON ${joinClause}
    `.trim();
}

/**
 * Generate SQL for an apply enrichment transform (wrapped in CTE)
 *
 * @param transform - ApplyEnrichment transform specification
 * @param sourceTable - Source table or CTE name (raw data)
 * @param lookupTable - Lookup table name (enrichment data)
 * @returns CTE SQL for the enriched result
 */
export function generateApplyEnrichmentSQL(
    transform: ApplyEnrichmentTransform,
    sourceTable: string,
    lookupTable: string
): string {
    const cteName = transform.as || `${transform.sourceAlias}_enriched`;
    const rawSQL = generateApplyEnrichmentRawSQL(transform, sourceTable, lookupTable);
    return buildCTE(cteName, rawSQL);
}

export function getApplyEnrichmentCTEName(transform: ApplyEnrichmentTransform): string {
    return transform.as || `${transform.sourceAlias}_enriched`;
}
