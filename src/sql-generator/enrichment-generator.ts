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
    const lookupAlias = (transform as any).lookupSourceAlias || (transform as any).contextSourceAlias;
    if (!lookupAlias) {
        throw new Error('ApplyEnrichment transform must specify lookupSourceAlias');
    }
    if (transform.joinOn.length === 0) {
        throw new Error('ApplyEnrichment transform must specify at least one join key');
    }
    const formulas = (transform as any).formulas || (transform as any).adjustments;
    if (!formulas || Object.keys(formulas).length === 0) {
        throw new Error('ApplyEnrichment transform must specify at least one formula');
    }
    for (const [columnName, enrichment] of Object.entries(formulas)) {
        const formula = (enrichment as any).formula;
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

    // Build enrichment columns (support both formulas and adjustments for backward compat)
    const formulas = (transform as any).formulas || (transform as any).adjustments;
    const enrichmentColumns = Object.entries(formulas).map(([columnName, enrichment]) => {
        const quotedColumn = quoteIdentifier(columnName);
        const formula = (enrichment as any).formula;
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

/**
 * Generate SQL with explicit column selection (wrapped in CTE)
 *
 * @param transform - ApplyEnrichment transform specification
 * @param sourceTable - Source table or CTE name
 * @param lookupTable - Lookup table name
 * @param selectColumns - Columns to select from source
 * @returns CTE SQL for the enriched result
 */
export function generateApplyEnrichmentSQLWithColumns(
    transform: ApplyEnrichmentTransform,
    sourceTable: string,
    lookupTable: string,
    selectColumns: string[]
): string {
    const cteName = transform.as || `${transform.sourceAlias}_enriched`;
    const rawSQL = generateApplyEnrichmentRawSQL(transform, sourceTable, lookupTable, selectColumns);
    return buildCTE(cteName, rawSQL);
}

/**
 * Get the CTE name for an apply enrichment transform
 *
 * @param transform - ApplyEnrichment transform specification
 * @returns CTE name
 */
export function getApplyEnrichmentCTEName(transform: ApplyEnrichmentTransform): string {
    return transform.as || `${transform.sourceAlias}_enriched`;
}
