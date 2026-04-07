/**
 * Format Generator
 *
 * Generates SQL for final formatting using DuckDB's format() function.
 * Formatting happens as the FINAL step before output - not during processing.
 */

import { quoteIdentifier } from './cte-builder';

// ─── Types ────────────────────────────────────────────────────────────────────

export type Locale = 'pt-BR' | 'en-US';

export interface ColumnFormatConfig {
    /** Rename column in output. Applied for ALL output formats (CSV, Parquet, JSON). */
    rename?: string;

    /** Number of decimal places. Only applied when output needs text formatting (CSV). */
    decimalPlaces?: number;

    /** Unit suffix (e.g., 'm³', 'kWh'). Appended after formatted number. */
    unit?: string;

    /** Currency prefix (e.g., 'R$', '$'). Prepended before formatted number. */
    currency?: string;

    /** Date/time format string (DuckDB strftime format). Overrides locale defaults. */
    dateFormat?: string;

    /**
     * If true, skip auto-formatting for this column even if its type would
     * normally be formatted. Useful for IDs that happen to be numeric.
     */
    raw?: boolean;
}

/**
 * Configuration for last-mile formatting of query output.
 *
 * Used with `report.format(config)` to apply locale-aware formatting (dates, numbers)
 * and column renames before the final output. Works with `build()` and `buildToFile()`.
 *
 * **How it works:** after the main query executes into a temp table, the system queries
 * `information_schema.columns` to detect each column's DuckDB type, then generates a
 * formatting SELECT using `strftime()` / `format()`. This means formatting requires
 * execution — it is not reflected in `toSQL()`.
 *
 * **Output-format behavior:**
 * - CSV/JSON: full formatting (type casting to text) + renames
 * - Parquet: renames only (native types preserved)
 */
export interface FormatConfig {
    /** Locale for number/date formatting */
    locale: Locale;

    /**
     * Default strftime format for DATE columns.
     * If not set, uses locale default: pt-BR='%d/%m/%Y', en-US='%Y-%m-%d'
     */
    dateFormat?: string;

    /**
     * Default strftime format for TIMESTAMP columns.
     * If not set, uses locale default: pt-BR='%d/%m/%Y %H:%M:%S', en-US='%Y-%m-%d %H:%M:%S'
     */
    dateTimeFormat?: string;

    /**
     * Default strftime format for TIMESTAMP WITH TIME ZONE columns.
     * If not set, uses locale default: pt-BR='%d/%m/%Y %H:%M:%S %z', en-US='%Y-%m-%d %H:%M:%S %z'
     *
     * Use %z for UTC offset (e.g., +03), %Z for timezone abbreviation.
     */
    dateTimeTZFormat?: string;

    /**
     * Default number of decimal places for DOUBLE/FLOAT columns.
     * If not set, no number formatting is applied (raw DuckDB output).
     */
    decimalPlaces?: number;

    /** Column-specific overrides. Keys are column names from the query output. */
    columns?: Record<string, ColumnFormatConfig>;
}

/** Schema entry for format system — uses raw DuckDB type strings (not the ColumnType enum). */
export interface FormatColumnSchema {
    name: string;
    /** Raw DuckDB type: 'DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'DOUBLE', 'DECIMAL(18,6)', 'VARCHAR', etc. */
    type: string;
}

// ─── Format Specs (number formatting) ─────────────────────────────────────────

interface FormatSpecs {
    getNumberFormat(decimalPlaces: number): string;
}

class BrazilianFormatSpecs implements FormatSpecs {
    getNumberFormat(decimalPlaces: number): string {
        return `{:t..${decimalPlaces}f}`;
    }
}

class USFormatSpecs implements FormatSpecs {
    getNumberFormat(decimalPlaces: number): string {
        return `{:,.${decimalPlaces}f}`;
    }
}

function getFormatSpecs(locale: Locale): FormatSpecs {
    return locale === 'pt-BR' ? new BrazilianFormatSpecs() : new USFormatSpecs();
}

// ─── Locale Defaults ──────────────────────────────────────────────────────────

export const LOCALE_DEFAULTS: Record<Locale, {
    dateFormat: string;
    dateTimeFormat: string;
    dateTimeTZFormat: string;
    numberFormat: FormatSpecs;
}> = {
    'pt-BR': {
        dateFormat: '%d/%m/%Y',
        dateTimeFormat: '%d/%m/%Y %H:%M:%S',
        dateTimeTZFormat: '%d/%m/%Y %H:%M:%S %z',
        numberFormat: new BrazilianFormatSpecs(),
    },
    'en-US': {
        dateFormat: '%Y-%m-%d',
        dateTimeFormat: '%Y-%m-%d %H:%M:%S',
        dateTimeTZFormat: '%Y-%m-%d %H:%M:%S %z',
        numberFormat: new USFormatSpecs(),
    },
};

// ─── Type Classification Helpers ──────────────────────────────────────────────

function isDateType(type: string): boolean {
    return type.toUpperCase() === 'DATE';
}

function isTimestampTZType(type: string): boolean {
    const upper = type.toUpperCase();
    return upper === 'TIMESTAMP WITH TIME ZONE' || upper === 'TIMESTAMPTZ';
}

function isTimestampType(type: string): boolean {
    const upper = type.toUpperCase();
    return upper === 'TIMESTAMP' || upper === 'TIMESTAMP WITHOUT TIME ZONE';
}

function isFloatingPointType(type: string): boolean {
    const upper = type.toUpperCase();
    return upper === 'DOUBLE' || upper === 'FLOAT' || upper === 'REAL' || upper.startsWith('DECIMAL');
}

// ─── New API ──────────────────────────────────────────────────────────────────

/**
 * Generate a SELECT statement that formats and renames columns.
 *
 * For each column in the schema:
 * 1. Check if there's an explicit config in FormatConfig.columns[name]
 *    - If config.raw=true → pass-through (but still rename if set)
 *    - If config has dateFormat/decimalPlaces → use those
 *    - If config only has rename → auto-format by type + rename
 * 2. If no explicit config → auto-format based on DuckDB type + locale defaults
 * 3. Apply rename if specified
 *
 * @param sourceTable - Table to SELECT FROM
 * @param schema - Column names and DuckDB types
 * @param config - Format configuration
 * @returns SELECT statement with formatted/renamed columns
 */
export function generateFormatSQL(
    sourceTable: string,
    schema: FormatColumnSchema[],
    config: FormatConfig,
): string {
    if (schema.length === 0) {
        return `SELECT * FROM ${sourceTable}`;
    }

    const defaults = LOCALE_DEFAULTS[config.locale];
    const columns = config.columns ?? {};

    const selectExprs: string[] = schema.map((col) => {
        const colConfig = columns[col.name];
        const sourceRef = quoteIdentifier(col.name);
        const outputName = colConfig?.rename ?? col.name;
        const outputRef = quoteIdentifier(outputName);
        const needsAlias = outputName !== col.name;

        // raw=true → pass-through, but still rename
        if (colConfig?.raw) {
            return needsAlias ? `${sourceRef} AS ${outputRef}` : sourceRef;
        }

        // Build the formatting expression
        const expr = buildColumnExpr(sourceRef, col.type, colConfig, config, defaults);

        // If expression is same as source and no rename → just emit the column
        if (expr === sourceRef && !needsAlias) {
            return sourceRef;
        }

        return `${expr} AS ${outputRef}`;
    });

    return `SELECT\n    ${selectExprs.join(',\n    ')}\nFROM ${sourceTable}`;
}

/**
 * Generate a SELECT statement that only renames columns (no type formatting).
 * Used for Parquet output where native types should be preserved.
 *
 * @param sourceTable - Table to SELECT FROM
 * @param schema - Column names and DuckDB types
 * @param config - Format configuration (only columns.rename is used)
 * @returns SELECT statement with renamed columns
 */
export function generateRenameSQL(
    sourceTable: string,
    schema: FormatColumnSchema[],
    config: FormatConfig,
): string {
    if (schema.length === 0) {
        return `SELECT * FROM ${sourceTable}`;
    }

    const columns = config.columns ?? {};

    const selectExprs: string[] = schema.map((col) => {
        const colConfig = columns[col.name];
        const sourceRef = quoteIdentifier(col.name);

        if (colConfig?.rename) {
            return `${sourceRef} AS ${quoteIdentifier(colConfig.rename)}`;
        }

        return sourceRef;
    });

    return `SELECT\n    ${selectExprs.join(',\n    ')}\nFROM ${sourceTable}`;
}

// ─── Internal: build formatting expression for a single column ────────────────

function buildColumnExpr(
    sourceRef: string,
    type: string,
    colConfig: ColumnFormatConfig | undefined,
    config: FormatConfig,
    defaults: typeof LOCALE_DEFAULTS['pt-BR'],
): string {
    // Explicit dateFormat in column config
    if (colConfig?.dateFormat) {
        return `strftime(${sourceRef}, '${colConfig.dateFormat}')`;
    }

    // Explicit decimalPlaces in column config
    if (colConfig?.decimalPlaces !== undefined) {
        return buildNumberExpr(sourceRef, colConfig.decimalPlaces, colConfig, config.locale);
    }

    // Auto-detect by type
    if (isDateType(type)) {
        const fmt = config.dateFormat ?? defaults.dateFormat;
        return `strftime(${sourceRef}, '${fmt}')`;
    }

    if (isTimestampTZType(type)) {
        const fmt = config.dateTimeTZFormat ?? defaults.dateTimeTZFormat;
        return `strftime(${sourceRef}, '${fmt}')`;
    }

    if (isTimestampType(type)) {
        const fmt = config.dateTimeFormat ?? defaults.dateTimeFormat;
        return `strftime(${sourceRef}, '${fmt}')`;
    }

    if (isFloatingPointType(type) && config.decimalPlaces !== undefined) {
        return buildNumberExpr(sourceRef, config.decimalPlaces, colConfig, config.locale);
    }

    // Integer types, VARCHAR, BOOLEAN, others → pass-through
    return sourceRef;
}

function buildNumberExpr(
    sourceRef: string,
    decimalPlaces: number,
    colConfig: ColumnFormatConfig | undefined,
    locale: Locale,
): string {
    const formatSpecs = getFormatSpecs(locale);
    const formatString = formatSpecs.getNumberFormat(decimalPlaces);
    let expr = `format('${formatString}', ${sourceRef})`;

    if (colConfig?.currency) {
        expr = `CONCAT('${colConfig.currency} ', ${expr})`;
    }

    if (colConfig?.unit) {
        expr = `CONCAT(${expr}, ' ${colConfig.unit}')`;
    }

    return expr;
}

// ─── Legacy API (deprecated) ──────────────────────────────────────────────────

function formatColumn(columnName: string, config: ColumnFormatConfig, formatSpecs: FormatSpecs): string {
    if (config.dateFormat) {
        return `strftime(${columnName}, '${config.dateFormat}') AS ${columnName}_formatted`;
    }

    if (config.decimalPlaces !== undefined) {
        const formatString = formatSpecs.getNumberFormat(config.decimalPlaces);
        let formattedValue = `format('${formatString}', ${columnName})`;

        if (config.currency) {
            formattedValue = `CONCAT('${config.currency} ', ${formattedValue})`;
        }

        if (config.unit) {
            formattedValue = `CONCAT(${formattedValue}, ' ${config.unit}')`;
        }

        return `${formattedValue} AS ${columnName}_formatted`;
    }

    return columnName;
}

/**
 * Generate a formatting CTE that wraps the final query
 *
 * @deprecated Use `generateFormatSQL()` instead — it replaces columns rather than appending `_formatted` suffixes.
 *
 * @example
 * ```typescript
 * const sql = generateFormatCTE('final_data', {
 *   locale: 'pt-BR',
 *   columns: {
 *     consumption: { decimalPlaces: 3, unit: 'm³' },
 *     cost: { decimalPlaces: 2, currency: 'R$' },
 *     timestamp: { dateFormat: '%d/%m/%Y %H:%M:%S' }
 *   }
 * });
 * ```
 */
export function generateFormatCTE(sourceTable: string, config: FormatConfig): string {
    const formattedColumns: string[] = [];
    const formatSpecs = getFormatSpecs(config.locale);

    for (const [columnName, columnConfig] of Object.entries(config.columns ?? {})) {
        formattedColumns.push(formatColumn(columnName, columnConfig, formatSpecs));
    }

    formattedColumns.push('*');

    return `
SELECT
    ${formattedColumns.join(',\n    ')}
FROM ${sourceTable}
    `.trim();
}

/**
 * Generate inline formatting for a single value
 *
 * @deprecated Use `generateFormatSQL()` for full-query formatting instead.
 *
 * @example
 * ```typescript
 * const sql = `
 *   SELECT
 *     device_name,
 *     ${formatValue('consumption', 'pt-BR', { decimalPlaces: 3, unit: 'm³' })}
 *   FROM readings
 * `;
 * ```
 */
export function formatValue(columnName: string, locale: Locale, config: ColumnFormatConfig): string {
    const formatSpecs = getFormatSpecs(locale);
    return formatColumn(columnName, config, formatSpecs);
}

/**
 * Get the appropriate CSV delimiter for a locale
 *
 * Brazilian locale uses ; because , is the decimal separator
 * US locale uses , because . is the decimal separator
 */
export function getLocaleCSVDelimiter(locale: Locale): string {
    return locale === 'pt-BR' ? ';' : ',';
}
