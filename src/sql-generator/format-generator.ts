/**
 * Format Generator
 *
 * Generates SQL for final formatting using DuckDB's format() function.
 * Formatting happens as the FINAL step before output - not during processing.
 */

export type Locale = 'pt-BR' | 'en-US';

export interface ColumnFormatConfig {
    /** Number of decimal places */
    decimalPlaces?: number;

    /** Unit suffix (e.g., 'm³', 'kWh') */
    unit?: string;

    /** Currency prefix (e.g., 'R$', '$') */
    currency?: string;

    /** Date/time format string (DuckDB strftime format) */
    dateFormat?: string;
}

export interface FormatConfig {
    /** Locale for number/date formatting */
    locale: Locale;

    /** Column-specific formatting rules */
    columns: Record<string, ColumnFormatConfig>;
}

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

    for (const [columnName, columnConfig] of Object.entries(config.columns)) {
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
