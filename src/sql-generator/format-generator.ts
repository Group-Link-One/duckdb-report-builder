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

/**
 * Format Generator
 *
 * Creates SQL for formatting columns using DuckDB's format() function.
 * This should be the LAST transform before output.
 */
export class FormatGenerator {
    /**
     * Generate a formatting CTE that wraps the final query
     *
     * @param sourceTable The table to format
     * @param config Formatting configuration
     * @returns SQL for a formatting CTE
     *
     * @example
     * ```typescript
     * const sql = FormatGenerator.generateFormatCTE('final_data', {
     *   locale: 'pt-BR',
     *   columns: {
     *     consumption: { decimalPlaces: 3, unit: 'm³' },
     *     cost: { decimalPlaces: 2, currency: 'R$' },
     *     timestamp: { dateFormat: '%d/%m/%Y %H:%M:%S' }
     *   }
     * });
     * // Output: SELECT format(...) AS consumption, ... FROM final_data
     * ```
     */
    static generateFormatCTE(sourceTable: string, config: FormatConfig): string {
        const formattedColumns: string[] = [];
        const formatSpecs = this.getFormatSpecs(config.locale);

        for (const [columnName, columnConfig] of Object.entries(config.columns)) {
            formattedColumns.push(this.formatColumn(columnName, columnConfig, formatSpecs));
        }

        // Pass through any columns not specified in the format config
        formattedColumns.push('*');

        return `
SELECT
    ${formattedColumns.join(',\n    ')}
FROM ${sourceTable}
        `.trim();
    }

    /**
     * Format a single column based on its configuration
     */
    private static formatColumn(columnName: string, config: ColumnFormatConfig, formatSpecs: FormatSpecs): string {
        // Date/time formatting
        if (config.dateFormat) {
            return `strftime(${columnName}, '${config.dateFormat}') AS ${columnName}_formatted`;
        }

        // Numeric formatting
        if (config.decimalPlaces !== undefined) {
            const formatString = formatSpecs.getNumberFormat(config.decimalPlaces);
            let formattedValue = `format('${formatString}', ${columnName})`;

            // Add currency prefix
            if (config.currency) {
                formattedValue = `CONCAT('${config.currency} ', ${formattedValue})`;
            }

            // Add unit suffix
            if (config.unit) {
                formattedValue = `CONCAT(${formattedValue}, ' ${config.unit}')`;
            }

            return `${formattedValue} AS ${columnName}_formatted`;
        }

        // No formatting - pass through
        return columnName;
    }

    /**
     * Get locale-specific format specifications
     */
    private static getFormatSpecs(locale: Locale): FormatSpecs {
        return locale === 'pt-BR' ? new BrazilianFormatSpecs() : new USFormatSpecs();
    }

    /**
     * Generate inline formatting for a single value
     * Use this when you need to format a value in-place within a larger query
     *
     * @example
     * ```typescript
     * const sql = `
     *   SELECT
     *     device_name,
     *     ${FormatGenerator.formatValue('consumption', 'pt-BR', { decimalPlaces: 3, unit: 'm³' })}
     *   FROM readings
     * `;
     * ```
     */
    static formatValue(columnName: string, locale: Locale, config: ColumnFormatConfig): string {
        const formatSpecs = this.getFormatSpecs(locale);
        return this.formatColumn(columnName, config, formatSpecs);
    }
}

/**
 * Format specifications interface
 */
interface FormatSpecs {
    getNumberFormat(decimalPlaces: number): string;
}

/**
 * Brazilian format specifications
 * - Thousands separator: . (dot)
 * - Decimal separator: , (comma)
 * - Example: 1.234,567
 */
class BrazilianFormatSpecs implements FormatSpecs {
    getNumberFormat(decimalPlaces: number): string {
        // DuckDB format string for Brazilian locale
        // {:t..Nf} where t = thousands dot, .. = decimal comma, N = decimal places
        return `{:t..${decimalPlaces}f}`;
    }
}

/**
 * US format specifications
 * - Thousands separator: , (comma)
 * - Decimal separator: . (dot)
 * - Example: 1,234.567
 */
class USFormatSpecs implements FormatSpecs {
    getNumberFormat(decimalPlaces: number): string {
        // DuckDB format string for US locale
        // {:,.Nf} where , = thousands comma, . = decimal dot, N = decimal places
        return `{:,.${decimalPlaces}f}`;
    }
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
