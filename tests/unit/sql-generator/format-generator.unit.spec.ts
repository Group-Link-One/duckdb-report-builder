/**
 * Format Generator Unit Tests
 *
 * Tests for format SQL generation.
 */

import {
    FormatConfig, generateFormatCTE, formatValue,
    getLocaleCSVDelimiter
} from '../../../src/sql-generator/format-generator';

describe('Format Generator', () => {
    describe('generateFormatCTE', () => {
        it('should generate format SQL with decimal places', () => {
            const config: FormatConfig = {
                locale: 'en-US',
                columns: {
                    price: { decimalPlaces: 2 },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            expect(sql).toContain('SELECT');
            expect(sql).toContain("format('{:,.2f}', price) AS price_formatted");
            expect(sql).toContain('FROM source_table');
            expect(sql).toContain('*');
        });

        it('should generate format SQL with currency', () => {
            const config: FormatConfig = {
                locale: 'en-US',
                columns: {
                    price: { decimalPlaces: 2, currency: '$' },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            expect(sql).toContain("CONCAT('$ ', format('{:,.2f}', price))");
        });

        it('should generate format SQL with unit', () => {
            const config: FormatConfig = {
                locale: 'en-US',
                columns: {
                    consumption: { decimalPlaces: 3, unit: 'kWh' },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            expect(sql).toContain("CONCAT(format('{:,.3f}', consumption), ' kWh')");
        });

        it('should generate format SQL with currency and unit', () => {
            const config: FormatConfig = {
                locale: 'pt-BR',
                columns: {
                    cost: { decimalPlaces: 2, currency: 'R$', unit: '/m³' },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            expect(sql).toContain("CONCAT('R$ '");
            expect(sql).toContain("/m³')");
        });

        it('should generate format SQL with date format', () => {
            const config: FormatConfig = {
                locale: 'en-US',
                columns: {
                    timestamp: { dateFormat: '%Y-%m-%d %H:%M:%S' },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            expect(sql).toContain("strftime(timestamp, '%Y-%m-%d %H:%M:%S') AS timestamp_formatted");
        });

        it('should generate format SQL with multiple columns', () => {
            const config: FormatConfig = {
                locale: 'pt-BR',
                columns: {
                    consumption: { decimalPlaces: 3, unit: 'm³' },
                    cost: { decimalPlaces: 2, currency: 'R$' },
                    timestamp: { dateFormat: '%d/%m/%Y %H:%M:%S' },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            expect(sql).toContain('consumption');
            expect(sql).toContain('cost');
            expect(sql).toContain('timestamp');
        });

        it('should use Brazilian format for pt-BR locale', () => {
            const config: FormatConfig = {
                locale: 'pt-BR',
                columns: {
                    value: { decimalPlaces: 2 },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            // Brazilian format uses {:t..Nf} for thousands dot, decimal comma
            expect(sql).toContain("'{:t..2f}'");
        });

        it('should use US format for en-US locale', () => {
            const config: FormatConfig = {
                locale: 'en-US',
                columns: {
                    value: { decimalPlaces: 2 },
                },
            };

            const sql = generateFormatCTE('source_table', config);

            // US format uses {:,.Nf} for thousands comma, decimal dot
            expect(sql).toContain("'{:,.2f}'");
        });
    });

    describe('formatValue', () => {
        it('should format value with decimal places', () => {
            const result = formatValue('price', 'en-US', {
                decimalPlaces: 2,
            });
            expect(result).toContain("format('{:,.2f}', price)");
            expect(result).toContain('AS price_formatted');
        });

        it('should format value with currency', () => {
            const result = formatValue('price', 'en-US', {
                decimalPlaces: 2,
                currency: '$',
            });
            expect(result).toContain("CONCAT('$ '");
            expect(result).toContain('price_formatted');
        });

        it('should format value with date', () => {
            const result = formatValue('created_at', 'en-US', {
                dateFormat: '%Y-%m-%d',
            });
            expect(result).toContain("strftime(created_at, '%Y-%m-%d')");
            expect(result).toContain('AS created_at_formatted');
        });

        it('should pass through unformatted columns', () => {
            const result = formatValue('id', 'en-US', {});
            expect(result).toBe('id');
        });
    });

    describe('getLocaleCSVDelimiter', () => {
        it('should return semicolon for pt-BR locale', () => {
            expect(getLocaleCSVDelimiter('pt-BR')).toBe(';');
        });

        it('should return comma for en-US locale', () => {
            expect(getLocaleCSVDelimiter('en-US')).toBe(',');
        });
    });
});
