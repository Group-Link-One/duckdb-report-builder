/**
 * Format Generator Unit Tests
 *
 * Tests for format SQL generation.
 */

import {
    FormatConfig, generateFormatCTE, formatValue,
    getLocaleCSVDelimiter, generateFormatSQL, generateRenameSQL,
    LOCALE_DEFAULTS, FormatColumnSchema,
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

    // ─── New API Tests ────────────────────────────────────────────────────────

    describe('generateFormatSQL', () => {
        const schema: FormatColumnSchema[] = [
            { name: 'device_id', type: 'BIGINT' },
            { name: 'event_date', type: 'DATE' },
            { name: 'consumption', type: 'DOUBLE' },
            { name: 'device_name', type: 'VARCHAR' },
        ];

        it('should auto-format DATE columns for pt-BR', () => {
            const sql = generateFormatSQL('raw_data', schema, { locale: 'pt-BR' });

            expect(sql).toContain("strftime(event_date, '%d/%m/%Y')");
            // BIGINT and VARCHAR pass through, DOUBLE without global decimalPlaces also passes
            expect(sql).toContain('device_id');
            expect(sql).toContain('device_name');
            expect(sql).not.toContain('format(');
        });

        it('should auto-format DATE columns for en-US', () => {
            const sql = generateFormatSQL('raw_data', schema, { locale: 'en-US' });

            expect(sql).toContain("strftime(event_date, '%Y-%m-%d')");
        });

        it('should auto-format DOUBLE when global decimalPlaces is set', () => {
            const sql = generateFormatSQL('raw_data', schema, {
                locale: 'pt-BR',
                decimalPlaces: 3,
            });

            expect(sql).toContain("format('{:t..3f}', consumption)");
            // BIGINT still passes through
            expect(sql).not.toContain("format('{:t..3f}', device_id)");
        });

        it('should NOT auto-format DOUBLE when global decimalPlaces is not set', () => {
            const sql = generateFormatSQL('raw_data', schema, { locale: 'pt-BR' });

            expect(sql).not.toContain("format(");
        });

        it('should rename columns via AS', () => {
            const sql = generateFormatSQL('raw_data', schema, {
                locale: 'pt-BR',
                columns: {
                    device_id: { rename: 'ID Dispositivo' },
                    event_date: { rename: 'Data' },
                    consumption: { rename: 'Consumo (m³)', decimalPlaces: 2 },
                    device_name: { rename: 'Nome do Dispositivo' },
                },
            });

            expect(sql).toContain('AS "ID Dispositivo"');
            expect(sql).toContain(' AS Data');                  // simple word — not quoted
            expect(sql).toContain('AS "Consumo (m³)"');
            expect(sql).toContain('AS "Nome do Dispositivo"');
        });

        it('should apply column-level decimalPlaces overriding global', () => {
            const sql = generateFormatSQL('raw_data', schema, {
                locale: 'pt-BR',
                decimalPlaces: 3,
                columns: {
                    consumption: { decimalPlaces: 2 },
                },
            });

            // Column-level override: 2 decimal places
            expect(sql).toContain("format('{:t..2f}', consumption)");
        });

        it('should skip formatting when raw=true', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'code', type: 'DOUBLE' },
                { name: 'value', type: 'DOUBLE' },
            ], {
                locale: 'pt-BR',
                decimalPlaces: 2,
                columns: { code: { raw: true } },
            });

            // code passes through, value gets formatted
            expect(sql).toContain("format('{:t..2f}', value)");
            expect(sql).not.toContain("format('{:t..2f}', code)");
            expect(sql).toMatch(/^\s*code[,\s]/m);
        });

        it('should rename even when raw=true', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'code', type: 'DOUBLE' },
            ], {
                locale: 'pt-BR',
                decimalPlaces: 2,
                columns: { code: { raw: true, rename: 'Código' } },
            });

            expect(sql).toContain('code AS "Código"');
            expect(sql).not.toContain("format(");
        });

        it('should handle TIMESTAMP auto-format', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'ts', type: 'TIMESTAMP' },
            ], { locale: 'pt-BR' });

            expect(sql).toContain("strftime(ts, '%d/%m/%Y %H:%M:%S')");
        });

        it('should handle TIMESTAMP WITH TIME ZONE with TZ format', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'ts', type: 'TIMESTAMP WITH TIME ZONE' },
            ], { locale: 'pt-BR' });

            expect(sql).toContain("strftime(ts, '%d/%m/%Y %H:%M:%S %z')");
        });

        it('should use custom dateTimeTZFormat when provided', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'ts', type: 'TIMESTAMP WITH TIME ZONE' },
            ], {
                locale: 'pt-BR',
                dateTimeTZFormat: '%d/%m/%Y %H:%M %Z',
            });

            expect(sql).toContain("strftime(ts, '%d/%m/%Y %H:%M %Z')");
        });

        it('should detect DECIMAL(p,s) as floating point', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'amount', type: 'DECIMAL(18,6)' },
            ], {
                locale: 'en-US',
                decimalPlaces: 2,
            });

            expect(sql).toContain("format('{:,.2f}', amount)");
        });

        it('should detect FLOAT and REAL as floating point', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'a', type: 'FLOAT' },
                { name: 'b', type: 'REAL' },
            ], {
                locale: 'en-US',
                decimalPlaces: 1,
            });

            expect(sql).toContain("format('{:,.1f}', a)");
            expect(sql).toContain("format('{:,.1f}', b)");
        });

        it('should apply currency and unit from column config', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'cost', type: 'DOUBLE' },
            ], {
                locale: 'pt-BR',
                columns: {
                    cost: { decimalPlaces: 2, currency: 'R$', unit: '/m³' },
                },
            });

            expect(sql).toContain("CONCAT('R$ '");
            expect(sql).toContain("/m³')");
        });

        it('should use custom dateFormat overriding locale default', () => {
            const sql = generateFormatSQL('raw_data', [
                { name: 'dt', type: 'DATE' },
            ], {
                locale: 'pt-BR',
                dateFormat: '%Y/%m/%d',
            });

            expect(sql).toContain("strftime(dt, '%Y/%m/%d')");
        });

        it('should handle empty schema', () => {
            const sql = generateFormatSQL('raw_data', [], { locale: 'pt-BR' });

            expect(sql).toBe('SELECT * FROM raw_data');
        });

        it('should match the full example from spec (CSV)', () => {
            const sql = generateFormatSQL('__export_raw', [
                { name: 'device_id', type: 'BIGINT' },
                { name: 'event_date', type: 'DATE' },
                { name: 'consumption', type: 'DOUBLE' },
                { name: 'device_name', type: 'VARCHAR' },
            ], {
                locale: 'pt-BR',
                decimalPlaces: 3,
                columns: {
                    device_id: { rename: 'ID Dispositivo' },
                    event_date: { rename: 'Data' },
                    consumption: { rename: 'Consumo (m³)', decimalPlaces: 2 },
                    device_name: { rename: 'Nome do Dispositivo' },
                },
            });

            expect(sql).toContain('device_id AS "ID Dispositivo"');
            expect(sql).toContain("strftime(event_date, '%d/%m/%Y') AS Data");
            expect(sql).toContain("format('{:t..2f}', consumption) AS \"Consumo (m³)\"");
            expect(sql).toContain('device_name AS "Nome do Dispositivo"');
            expect(sql).toContain('FROM __export_raw');
        });
    });

    describe('generateRenameSQL', () => {
        it('should rename columns without formatting', () => {
            const sql = generateRenameSQL('raw_data', [
                { name: 'device_id', type: 'BIGINT' },
                { name: 'value', type: 'DOUBLE' },
            ], {
                locale: 'pt-BR',
                decimalPlaces: 2,
                columns: {
                    device_id: { rename: 'ID' },
                    value: { rename: 'Valor', decimalPlaces: 2 },
                },
            });

            expect(sql).toContain('device_id AS ID');
            expect(sql).toContain('value AS Valor');
            // No formatting applied
            expect(sql).not.toContain('format(');
            expect(sql).not.toContain('strftime(');
        });

        it('should pass through columns without rename', () => {
            const sql = generateRenameSQL('raw_data', [
                { name: 'id', type: 'INTEGER' },
                { name: 'name', type: 'VARCHAR' },
            ], {
                locale: 'en-US',
            });

            expect(sql).toContain('id');
            expect(sql).toContain('name');
            expect(sql).not.toContain(' AS ');
        });

        it('should handle empty schema', () => {
            const sql = generateRenameSQL('raw_data', [], { locale: 'pt-BR' });
            expect(sql).toBe('SELECT * FROM raw_data');
        });
    });

    describe('LOCALE_DEFAULTS', () => {
        it('should have pt-BR defaults', () => {
            expect(LOCALE_DEFAULTS['pt-BR'].dateFormat).toBe('%d/%m/%Y');
            expect(LOCALE_DEFAULTS['pt-BR'].dateTimeFormat).toBe('%d/%m/%Y %H:%M:%S');
            expect(LOCALE_DEFAULTS['pt-BR'].dateTimeTZFormat).toBe('%d/%m/%Y %H:%M:%S %z');
        });

        it('should have en-US defaults', () => {
            expect(LOCALE_DEFAULTS['en-US'].dateFormat).toBe('%Y-%m-%d');
            expect(LOCALE_DEFAULTS['en-US'].dateTimeFormat).toBe('%Y-%m-%d %H:%M:%S');
            expect(LOCALE_DEFAULTS['en-US'].dateTimeTZFormat).toBe('%Y-%m-%d %H:%M:%S %z');
        });
    });
});
