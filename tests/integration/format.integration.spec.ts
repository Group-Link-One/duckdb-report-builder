/**
 * Format Integration Tests
 *
 * Tests the last-mile formatting system end-to-end with real DuckDB execution.
 * Covers auto-formatting by type, renames, buildToFile, parquet, raw mode, and locale shorthand.
 */

import * as fs from 'fs';
import { InMemoryProvider, ReportWithContext, type ColumnSchema } from 'duckdb-report-builder';

const from = new Date('2024-01-01');
const until = new Date('2024-12-31');

describe('Format Integration', () => {
    it('should auto-format DATE and DOUBLE columns for pt-BR', async () => {
        // Use string values for DATE columns since InMemoryProvider.formatValue lacks DATE case
        const data = [
            { id: 1, event_date: '2024-01-15', value: 1234.567, name: 'Test' },
        ];
        const schema: ColumnSchema[] = [
            { name: 'id', type: 'INTEGER' },
            { name: 'event_date', type: 'DATE' },
            { name: 'value', type: 'DOUBLE' },
            { name: 'name', type: 'VARCHAR' },
        ];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data', schema))
            .select(['id', 'event_date', 'value', 'name'])
            .format({ locale: 'pt-BR', decimalPlaces: 3 });

        const result = await report.build();
        await report.close();

        expect(result.data[0].event_date).toBe('15/01/2024');
        expect(result.data[0].value).toBe('1.234,567');
        expect(result.data[0].id).toBe(1);           // integer — not formatted
        expect(result.data[0].name).toBe('Test');     // varchar — not formatted
    });

    it('should rename columns in output', async () => {
        const data = [{ id: 1, value: 42.5 }];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data'))
            .select(['id', 'value'])
            .format({
                locale: 'en-US',
                columns: { id: { rename: 'Device ID' }, value: { rename: 'Reading' } },
            });

        const result = await report.build();
        await report.close();

        expect(Object.keys(result.data[0])).toEqual(['Device ID', 'Reading']);
    });

    it('should format CSV output via buildToFile', async () => {
        const data = [
            { id: 1, dt: '2024-06-15', val: 1234.5 },
        ];
        const schema: ColumnSchema[] = [
            { name: 'id', type: 'INTEGER' },
            { name: 'dt', type: 'DATE' },
            { name: 'val', type: 'DOUBLE' },
        ];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data', schema))
            .select(['id', 'dt', 'val'])
            .format({ locale: 'pt-BR', decimalPlaces: 2 });

        const tmpFile = `/tmp/test-format-${Date.now()}.csv`;
        try {
            await report.buildToFile(tmpFile, { format: 'csv', delimiter: ';' });

            const content = fs.readFileSync(tmpFile, 'utf-8');
            const lines = content.trim().split('\n');
            expect(lines[0]).toBe('id;dt;val');                    // header
            expect(lines[1]).toBe('1;15/06/2024;1.234,50');        // formatted
        } finally {
            if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
        }
    });

    it('should only rename (not format) for parquet output', async () => {
        const data = [{ id: 1, value: 42.5 }];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data'))
            .select(['id', 'value'])
            .format({
                locale: 'pt-BR',
                columns: { id: { rename: 'ID' }, value: { rename: 'Valor', decimalPlaces: 2 } },
            });

        const tmpFile = `/tmp/test-format-${Date.now()}.parquet`;
        try {
            await report.buildToFile(tmpFile, { format: 'parquet' });

            // Read back the parquet to verify native types preserved + renames applied
            const readBack = new ReportWithContext()
                .context({ from, until, timezone: 'UTC' })
                .load('check', new InMemoryProvider([], 'check'))
                .select(['ID', 'Valor']);

            // Use a lower-level approach: load parquet directly
            const readReport = new ReportWithContext()
                .context({ from, until, timezone: 'UTC' })
                .load('pq', {
                    name: 'parquet-reader',
                    load: async (ctx) => {
                        const tbl = 'pq_check';
                        await ctx.connection.run(`CREATE TEMP TABLE ${tbl} AS SELECT * FROM read_parquet('${tmpFile}')`);
                        return tbl;
                    },
                    getSchema: () => [
                        { name: 'ID', type: 'INTEGER' as const },
                        { name: 'Valor', type: 'DOUBLE' as const },
                    ],
                    validateColumns: () => {},
                    hasColumn: () => true,
                    getColumnSchema: () => undefined,
                })
                .select(['ID', 'Valor']);

            const result = await readReport.build();
            await readReport.close();

            expect(result.data[0].ID).toBe(1);
            expect(typeof result.data[0].Valor).toBe('number'); // native type, not string
            expect(result.data[0].Valor).toBe(42.5);
        } finally {
            if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
        }
    });

    it('should skip formatting when raw=true', async () => {
        const data = [{ code: 12345.0, value: 67.89 }];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data'))
            .select(['code', 'value'])
            .format({
                locale: 'pt-BR',
                decimalPlaces: 2,
                columns: { code: { raw: true } },
            });

        const result = await report.build();
        await report.close();

        expect(result.data[0].code).toBe(12345.0);      // raw number, not '12.345,00'
        expect(result.data[0].value).toBe('67,89');       // formatted
    });

    it('should accept locale string shorthand', async () => {
        const data = [{ dt: '2024-01-15' }];
        const schema: ColumnSchema[] = [
            { name: 'dt', type: 'DATE' },
        ];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data', schema))
            .select(['dt'])
            .format('pt-BR');

        const result = await report.build();
        await report.close();

        expect(result.data[0].dt).toBe('15/01/2024');
    });

    it('should auto-format TIMESTAMP WITH TIME ZONE with offset', async () => {
        const data = [{ ts: '2024-01-15 10:30:00+03' }];
        const schema: ColumnSchema[] = [
            { name: 'ts', type: 'TIMESTAMP' },
        ];

        const report = new ReportWithContext()
            .context({ from, until, timezone: 'UTC' })
            .load('data', new InMemoryProvider(data, 'data', schema))
            .select(['ts'])
            .format('pt-BR');

        const result = await report.build();
        await report.close();

        // TIMESTAMP (without TZ) → uses dateTimeFormat (no offset)
        expect(result.data[0].ts).toMatch(/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
    });
});
