/**
 * DuckDB Config Integration Tests
 *
 * Verifies that users can configure DuckDB instance settings
 * (memory, threads, path, spill directory, etc.) via the fluent API.
 */

import { InMemoryProvider, ReportWithContext } from 'duckdb-report-builder';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

describe('DuckDB Config Integration', () => {
    it('should accept custom settings (threads, memory_limit)', async () => {
        const data = [
            { device_id: 1n, value: 100 },
            { device_id: 2n, value: 200 },
        ];

        const report = new ReportWithContext()
            .duckdb({
                settings: {
                    threads: '1',
                    memory_limit: '256MB',
                },
            })
            .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
            .load('readings', new InMemoryProvider(data, 'readings'))
            .select(['device_id', 'value']);

        const result = await report.build();
        await report.close();

        expect(result.data).toHaveLength(2);
    });

    it('should accept a file-based database path', async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-test-'));
        const dbPath = path.join(tmpDir, 'test.duckdb');

        try {
            const data = [{ device_id: 1n, value: 42 }];

            const report = new ReportWithContext()
                .duckdb({ path: dbPath })
                .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
                .load('readings', new InMemoryProvider(data, 'readings'))
                .select(['device_id', 'value']);

            const result = await report.build();
            await report.close();

            expect(result.data).toHaveLength(1);
            expect(fs.existsSync(dbPath)).toBe(true);
        } finally {
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });

    it('should work with default config (in-memory, no custom settings)', async () => {
        const data = [{ device_id: 1n, value: 10 }];

        const report = new ReportWithContext()
            .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
            .load('readings', new InMemoryProvider(data, 'readings'))
            .select(['device_id', 'value']);

        const result = await report.build();
        await report.close();

        expect(result.data).toHaveLength(1);
    });
});
