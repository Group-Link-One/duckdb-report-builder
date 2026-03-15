/**
 * Empty Source Integration Tests
 *
 * Verifies that InMemoryProvider works correctly with zero rows.
 * This happens in production when a remote source returns no data
 * for the requested period (e.g., a new device with no readings yet).
 */

import { InMemoryProvider, ReportWithContext } from 'duckdb-report-builder';
import { ColumnSchema } from '../../src/providers/i-data-source-provider';

describe('Empty Source', () => {
    it('should handle InMemoryProvider with zero rows and explicit schema', async () => {
        const schema: ColumnSchema[] = [
            { name: 'timestamp', type: 'TIMESTAMP', nullable: false },
            { name: 'device_id', type: 'BIGINT', nullable: false },
            { name: 'value', type: 'DOUBLE', nullable: true },
        ];

        const report = new ReportWithContext()
            .context({
                from: new Date('2024-01-01'),
                until: new Date('2024-01-02'),
                timezone: 'UTC',
            })
            .load('readings', new InMemoryProvider([], 'empty_readings', schema))
            .select(['readings.timestamp', 'readings.device_id', 'readings.value'])

        const result = await report.build();

        expect(result.data).toEqual([]);
        expect(result.sql).toContain('SELECT');
        expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);

        await report.close();
    });

    it('should handle empty source joined with non-empty source', async () => {
        const emptySchema: ColumnSchema[] = [
            { name: 'timestamp', type: 'TIMESTAMP', nullable: false },
            { name: 'device_id', type: 'BIGINT', nullable: false },
            { name: 'value', type: 'DOUBLE', nullable: true },
        ];

        const deviceContexts = [
            { device_id: 101n, name: 'Device A' },
        ];

        const report = new ReportWithContext()
            .context({
                from: new Date('2024-01-01'),
                until: new Date('2024-01-02'),
                timezone: 'UTC',
            })
            .load('readings', new InMemoryProvider([], 'empty_readings', emptySchema))
            .load('devices', new InMemoryProvider(deviceContexts))
            .join('readings', 'devices', { device_id: 'device_id' })
            .select(['readings.timestamp', 'devices.name', 'readings.value']);

        const result = await report.build();

        expect(result.data).toEqual([]);

        await report.close();
    });

    it('should handle empty source with coarsen transform', async () => {
        const schema: ColumnSchema[] = [
            { name: 'timestamp', type: 'TIMESTAMP', nullable: false },
            { name: 'device_id', type: 'BIGINT', nullable: false },
            { name: 'value', type: 'DOUBLE', nullable: true },
        ];

        const report = new ReportWithContext()
            .context({
                from: new Date('2024-01-01'),
                until: new Date('2024-01-02'),
                timezone: 'UTC',
            })
            .load('readings', new InMemoryProvider([], 'empty_coarsen', schema))
            .coarsen('readings', {
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
                groupBy: ['device_id'],
            })
            .select(['timestamp', 'device_id', 'value']);

        const result = await report.build();

        expect(result.data).toEqual([]);

        await report.close();
    });
});
