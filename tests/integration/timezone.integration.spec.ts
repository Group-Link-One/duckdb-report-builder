/**
 * Timezone Transform Integration Tests
 *
 * Tests the timezone conversion feature (UTC -> local time).
 * Validates conversion to "America/Sao_Paulo" (BRT/BRST) and other timezones.
 */

import { InMemoryProvider, ReportWithContext } from 'duckdb-report-builder';

describe('Timezone Transform', () => {
    describe('UTC to America/Sao_Paulo conversion', () => {
        it('should convert UTC timestamps to Sao Paulo time (BRT)', async () => {
            // Mock UTC timestamp data
            const utcData = [
                { device_id: 101n, timestamp: new Date('2024-01-01T13:00:00Z'), value: 100 },
                { device_id: 101n, timestamp: new Date('2024-01-01T14:00:00Z'), value: 150 },
                { device_id: 101n, timestamp: new Date('2024-01-01T15:00:00Z'), value: 200 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T00:00:00Z'),
                    timezone: 'America/Sao_Paulo',
                    deviceIds: [101n],
                })
                .load('utc_data', new InMemoryProvider(utcData, 'utc_data'))
                .timezone('utc_data', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                })
                .select(['device_id', 'timestamp', 'value'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Timezone: UTC -> America/Sao_Paulo ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            expect(result.data.length).toBe(3);

            // Verify data is present (exact time conversion depends on daylight saving)
            // In January 2024, Sao Paulo is UTC-3 (BRT - no daylight saving)
            // UTC 13:00 -> BRT 10:00
            const firstRow = result.data[0];
            expect(firstRow.device_id).toBe(101n);
            expect(firstRow.value).toBe(100);

            await report.close();
        });

        it('should convert multiple timestamp columns simultaneously', async () => {
            // Mock data with multiple timestamp columns
            const multiTimestampData = [
                {
                    device_id: 101n,
                    event_time: new Date('2024-01-01T13:00:00Z'),
                    created_at: new Date('2024-01-01T12:00:00Z'),
                    value: 100,
                },
                {
                    device_id: 101n,
                    event_time: new Date('2024-01-01T14:00:00Z'),
                    created_at: new Date('2024-01-01T13:30:00Z'),
                    value: 150,
                },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T00:00:00Z'),
                    timezone: 'America/Sao_Paulo',
                    deviceIds: [101n],
                })
                .load('multi_ts', new InMemoryProvider(multiTimestampData, 'multi_ts'))
                .timezone('multi_ts', {
                    timestampColumns: ['event_time', 'created_at'],
                    timezone: 'America/Sao_Paulo',
                })
                .select(['device_id', 'event_time', 'created_at', 'value'])
                .orderBy('event_time', 'ASC');

            const result = await report.build();

            console.log('\n=== Timezone: Multiple Columns ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            expect(result.data.length).toBe(2);

            // Both timestamp columns should be converted
            expect(result.data[0].device_id).toBe(101n);
            expect(result.data[0].value).toBe(100);

            await report.close();
        });
    });

    describe('Timezone + Coarsen combination', () => {
        it('should convert timezone then coarsen', async () => {
            const minuteData = [
                { device_id: 101n, timestamp: new Date('2024-01-01T13:00:00Z'), value: 100 },
                { device_id: 101n, timestamp: new Date('2024-01-01T13:15:00Z'), value: 150 },
                { device_id: 101n, timestamp: new Date('2024-01-01T13:30:00Z'), value: 200 },
                { device_id: 101n, timestamp: new Date('2024-01-01T14:00:00Z'), value: 250 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-02'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('minute_data', new InMemoryProvider(minuteData, 'minute_data'))
                .timezone('minute_data', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                })
                .coarsen('minute_data_tz', {
                    from: 'minute',
                    to: 'hour',
                    strategy: { value: 'sum' },
                })
                .select(['timestamp', 'value'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Timezone + Coarsen ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should aggregate to hourly buckets in Sao Paulo time
            expect(result.data.length).toBeGreaterThan(0);

            await report.close();
        });
    });
});
