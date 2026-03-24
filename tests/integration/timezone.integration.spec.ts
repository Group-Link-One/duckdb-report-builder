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

    /**
     * Helper to convert DuckDBTimestampValue to a comparable string.
     * DuckDBTimestampValue has .micros (BigInt, microseconds since epoch).
     * After timezone conversion the result is a bare TIMESTAMP (not TIMESTAMPTZ),
     * so the micros represent "wall clock" in the target timezone.
     * Converting to Date (which interprets as UTC) gives us the wall-clock string.
     */
    function tsToString(val: any): string {
        const ms = Number(val.micros / 1000n);
        return new Date(ms).toISOString().replace('.000Z', '');
    }

    describe('Exact timestamp assertions', () => {
        it('should correctly convert UTC timestamps to São Paulo time (BRT = UTC-3)', async () => {
            // January = BRT (no DST since Brazil abolished it in 2019), so UTC-3
            const utcData = [
                { device_id: 1n, timestamp: new Date('2024-01-01T13:00:00Z'), value: 10 },
                { device_id: 1n, timestamp: new Date('2024-01-01T14:00:00Z'), value: 20 },
                { device_id: 1n, timestamp: new Date('2024-01-01T15:00:00Z'), value: 30 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T00:00:00Z'),
                    timezone: 'America/Sao_Paulo',
                    deviceIds: [1n],
                })
                .load('data', new InMemoryProvider(utcData, 'data'))
                .timezone('data', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                })
                .select(['device_id', 'timestamp', 'value'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            expect(result.data.length).toBe(3);
            expect(tsToString(result.data[0].timestamp)).toBe('2024-01-01T10:00:00');
            expect(tsToString(result.data[1].timestamp)).toBe('2024-01-01T11:00:00');
            expect(tsToString(result.data[2].timestamp)).toBe('2024-01-01T12:00:00');

            // Values should be unchanged
            expect(result.data[0].value).toBe(10);
            expect(result.data[1].value).toBe(20);
            expect(result.data[2].value).toBe(30);

            await report.close();
        });

        it('should correctly shift timestamps across day boundaries', async () => {
            // UTC 02:00 on Jan 15 → São Paulo (UTC-3) = 23:00 on Jan 14
            // This is the key regression test: the old bug would NOT shift across day boundaries
            const utcData = [
                { device_id: 1n, timestamp: new Date('2024-01-15T02:00:00Z'), value: 42 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-14T00:00:00Z'),
                    until: new Date('2024-01-16T00:00:00Z'),
                    timezone: 'America/Sao_Paulo',
                    deviceIds: [1n],
                })
                .load('data', new InMemoryProvider(utcData, 'data'))
                .timezone('data', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                })
                .select(['device_id', 'timestamp', 'value']);

            const result = await report.build();

            expect(result.data.length).toBe(1);
            // Must be previous day — the old single AT TIME ZONE bug went the wrong direction
            expect(tsToString(result.data[0].timestamp)).toBe('2024-01-14T23:00:00');
            expect(result.data[0].value).toBe(42);

            await report.close();
        });

        it('should handle DST transitions (America/New_York EDT vs EST)', async () => {
            // America/New_York:
            //   EDT (summer) = UTC-4
            //   EST (winter) = UTC-5
            // DST 2024: starts Mar 10, ends Nov 3
            const utcData = [
                // June 15 is EDT (UTC-4): 18:00 UTC → 14:00 EDT
                { device_id: 1n, timestamp: new Date('2024-06-15T18:00:00Z'), value: 100 },
                // Dec 15 is EST (UTC-5): 18:00 UTC → 13:00 EST
                { device_id: 1n, timestamp: new Date('2024-12-15T18:00:00Z'), value: 200 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2025-01-01T00:00:00Z'),
                    timezone: 'America/New_York',
                    deviceIds: [1n],
                })
                .load('data', new InMemoryProvider(utcData, 'data'))
                .timezone('data', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/New_York',
                })
                .select(['device_id', 'timestamp', 'value'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            expect(result.data.length).toBe(2);
            // EDT (UTC-4): 18:00 UTC → 14:00 local
            expect(tsToString(result.data[0].timestamp)).toBe('2024-06-15T14:00:00');
            expect(result.data[0].value).toBe(100);
            // EST (UTC-5): 18:00 UTC → 13:00 local
            expect(tsToString(result.data[1].timestamp)).toBe('2024-12-15T13:00:00');
            expect(result.data[1].value).toBe(200);

            await report.close();
        });

        it('should not produce duplicate columns (EXCLUDE fix)', async () => {
            // The old bug: SELECT *, (ts AT TIME ZONE ...) AS ts created duplicate columns.
            // DuckDB resolved ambiguous refs to the original (UTC) value.
            const utcData = [
                { device_id: 1n, timestamp: new Date('2024-01-01T15:00:00Z'), value: 99 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T00:00:00Z'),
                    timezone: 'America/Sao_Paulo',
                    deviceIds: [1n],
                })
                .load('data', new InMemoryProvider(utcData, 'data'))
                .timezone('data', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                })
                .select(['device_id', 'timestamp', 'value']);

            const result = await report.build();

            expect(result.data.length).toBe(1);

            const row = result.data[0];
            // Should have exactly three keys — no duplicate "timestamp"
            const keys = Object.keys(row);
            expect(keys).toHaveLength(3);
            expect(keys).toContain('device_id');
            expect(keys).toContain('timestamp');
            expect(keys).toContain('value');

            // The timestamp must be the converted value (12:00 SP), not the original (15:00 UTC)
            expect(tsToString(row.timestamp)).toBe('2024-01-01T12:00:00');

            await report.close();
        });

        it('timezone conversion should work with downstream enrichment', async () => {
            // Apply timezone conversion, then enrichment that extracts the hour.
            // If enrichment sees the original UTC timestamp, EXTRACT(HOUR ...) would give 15.
            // If enrichment sees the converted SP timestamp, it gives 12.
            const readings = [
                { device_id: 1n, channel: 1, timestamp: new Date('2024-01-01T15:00:00Z'), raw_value: 500 },
            ];

            const contexts = [
                { device_id: 1n, channel: 1, multiplier: 0.001 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T00:00:00Z'),
                    timezone: 'America/Sao_Paulo',
                    deviceIds: [1n],
                })
                .load('readings', new InMemoryProvider(readings, 'readings'))
                .load('contexts', new InMemoryProvider(contexts, 'contexts'))
                .timezone('readings', {
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                })
                .applyEnrichment('readings_tz', {
                    lookupSource: 'contexts',
                    joinOn: ['device_id', 'channel'],
                    formulas: {
                        local_hour: { formula: 'EXTRACT(HOUR FROM r.timestamp)' },
                        adjusted_value: { formula: 'r.raw_value * c.multiplier' },
                    },
                    as: 'enriched',
                })
                .select(['device_id', 'timestamp', 'local_hour', 'adjusted_value']);

            const result = await report.build();

            expect(result.data.length).toBe(1);

            const row = result.data[0];
            // The enrichment should see the converted timestamp (12:00 SP), not 15:00 UTC
            expect(Number(row.local_hour)).toBe(12);
            // Calculation should also work correctly
            expect(row.adjusted_value).toBeCloseTo(0.5, 5);

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
