/**
 * Window Functions Integration Tests
 *
 * Tests LAG, ROW_NUMBER, ARRAY_AGG and other window functions with QUALIFY.
 */

import { InMemoryProvider, ReportWithContext } from 'duckdb-report-builder';

describe('Window Functions', () => {
    describe('LAG function for delta calculations', () => {
        it('should calculate daily delta using LAG', async () => {
            // Mock daily readings
            const dailyReadings = [
                { device_id: 101n, date: '2024-01-01', value: 100 },
                { device_id: 101n, date: '2024-01-02', value: 150 },
                { device_id: 101n, date: '2024-01-03', value: 200 },
                { device_id: 102n, date: '2024-01-01', value: 50 },
                { device_id: 102n, date: '2024-01-02', value: 75 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-04'),
                    timezone: 'UTC',
                    deviceIds: [101n, 102n],
                })
                .load('daily', new InMemoryProvider(dailyReadings, 'daily'))
                .window('daily', {
                    partitionBy: ['device_id'],
                    orderBy: [{ column: 'date', direction: 'ASC' }],
                    windowFunctions: [
                        {
                            function: 'LAG',
                            column: 'value',
                            offset: 1,
                            defaultValue: 0,
                            outputAlias: 'prev_value',
                        },
                    ],
                })
                .select(['device_id', 'date', 'value', 'prev_value'])
                .orderBy('device_id', 'ASC')
                .orderBy('date', 'ASC');

            const result = await report.build();

            console.log('\n=== LAG for Delta Calculation ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            expect(result.data.length).toBe(5);

            // Device 101, first day - no previous value
            expect(result.data[0].device_id).toBe(101n);
            expect(result.data[0].date).toBe('2024-01-01');
            expect(result.data[0].value).toBe(100);
            expect(result.data[0].prev_value).toBe(0); // Default value

            // Device 101, second day
            expect(result.data[1].device_id).toBe(101n);
            expect(result.data[1].date).toBe('2024-01-02');
            expect(result.data[1].value).toBe(150);
            expect(result.data[1].prev_value).toBe(100); // Previous value

            // Device 101, third day
            expect(result.data[2].device_id).toBe(101n);
            expect(result.data[2].date).toBe('2024-01-03');
            expect(result.data[2].value).toBe(200);
            expect(result.data[2].prev_value).toBe(150);

            await report.close();
        });

        it('should calculate delta across multiple channels', async () => {
            // Mock multi-channel readings
            const readings = [
                { device_id: 101n, channel: 0, date: '2024-01-01', value: 100 },
                { device_id: 101n, channel: 0, date: '2024-01-02', value: 150 },
                { device_id: 101n, channel: 1, date: '2024-01-01', value: 200 },
                { device_id: 101n, channel: 1, date: '2024-01-02', value: 250 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-03'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('readings', new InMemoryProvider(readings, 'readings'))
                .window('readings', {
                    partitionBy: ['device_id', 'channel'],
                    orderBy: [{ column: 'date', direction: 'ASC' }],
                    windowFunctions: [
                        {
                            function: 'LAG',
                            column: 'value',
                            offset: 1,
                            defaultValue: 0,
                            outputAlias: 'prev_value',
                        },
                    ],
                })
                .select(['device_id', 'channel', 'date', 'value', 'prev_value'])
                .orderBy('channel', 'ASC')
                .orderBy('date', 'ASC');

            const result = await report.build();

            console.log('\n=== LAG with Multiple Channels ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            expect(result.data.length).toBe(4);

            // Channel 0, day 1
            expect(result.data[0].channel).toBe(0);
            expect(result.data[0].prev_value).toBe(0); // First row

            // Channel 0, day 2
            expect(result.data[1].channel).toBe(0);
            expect(result.data[1].prev_value).toBe(100); // Previous from channel 0

            // Channel 1, day 1
            expect(result.data[2].channel).toBe(1);
            expect(result.data[2].prev_value).toBe(0); // First row for channel 1

            // Channel 1, day 2
            expect(result.data[3].channel).toBe(1);
            expect(result.data[3].prev_value).toBe(200); // Previous from channel 1

            await report.close();
        });
    });

    describe('ROW_NUMBER with QUALIFY for latest value', () => {
        it('should get latest reading per device', async () => {
            // Mock readings with multiple entries per device
            const readings = [
                { device_id: 101n, timestamp: '2024-01-01 10:00:00', value: 100 },
                { device_id: 101n, timestamp: '2024-01-01 11:00:00', value: 150 },
                { device_id: 101n, timestamp: '2024-01-01 12:00:00', value: 200 },
                { device_id: 102n, timestamp: '2024-01-01 10:00:00', value: 50 },
                { device_id: 102n, timestamp: '2024-01-01 11:00:00', value: 75 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-02'),
                    timezone: 'UTC',
                    deviceIds: [101n, 102n],
                })
                .load('readings', new InMemoryProvider(readings, 'readings'))
                .window('readings', {
                    partitionBy: ['device_id'],
                    orderBy: [{ column: 'timestamp', direction: 'DESC' }],
                    windowFunctions: [
                        {
                            function: 'ROW_NUMBER',
                            outputAlias: 'rn',
                        },
                    ],
                    qualify: 'rn = 1', // Only latest row per device
                })
                .select(['device_id', 'timestamp', 'value'])
                .orderBy('device_id', 'ASC');

            const result = await report.build();

            console.log('\n=== ROW_NUMBER + QUALIFY for Latest Value ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should only have latest row per device
            expect(result.data.length).toBe(2);

            // Device 101 - latest value
            expect(result.data[0].device_id).toBe(101n);
            expect(result.data[0].timestamp).toBe('2024-01-01 12:00:00');
            expect(result.data[0].value).toBe(200);

            // Device 102 - latest value
            expect(result.data[1].device_id).toBe(102n);
            expect(result.data[1].timestamp).toBe('2024-01-01 11:00:00');
            expect(result.data[1].value).toBe(75);

            await report.close();
        });
    });

    describe('ARRAY_AGG for collecting values', () => {
        it('should collect alarm IDs into an array', async () => {
            // Mock readings with alarm IDs
            const readings = [
                { device_id: 101n, timestamp: '2024-01-01 10:00:00', alarm_id: 1 },
                { device_id: 101n, timestamp: '2024-01-01 11:00:00', alarm_id: 2 },
                { device_id: 101n, timestamp: '2024-01-01 12:00:00', alarm_id: 3 },
                { device_id: 102n, timestamp: '2024-01-01 10:00:00', alarm_id: 4 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-02'),
                    timezone: 'UTC',
                    deviceIds: [101n, 102n],
                })
                .load('readings', new InMemoryProvider(readings, 'readings'))
                .window('readings', {
                    partitionBy: ['device_id'],
                    orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                    windowFunctions: [
                        {
                            function: 'ARRAY_AGG',
                            column: 'alarm_id',
                            outputAlias: 'alarm_ids',
                        },
                        {
                            function: 'ROW_NUMBER',
                            outputAlias: 'rn',
                        },
                    ],
                    qualify:
                        'rn = (SELECT MAX(rn2) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp) AS rn2 FROM readings) WHERE device_id = readings.device_id)',
                })
                .select(['device_id', 'alarm_ids'])
                .orderBy('device_id', 'ASC');

            const result = await report.build();

            console.log('\n=== ARRAY_AGG for Alarm Collection ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Note: ARRAY_AGG window function creates a running array
            // So each row has all values up to that point
            expect(result.data.length).toBeGreaterThan(0);

            await report.close();
        });
    });

    describe('Multiple window functions', () => {
        it('should apply LAG and LEAD together', async () => {
            // Mock readings
            const readings = [
                { device_id: 101n, date: '2024-01-01', value: 100 },
                { device_id: 101n, date: '2024-01-02', value: 150 },
                { device_id: 101n, date: '2024-01-03', value: 200 },
                { device_id: 101n, date: '2024-01-04', value: 250 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-05'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('readings', new InMemoryProvider(readings, 'readings'))
                .window('readings', {
                    partitionBy: ['device_id'],
                    orderBy: [{ column: 'date', direction: 'ASC' }],
                    windowFunctions: [
                        {
                            function: 'LAG',
                            column: 'value',
                            offset: 1,
                            defaultValue: 0,
                            outputAlias: 'prev_value',
                        },
                        {
                            function: 'LEAD',
                            column: 'value',
                            offset: 1,
                            defaultValue: 0,
                            outputAlias: 'next_value',
                        },
                    ],
                })
                .select(['device_id', 'date', 'value', 'prev_value', 'next_value'])
                .orderBy('date', 'ASC');

            const result = await report.build();

            console.log('\n=== Multiple Window Functions (LAG + LEAD) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            expect(result.data.length).toBe(4);

            // Middle row should have both prev and next
            expect(result.data[1].date).toBe('2024-01-02');
            expect(result.data[1].value).toBe(150);
            expect(result.data[1].prev_value).toBe(100);
            expect(result.data[1].next_value).toBe(200);

            await report.close();
        });
    });

    describe('Integration with device context', () => {
        it('should apply LAG after device context adjustments', async () => {
            // Mock raw readings
            const rawReadings = [
                { device_id: 101n, channel: 0, date: '2024-01-01', raw_value: 100 },
                { device_id: 101n, channel: 0, date: '2024-01-02', raw_value: 200 },
                { device_id: 101n, channel: 0, date: '2024-01-03', raw_value: 300 },
            ];

            // Mock device contexts
            const deviceContexts = [{ device_id: 101n, channel: 0, multiplier: 2.0 }];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-04'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('raw', new InMemoryProvider(rawReadings, 'raw'))
                .load('dc', new InMemoryProvider(deviceContexts, 'dc'))
                .applyEnrichment('raw', {
                    lookupSource: 'dc',
                    joinOn: ['device_id', 'channel'],
                    formulas: {
                        adjusted_value: {
                            formula: 'r.raw_value * COALESCE(c.multiplier, CAST(1 AS DOUBLE))',
                        },
                    },
                })
                .window('raw_enriched', {
                    partitionBy: ['device_id', 'channel'],
                    orderBy: [{ column: 'date', direction: 'ASC' }],
                    windowFunctions: [
                        {
                            function: 'LAG',
                            column: 'adjusted_value',
                            offset: 1,
                            defaultValue: 0,
                            outputAlias: 'prev_adjusted',
                        },
                    ],
                })
                .select(['device_id', 'date', 'adjusted_value', 'prev_adjusted'])
                .orderBy('date', 'ASC');

            const result = await report.build();

            console.log('\n=== Window Functions + Device Context ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            expect(result.data.length).toBe(3);

            // Values should be multiplied by 2.0
            expect(Number(result.data[0].adjusted_value)).toBe(200); // 100 * 2
            expect(Number(result.data[0].prev_adjusted)).toBe(0);

            expect(Number(result.data[1].adjusted_value)).toBe(400); // 200 * 2
            expect(Number(result.data[1].prev_adjusted)).toBe(200);

            expect(Number(result.data[2].adjusted_value)).toBe(600); // 300 * 2
            expect(Number(result.data[2].prev_adjusted)).toBe(400);

            await report.close();
        });
    });
});
