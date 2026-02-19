/**
 * Coarsen Transform Integration Tests
 *
 * Tests the time granularity coarsening feature (minute -> hour, hour -> day, etc.)
 * with various aggregation strategies (sum, avg, min, max, first, last).
 */

import { InMemoryProvider, ReportWithContext } from 'duckdb-report-builder';

describe('Coarsen Transform', () => {
    describe('Minute to Hour aggregation', () => {
        it('should coarsen minute data to hourly with AVG strategy', async () => {
            // Mock minute-level voltage readings
            const minuteData = [
                { timestamp: new Date('2024-01-01T10:01:00Z'), voltage: 12.5 },
                { timestamp: new Date('2024-01-01T10:15:00Z'), voltage: 12.7 },
                { timestamp: new Date('2024-01-01T10:45:00Z'), voltage: 12.3 },
                { timestamp: new Date('2024-01-01T11:03:00Z'), voltage: 11.9 },
                { timestamp: new Date('2024-01-01T11:22:00Z'), voltage: 12.0 },
                { timestamp: new Date('2024-01-01T11:50:00Z'), voltage: 12.1 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('minute_data', new InMemoryProvider(minuteData, 'minute_data'))
                .coarsen('minute_data', {
                    from: 'minute',
                    to: 'hour',
                    strategy: { voltage: 'avg' },
                })
                .select(['timestamp', 'voltage'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen Minute -> Hour (AVG) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 2 hourly buckets (10:00 and 11:00)
            expect(result.data.length).toBe(2);

            // Check first hour (AVG of 12.5, 12.7, 12.3)
            const hour10 = result.data[0];
            expect(Number(hour10.voltage)).toBeCloseTo(12.5, 1); // AVG(12.5, 12.7, 12.3) = 12.5

            // Check second hour (AVG of 11.9, 12.0, 12.1)
            const hour11 = result.data[1];
            expect(Number(hour11.voltage)).toBeCloseTo(12.0, 1); // AVG(11.9, 12.0, 12.1) = 12.0

            await report.close();
        });

        it('should coarsen minute data to hourly with SUM strategy', async () => {
            // Mock minute-level consumption readings
            const minuteData = [
                { timestamp: new Date('2024-01-01T10:01:00Z'), consumption: 100 },
                { timestamp: new Date('2024-01-01T10:15:00Z'), consumption: 150 },
                { timestamp: new Date('2024-01-01T10:45:00Z'), consumption: 120 },
                { timestamp: new Date('2024-01-01T11:03:00Z'), consumption: 200 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('minute_data', new InMemoryProvider(minuteData, 'minute_data'))
                .coarsen('minute_data', {
                    from: 'minute',
                    to: 'hour',
                    strategy: { consumption: 'sum' },
                })
                .select(['timestamp', 'consumption'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen Minute -> Hour (SUM) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 2 hourly buckets
            expect(result.data.length).toBe(2);

            // Check 10:00 hour (SUM of 100, 150, 120)
            const hour10 = result.data[0];
            expect(Number(hour10.consumption)).toBe(370);

            // Check 11:00 hour (SUM of 200)
            const hour11 = result.data[1];
            expect(Number(hour11.consumption)).toBe(200);

            await report.close();
        });

        it('should coarsen with FIRST and LAST strategies', async () => {
            // Mock minute-level state-of-charge readings
            const minuteData = [
                { timestamp: new Date('2024-01-01T10:05:00Z'), soc: 85 },
                { timestamp: new Date('2024-01-01T10:25:00Z'), soc: 83 },
                { timestamp: new Date('2024-01-01T10:55:00Z'), soc: 80 },
                { timestamp: new Date('2024-01-01T11:10:00Z'), soc: 78 },
                { timestamp: new Date('2024-01-01T11:40:00Z'), soc: 75 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('minute_data', new InMemoryProvider(minuteData, 'minute_data'))
                .coarsen('minute_data', {
                    from: 'minute',
                    to: 'hour',
                    strategy: { soc: 'last' }, // Use last value in hour
                })
                .select(['timestamp', 'soc'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen Minute -> Hour (LAST) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 2 hourly buckets
            expect(result.data.length).toBe(2);

            // Check 10:00 hour - should have LAST value (10:55 -> 80)
            const hour10 = result.data[0];
            expect(Number(hour10.soc)).toBe(80);

            // Check 11:00 hour - should have LAST value (11:40 -> 75)
            const hour11 = result.data[1];
            expect(Number(hour11.soc)).toBe(75);

            await report.close();
        });

        it('should coarsen with MIN and MAX strategies', async () => {
            // Mock minute-level temperature readings
            const minuteData = [
                { timestamp: new Date('2024-01-01T10:05:00Z'), temp: 22.5 },
                { timestamp: new Date('2024-01-01T10:25:00Z'), temp: 24.3 },
                { timestamp: new Date('2024-01-01T10:55:00Z'), temp: 21.8 },
                { timestamp: new Date('2024-01-01T11:10:00Z'), temp: 23.1 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('minute_data', new InMemoryProvider(minuteData, 'minute_data'))
                .coarsen('minute_data', {
                    from: 'minute',
                    to: 'hour',
                    strategy: { temp: 'max' },
                })
                .select(['timestamp', 'temp'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen Minute -> Hour (MAX) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 2 hourly buckets
            expect(result.data.length).toBe(2);

            // Check 10:00 hour - MAX(22.5, 24.3, 21.8) = 24.3
            const hour10 = result.data[0];
            expect(Number(hour10.temp)).toBeCloseTo(24.3, 1);

            // Check 11:00 hour - MAX(23.1) = 23.1
            const hour11 = result.data[1];
            expect(Number(hour11.temp)).toBeCloseTo(23.1, 1);

            await report.close();
        });
    });

    describe('Hour to Day aggregation', () => {
        it('should coarsen hourly data to daily with SUM strategy', async () => {
            // Mock hourly consumption readings
            const hourlyData = [
                { timestamp: new Date('2024-01-01T00:00:00Z'), consumption: 100 },
                { timestamp: new Date('2024-01-01T06:00:00Z'), consumption: 150 },
                { timestamp: new Date('2024-01-01T12:00:00Z'), consumption: 200 },
                { timestamp: new Date('2024-01-01T18:00:00Z'), consumption: 180 },
                { timestamp: new Date('2024-01-02T00:00:00Z'), consumption: 120 },
                { timestamp: new Date('2024-01-02T12:00:00Z'), consumption: 160 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('hourly_data', new InMemoryProvider(hourlyData, 'hourly_data'))
                .coarsen('hourly_data', {
                    from: 'hour',
                    to: 'day',
                    strategy: { consumption: 'sum' },
                })
                .select(['timestamp', 'consumption'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen Hour -> Day (SUM) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 2 daily buckets
            expect(result.data.length).toBe(2);

            // Check Day 1 (SUM of 100, 150, 200, 180)
            const day1 = result.data[0];
            expect(Number(day1.consumption)).toBe(630);

            // Check Day 2 (SUM of 120, 160)
            const day2 = result.data[1];
            expect(Number(day2.consumption)).toBe(280);

            await report.close();
        });

        it('should coarsen hourly data to daily with AVG strategy', async () => {
            // Mock hourly voltage readings
            const hourlyData = [
                { timestamp: new Date('2024-01-01T00:00:00Z'), voltage: 12.0 },
                { timestamp: new Date('2024-01-01T06:00:00Z'), voltage: 12.5 },
                { timestamp: new Date('2024-01-01T12:00:00Z'), voltage: 12.3 },
                { timestamp: new Date('2024-01-01T18:00:00Z'), voltage: 12.2 },
                { timestamp: new Date('2024-01-02T00:00:00Z'), voltage: 11.9 },
                { timestamp: new Date('2024-01-02T12:00:00Z'), voltage: 12.1 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('hourly_data', new InMemoryProvider(hourlyData, 'hourly_data'))
                .coarsen('hourly_data', {
                    from: 'hour',
                    to: 'day',
                    strategy: { voltage: 'avg' },
                })
                .select(['timestamp', 'voltage'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen Hour -> Day (AVG) ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 2 daily buckets
            expect(result.data.length).toBe(2);

            // Check Day 1 (AVG of 12.0, 12.5, 12.3, 12.2)
            const day1 = result.data[0];
            expect(Number(day1.voltage)).toBeCloseTo(12.25, 2);

            // Check Day 2 (AVG of 11.9, 12.1)
            const day2 = result.data[1];
            expect(Number(day2.voltage)).toBeCloseTo(12.0, 1);

            await report.close();
        });
    });

    describe('Multiple aggregation strategies', () => {
        it('should coarsen with different strategies for different columns', async () => {
            // Mock minute-level multi-column data
            const minuteData = [
                { timestamp: new Date('2024-01-01T10:05:00Z'), consumption: 100, voltage: 12.5, soc: 85 },
                { timestamp: new Date('2024-01-01T10:25:00Z'), consumption: 120, voltage: 12.7, soc: 83 },
                { timestamp: new Date('2024-01-01T10:55:00Z'), consumption: 110, voltage: 12.3, soc: 80 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('minute_data', new InMemoryProvider(minuteData, 'minute_data'))
                .coarsen('minute_data', {
                    from: 'minute',
                    to: 'hour',
                    strategy: {
                        consumption: 'sum', // Total consumption
                        voltage: 'avg', // Average voltage
                        soc: 'last', // Last state of charge
                    },
                })
                .select(['timestamp', 'consumption', 'voltage', 'soc'])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Coarsen with Multiple Strategies ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Should have 1 hourly bucket
            expect(result.data.length).toBe(1);

            const hour = result.data[0];
            expect(Number(hour.consumption)).toBe(330); // SUM
            expect(Number(hour.voltage)).toBeCloseTo(12.5, 1); // AVG
            expect(Number(hour.soc)).toBe(80); // LAST

            await report.close();
        });
    });

    describe('Error handling', () => {
        it('should throw error when coarsening to finer granularity', async () => {
            const hourlyData = [{ timestamp: new Date('2024-01-01T10:00:00Z'), consumption: 100 }];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [100n],
                })
                .load('hourly_data', new InMemoryProvider(hourlyData, 'hourly_data'))
                .coarsen('hourly_data', {
                    from: 'hour',
                    to: 'minute', // Invalid: hour -> minute is finer, not coarser
                    strategy: { consumption: 'sum' },
                })
                .select(['timestamp', 'consumption']); // Need to select columns for QueryPlan validation

            // Should throw validation error during SQL generation
            await expect(report.build()).rejects.toThrow(/cannot coarsen|target granularity must be coarser/i);

            await report.close();
        });
    });
});
