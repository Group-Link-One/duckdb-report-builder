/**
 * End-to-End Pipeline Integration Tests
 *
 * Tests the complete pipeline combining:
 * - Device context application
 * - Time granularity coarsening
 * - Multi-source joins
 *
 * This replicates the "mismatched zipper" problem described in the plan:
 * - Consumption data at hourly granularity
 * - Battery data at minute granularity
 * - Coarsen battery to hourly to enable join
 */

import { InMemoryProvider, ReportWithContext } from 'duckdb-report-builder';

describe('End-to-End Pipeline', () => {
    describe('Device Context + Coarsen + Join', () => {
        it('should solve the mismatched zipper problem', async () => {
            /**
             * Scenario: Hourly consumption + Minute battery voltage
             * Goal: Join on aligned hourly timestamps
             */

            // 1. Device contexts (offsets/multipliers)
            const deviceContexts = [
                {
                    device_id: 101,
                    channel: 1,
                    multiplier: 0.001, // Convert Wh to kWh
                    raw_offset: 0,
                    result_offset: 0,
                },
            ];

            // 2. Raw hourly consumption from ClickHouse (already aggregated)
            const rawConsumption = [
                {
                    timestamp: new Date('2024-01-01T10:00:00Z'),
                    device_id: 101,
                    channel: 1,
                    daily_raw: 1000, // Wh
                },
                {
                    timestamp: new Date('2024-01-01T11:00:00Z'),
                    device_id: 101,
                    channel: 1,
                    daily_raw: 1200, // Wh
                },
                {
                    timestamp: new Date('2024-01-01T12:00:00Z'),
                    device_id: 101,
                    channel: 1,
                    daily_raw: 1100, // Wh
                },
            ];

            // 3. Minute-level battery data from ClickHouse
            const batteryMinute = [
                // Hour 10
                {
                    timestamp: new Date('2024-01-01T10:01:00Z'),
                    device_id: 101,
                    voltage: 12.5,
                    current: 2.1,
                },
                {
                    timestamp: new Date('2024-01-01T10:15:00Z'),
                    device_id: 101,
                    voltage: 12.7,
                    current: 2.3,
                },
                {
                    timestamp: new Date('2024-01-01T10:45:00Z'),
                    device_id: 101,
                    voltage: 12.3,
                    current: 2.0,
                },
                // Hour 11
                {
                    timestamp: new Date('2024-01-01T11:03:00Z'),
                    device_id: 101,
                    voltage: 11.9,
                    current: 1.8,
                },
                {
                    timestamp: new Date('2024-01-01T11:22:00Z'),
                    device_id: 101,
                    voltage: 12.0,
                    current: 1.9,
                },
                {
                    timestamp: new Date('2024-01-01T11:50:00Z'),
                    device_id: 101,
                    voltage: 12.1,
                    current: 2.0,
                },
                // Hour 12
                {
                    timestamp: new Date('2024-01-01T12:10:00Z'),
                    device_id: 101,
                    voltage: 12.4,
                    current: 2.2,
                },
                {
                    timestamp: new Date('2024-01-01T12:40:00Z'),
                    device_id: 101,
                    voltage: 12.2,
                    current: 2.1,
                },
            ];

            // Build report with full pipeline
            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('contexts', new InMemoryProvider(deviceContexts, 'device_contexts'))
                .load('consumption', new InMemoryProvider(rawConsumption, 'raw_consumption'))
                .load('battery', new InMemoryProvider(batteryMinute, 'battery_minute'))
                // Step 1: Apply device context to consumption (convert Wh -> kWh)
                .applyEnrichment('consumption', {
                    lookupSource: 'contexts',
                    joinOn: ['device_id', 'channel'],
                    formulas: {
                        consumption_kwh: {
                            formula: 'r.daily_raw * c.multiplier',
                        },
                    },
                    as: 'consumption_adjusted',
                })
                // Step 2: Coarsen battery data from minute -> hour
                .coarsen('battery', {
                    from: 'minute',
                    to: 'hour',
                    strategy: {
                        device_id: 'first', // Carry forward device_id
                        voltage: 'avg',
                        current: 'avg',
                    },
                    as: 'battery_hourly',
                })
                // Step 3: Join consumption + battery on timestamp and device_id
                .join('consumption_adjusted', 'battery_hourly', {
                    timestamp: 'timestamp',
                    device_id: 'device_id',
                })
                // Step 4: Select output columns
                .select([
                    'consumption_adjusted.timestamp',
                    'consumption_adjusted.device_id',
                    'consumption_adjusted.consumption_kwh',
                    'battery_hourly.voltage',
                    'battery_hourly.current',
                ])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== End-to-End Pipeline ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Assertions
            expect(result.data.length).toBe(3); // 3 hourly buckets

            // First hour (10:00)
            const hour10 = result.data[0];
            expect(Number(hour10.consumption_kwh)).toBeCloseTo(1.0, 2); // 1000 Wh * 0.001 = 1 kWh
            expect(Number(hour10.voltage)).toBeCloseTo(12.5, 1); // AVG(12.5, 12.7, 12.3)
            expect(Number(hour10.current)).toBeCloseTo(2.13, 1); // AVG(2.1, 2.3, 2.0)

            // Second hour (11:00)
            const hour11 = result.data[1];
            expect(Number(hour11.consumption_kwh)).toBeCloseTo(1.2, 2); // 1200 Wh * 0.001 = 1.2 kWh
            expect(Number(hour11.voltage)).toBeCloseTo(12.0, 1); // AVG(11.9, 12.0, 12.1)
            expect(Number(hour11.current)).toBeCloseTo(1.9, 1); // AVG(1.8, 1.9, 2.0)

            // Third hour (12:00)
            const hour12 = result.data[2];
            expect(Number(hour12.consumption_kwh)).toBeCloseTo(1.1, 2); // 1100 Wh * 0.001 = 1.1 kWh
            expect(Number(hour12.voltage)).toBeCloseTo(12.3, 1); // AVG(12.4, 12.2)
            expect(Number(hour12.current)).toBeCloseTo(2.15, 1); // AVG(2.2, 2.1)

            await report.close();
        });
    });

    describe('Complex pipeline with multiple coarsening steps', () => {
        it('should handle multi-step coarsening and joining', async () => {
            /**
             * Scenario: Daily consumption + Hourly temperature + Minute voltage
             * Goal: Coarsen temperature and voltage to daily, then join all
             */

            // Daily consumption
            const dailyConsumption = [
                { timestamp: new Date('2024-01-01T00:00:00Z'), device_id: 101, consumption: 24000 },
                { timestamp: new Date('2024-01-02T00:00:00Z'), device_id: 101, consumption: 26000 },
            ];

            // Hourly temperature
            const hourlyTemp = [
                { timestamp: new Date('2024-01-01T00:00:00Z'), device_id: 101, temp: 22.0 },
                { timestamp: new Date('2024-01-01T06:00:00Z'), device_id: 101, temp: 24.0 },
                { timestamp: new Date('2024-01-01T12:00:00Z'), device_id: 101, temp: 26.0 },
                { timestamp: new Date('2024-01-01T18:00:00Z'), device_id: 101, temp: 23.0 },
                { timestamp: new Date('2024-01-02T00:00:00Z'), device_id: 101, temp: 21.0 },
                { timestamp: new Date('2024-01-02T12:00:00Z'), device_id: 101, temp: 25.0 },
            ];

            // Minute voltage
            const minuteVoltage = [
                { timestamp: new Date('2024-01-01T10:00:00Z'), device_id: 101, voltage: 12.5 },
                { timestamp: new Date('2024-01-01T14:00:00Z'), device_id: 101, voltage: 12.3 },
                { timestamp: new Date('2024-01-02T08:00:00Z'), device_id: 101, voltage: 12.1 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-02T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('consumption', new InMemoryProvider(dailyConsumption, 'daily_consumption'))
                .load('temp', new InMemoryProvider(hourlyTemp, 'hourly_temp'))
                .load('voltage', new InMemoryProvider(minuteVoltage, 'minute_voltage'))
                // Coarsen temperature from hour -> day
                .coarsen('temp', {
                    from: 'hour',
                    to: 'day',
                    strategy: {
                        device_id: 'first', // Carry forward device_id
                        temp: 'avg',
                    },
                    as: 'temp_daily',
                })
                // Coarsen voltage from minute -> day (extreme coarsening)
                .coarsen('voltage', {
                    from: 'minute',
                    to: 'day',
                    strategy: {
                        device_id: 'first', // Carry forward device_id
                        voltage: 'avg',
                    },
                    as: 'voltage_daily',
                })
                // Join all three sources on timestamp and device_id
                .join('consumption', 'temp_daily', { timestamp: 'timestamp', device_id: 'device_id' })
                .join('consumption', 'voltage_daily', { timestamp: 'timestamp', device_id: 'device_id' })
                .select([
                    'consumption.timestamp',
                    'consumption.device_id',
                    'consumption.consumption',
                    'temp_daily.temp',
                    'voltage_daily.voltage',
                ])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Multi-Step Coarsening Pipeline ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Assertions
            expect(result.data.length).toBe(2); // 2 daily buckets

            // Day 1
            const day1 = result.data[0];
            expect(Number(day1.consumption)).toBe(24000);
            expect(Number(day1.temp)).toBeCloseTo(23.75, 1); // AVG(22, 24, 26, 23)
            expect(Number(day1.voltage)).toBeCloseTo(12.4, 1); // AVG(12.5, 12.3)

            // Day 2
            const day2 = result.data[1];
            expect(Number(day2.consumption)).toBe(26000);
            expect(Number(day2.temp)).toBeCloseTo(23.0, 1); // AVG(21, 25)
            expect(Number(day2.voltage)).toBeCloseTo(12.1, 1); // AVG(12.1)

            await report.close();
        });
    });

    describe('Full example from plan', () => {
        it('should replicate the plan example with device context + coarsen + pivot + join', async () => {
            /**
             * Replicates the Appendix example from the plan
             */

            // Device contexts
            const deviceContexts = [
                {
                    device_id: 101,
                    channel_index: 1,
                    raw_offset: 0,
                    result_offset: 0,
                    multiplier: 0.001, // Wh -> kWh
                },
            ];

            // Raw hourly consumption deltas
            const rawConsumption = [
                {
                    timestamp: new Date('2024-01-01T10:00:00Z'),
                    device_id: 101,
                    channel_index: 1,
                    daily_raw: 12500, // Wh
                    latest_raw: 500000,
                },
                {
                    timestamp: new Date('2024-01-01T11:00:00Z'),
                    device_id: 101,
                    channel_index: 1,
                    daily_raw: 14200, // Wh
                    latest_raw: 514200,
                },
            ];

            // Minute battery voltage
            const batteryMinute = [
                { timestamp: new Date('2024-01-01T10:05:00Z'), device_id: 101, voltage: 12.3 },
                { timestamp: new Date('2024-01-01T10:35:00Z'), device_id: 101, voltage: 12.1 },
                { timestamp: new Date('2024-01-01T11:15:00Z'), device_id: 101, voltage: 12.0 },
            ];

            const report = new ReportWithContext()
                .context({
                    from: new Date('2024-01-01T00:00:00Z'),
                    until: new Date('2024-01-01T23:59:59Z'),
                    timezone: 'UTC',
                    deviceIds: [101n],
                })
                .load('device_contexts', new InMemoryProvider(deviceContexts, 'device_contexts'))
                .load('raw_consumption', new InMemoryProvider(rawConsumption, 'raw_consumption'))
                .load('battery', new InMemoryProvider(batteryMinute, 'battery'))
                // Apply device context adjustments
                .applyEnrichment('raw_consumption', {
                    lookupSource: 'device_contexts',
                    joinOn: ['device_id', 'channel_index'],
                    formulas: {
                        consumption: {
                            formula: 'r.daily_raw * c.multiplier',
                        },
                        absolute: {
                            formula: '(r.latest_raw - c.raw_offset) * c.multiplier + c.result_offset',
                        },
                    },
                    as: 'consumption_adjusted',
                })
                // Coarsen battery from minute -> hour
                .coarsen('battery', {
                    from: 'minute',
                    to: 'hour',
                    strategy: {
                        device_id: 'first', // Carry forward device_id
                        voltage: 'avg',
                    },
                    as: 'battery_hourly',
                })
                // Join on timestamp and device_id
                .join('consumption_adjusted', 'battery_hourly', {
                    timestamp: 'timestamp',
                    device_id: 'device_id',
                })
                .select([
                    'consumption_adjusted.timestamp',
                    'consumption_adjusted.device_id',
                    'consumption_adjusted.consumption',
                    'consumption_adjusted.absolute',
                    'battery_hourly.voltage',
                ])
                .orderBy('timestamp', 'ASC');

            const result = await report.build();

            console.log('\n=== Full Plan Example ===');
            console.log(result.sql);
            console.log('Results:', result.data);

            // Assertions
            expect(result.data.length).toBe(2);

            // Hour 10:00
            const hour10 = result.data[0];
            expect(Number(hour10.consumption)).toBeCloseTo(12.5, 1); // 12500 Wh * 0.001 = 12.5 kWh
            expect(Number(hour10.absolute)).toBeCloseTo(500, 1); // 500000 * 0.001 = 500 kWh
            expect(Number(hour10.voltage)).toBeCloseTo(12.2, 1); // AVG(12.3, 12.1)

            // Hour 11:00
            const hour11 = result.data[1];
            expect(Number(hour11.consumption)).toBeCloseTo(14.2, 1); // 14200 Wh * 0.001 = 14.2 kWh
            expect(Number(hour11.absolute)).toBeCloseTo(514.2, 1); // 514200 * 0.001 = 514.2 kWh
            expect(Number(hour11.voltage)).toBeCloseTo(12.0, 1); // AVG(12.0)

            await report.close();
        });
    });
});
