# Examples

## Table of Contents

- [Basic Report](#basic-report)
- [Pivot Transform](#pivot-transform)
- [LOCF (Last Observation Carried Forward)](#locf-last-observation-carried-forward)
- [Coarsening Time Granularity](#coarsening-time-granularity)
- [Joining Multiple Sources](#joining-multiple-sources)
- [Enrichment with Context Data](#enrichment-with-context-data)
- [Timezone Conversion](#timezone-conversion)
- [Window Functions](#window-functions)
- [Formatting Output](#formatting-output)
- [Complex Pipeline](#complex-pipeline)

---

## Basic Report

The simplest report loads data and selects columns:

```typescript
import { ReportWithContext, InMemoryProvider } from 'duckdb-report-builder';

const readings = [
    { device_id: 101, timestamp: new Date('2024-01-01T10:00:00Z'), value: 100 },
    { device_id: 101, timestamp: new Date('2024-01-01T11:00:00Z'), value: 150 },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(readings))
    .select(['device_id', 'timestamp', 'value']);

const result = await report.build();
console.log(result.data);
// [
//   { device_id: 101, timestamp: 2024-01-01T10:00:00.000Z, value: 100 },
//   { device_id: 101, timestamp: 2024-01-01T11:00:00.000Z, value: 150 }
// ]
```

---

## Pivot Transform

Convert rows to columns - useful when data is stored in EAV (Entity-Attribute-Value) format:

```typescript
// Raw data with metric_id column (EAV format)
const rawData = [
    { device_id: 101, timestamp: '2024-01-01T10:00:00Z', metric_id: 1, value: 22.5 },  // temperature
    { device_id: 101, timestamp: '2024-01-01T10:00:00Z', metric_id: 2, value: 1013 }, // pressure
    { device_id: 101, timestamp: '2024-01-01T11:00:00Z', metric_id: 1, value: 23.0 },  // temperature
    { device_id: 101, timestamp: '2024-01-01T11:00:00Z', metric_id: 2, value: 1012 }, // pressure
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(rawData))
    .pivot('readings', {
        on: 'metric_id',
        val: 'value',
        cols: [
            { id: 1, alias: 'temperature' },
            { id: 2, alias: 'pressure' },
        ],
        groupBy: ['device_id', 'timestamp'],
    })
    .select(['device_id', 'timestamp', 'temperature', 'pressure']);

const result = await report.build();
console.log(result.data);
// [
//   { device_id: 101, timestamp: '2024-01-01T10:00:00Z', temperature: 22.5, pressure: 1013 },
//   { device_id: 101, timestamp: '2024-01-01T11:00:00Z', temperature: 23.0, pressure: 1012 }
// ]
```

---

## LOCF (Last Observation Carried Forward)

Fill gaps in sparse data using a complete timeline:

```typescript
// Sparse sensor readings (only when values change)
const readings = [
    { device_id: 101, timestamp: '2024-01-01T10:00:00Z', temperature: 22.5 },
    // No reading at 11:00
    { device_id: 101, timestamp: '2024-01-01T12:00:00Z', temperature: 23.0 },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01T10:00:00Z'),
        until: new Date('2024-01-01T13:00:00Z'),
        timezone: 'UTC',
        deviceIds: [101],
    })
    .load('timeline', new TimelineProvider('hour', true))
    .load('readings', new InMemoryProvider(readings))
    .locf('readings', {
        baseTimeline: 'timeline',
        joinKeys: ['device_id'],
        columns: ['temperature'],
        maxLookbackSeconds: 7200, // 2 hours
    })
    .select(['timeline.timestamp', 'device_id', 'temperature'])
    .orderBy('timeline.timestamp', 'ASC');

const result = await report.build();
console.log(result.data);
// [
//   { timestamp: '2024-01-01T10:00:00Z', device_id: 101, temperature: 22.5 },
//   { timestamp: '2024-01-01T11:00:00Z', device_id: 101, temperature: 22.5 }, // carried forward
//   { timestamp: '2024-01-01T12:00:00Z', device_id: 101, temperature: 23.0 }
// ]
```

---

## Coarsening Time Granularity

Aggregate data from fine to coarse granularity:

```typescript
// Minute-level battery data
const batteryMinute = [
    { timestamp: '2024-01-01T10:01:00Z', device_id: 101, voltage: 12.5, current: 2.1 },
    { timestamp: '2024-01-01T10:15:00Z', device_id: 101, voltage: 12.7, current: 2.3 },
    { timestamp: '2024-01-01T10:45:00Z', device_id: 101, voltage: 12.3, current: 2.0 },
    { timestamp: '2024-01-01T11:03:00Z', device_id: 101, voltage: 11.9, current: 1.8 },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('battery', new InMemoryProvider(batteryMinute))
    .coarsen('battery', {
        from: 'minute',
        to: 'hour',
        strategy: {
            device_id: 'first',  // Carry forward
            voltage: 'avg',      // Average
            current: 'avg',      // Average
        },
    })
    .select(['timestamp', 'device_id', 'voltage', 'current'])
    .orderBy('timestamp', 'ASC');

const result = await report.build();
console.log(result.data);
// Hourly averages:
// [
//   { timestamp: '2024-01-01T10:00:00Z', device_id: 101, voltage: 12.5, current: 2.13 },
//   { timestamp: '2024-01-01T11:00:00Z', device_id: 101, voltage: 11.9, current: 1.8 }
// ]
```

---

## Joining Multiple Sources

Join data from different sources:

```typescript
const readings = [
    { timestamp: '2024-01-01T10:00:00Z', device_id: 101, value: 100 },
    { timestamp: '2024-01-01T10:00:00Z', device_id: 102, value: 200 },
];

const devices = [
    { device_id: 101, name: 'Sensor A', location: 'Building 1' },
    { device_id: 102, name: 'Sensor B', location: 'Building 2' },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(readings))
    .load('devices', new InMemoryProvider(devices))
    .join('readings', 'devices', { device_id: 'device_id' }, 'LEFT')
    .select([
        'readings.timestamp',
        'readings.device_id',
        'devices.name',
        'devices.location',
        'readings.value',
    ]);

const result = await report.build();
console.log(result.data);
// [
//   { timestamp: '2024-01-01T10:00:00Z', device_id: 101, name: 'Sensor A', location: 'Building 1', value: 100 },
//   { timestamp: '2024-01-01T10:00:00Z', device_id: 102, name: 'Sensor B', location: 'Building 2', value: 200 }
// ]
```

---

## Enrichment with Context Data

Apply context-specific calculations (offsets, multipliers, formulas):

```typescript
// Raw readings
const rawReadings = [
    { timestamp: '2024-01-01T10:00:00Z', device_id: 101, channel: 1, raw_value: 1000 },
    { timestamp: '2024-01-01T11:00:00Z', device_id: 101, channel: 1, raw_value: 1200 },
];

// Device contexts with conversion factors
const contexts = [
    { device_id: 101, channel: 1, multiplier: 0.001, offset: 0 },  // Wh -> kWh
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(rawReadings))
    .load('contexts', new InMemoryProvider(contexts))
    .applyEnrichment('readings', {
        lookupSource: 'contexts',
        joinOn: ['device_id', 'channel'],
        formulas: {
            energy_kwh: { formula: 'r.raw_value * c.multiplier' },
        },
    })
    .select(['timestamp', 'device_id', 'energy_kwh']);

const result = await report.build();
console.log(result.data);
// [
//   { timestamp: '2024-01-01T10:00:00Z', device_id: 101, energy_kwh: 1.0 },
//   { timestamp: '2024-01-01T11:00:00Z', device_id: 101, energy_kwh: 1.2 }
// ]
```

---

## Timezone Conversion

Convert UTC timestamps to local time:

```typescript
const readings = [
    { timestamp: '2024-01-01T10:00:00Z', device_id: 101, value: 100 },  // UTC
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'America/Sao_Paulo',
    })
    .load('readings', new InMemoryProvider(readings))
    .timezone('readings', {
        timestampColumns: ['timestamp'],
        timezone: 'America/Sao_Paulo',
    })
    .select(['timestamp', 'device_id', 'value']);

const result = await report.build();
console.log(result.data);
// [
//   { timestamp: '2024-01-01T07:00:00', device_id: 101, value: 100 }  // Sao Paulo time (UTC-3)
// ]
```

---

## Window Functions

### Calculate Delta Using LAG

```typescript
const dailyReadings = [
    { device_id: 101, date: '2024-01-01', total: 100 },
    { device_id: 101, date: '2024-01-02', total: 150 },
    { device_id: 101, date: '2024-01-03', total: 200 },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-04'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(dailyReadings))
    .window('readings', {
        partitionBy: ['device_id'],
        orderBy: [{ column: 'date', direction: 'ASC' }],
        windowFunctions: [{
            function: 'LAG',
            column: 'total',
            offset: 1,
            defaultValue: 0,
            outputAlias: 'prev_total',
        }],
    })
    .select([
        'device_id',
        'date',
        'total',
        'prev_total',
        ['total - prev_total', 'delta']  // Raw expression with alias
    ]);

const result = await report.build();
console.log(result.data);
// [
//   { device_id: 101, date: '2024-01-01', total: 100, prev_total: 0, delta: 100 },
//   { device_id: 101, date: '2024-01-02', total: 150, prev_total: 100, delta: 50 },
//   { device_id: 101, date: '2024-01-03', total: 200, prev_total: 150, delta: 50 }
// ]
```

### Get Latest Row per Device

```typescript
const readings = [
    { device_id: 101, timestamp: '2024-01-01 10:00:00', value: 100 },
    { device_id: 101, timestamp: '2024-01-01 11:00:00', value: 150 },
    { device_id: 102, timestamp: '2024-01-01 10:00:00', value: 50 },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(readings))
    .window('readings', {
        partitionBy: ['device_id'],
        orderBy: [{ column: 'timestamp', direction: 'DESC' }],
        windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
        qualify: 'rn = 1',  // Only keep latest row per device
    })
    .select(['device_id', 'timestamp', 'value']);

const result = await report.build();
console.log(result.data);
// [
//   { device_id: 101, timestamp: '2024-01-01 11:00:00', value: 150 },
//   { device_id: 102, timestamp: '2024-01-01 10:00:00', value: 50 }
// ]
```

---

## Formatting Output

Apply locale-specific formatting to output columns:

```typescript
const data = [
    { device_id: 101, consumption: 1234.5678, cost: 987.65, timestamp: '2024-01-01T10:00:00Z' },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-02'),
        timezone: 'UTC',
    })
    .load('readings', new InMemoryProvider(data))
    .select(['device_id', 'consumption', 'cost', 'timestamp'])
    .format({
        locale: 'pt-BR',
        columns: {
            consumption: { decimalPlaces: 3, unit: ' m³' },
            cost: { decimalPlaces: 2, currency: 'R$ ' },
            timestamp: { dateFormat: '%d/%m/%Y %H:%M:%S' }
        }
    });

const result = await report.build();
console.log(result.data);
// [
//   { 
//     device_id: 101, 
//     consumption: '1.234,568 m³',  // Brazilian format
//     cost: 'R$ 987,65',            // Brazilian currency format
//     timestamp: '01/01/2024 07:00:00'
//   }
// ]
```

---

## Complex Pipeline

Combine multiple transformations for a complete data pipeline:

```typescript
// Device contexts with conversion factors
const deviceContexts = [
    { device_id: 101, channel: 1, multiplier: 0.001, raw_offset: 0, result_offset: 0 },
];

// Raw hourly consumption
const rawConsumption = [
    { timestamp: '2024-01-01T10:00:00Z', device_id: 101, channel: 1, daily_raw: 1000 },
    { timestamp: '2024-01-01T11:00:00Z', device_id: 101, channel: 1, daily_raw: 1200 },
];

// Minute-level battery data
const batteryMinute = [
    { timestamp: '2024-01-01T10:05:00Z', device_id: 101, voltage: 12.3, current: 2.1 },
    { timestamp: '2024-01-01T10:35:00Z', device_id: 101, voltage: 12.5, current: 2.2 },
    { timestamp: '2024-01-01T11:15:00Z', device_id: 101, voltage: 12.1, current: 1.9 },
];

const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01T00:00:00Z'),
        until: new Date('2024-01-01T23:59:59Z'),
        timezone: 'UTC',
        deviceIds: [101],
    })
    // Load all sources
    .load('contexts', new InMemoryProvider(deviceContexts, 'device_contexts'))
    .load('consumption', new InMemoryProvider(rawConsumption, 'raw_consumption'))
    .load('battery', new InMemoryProvider(batteryMinute, 'battery_minute'))
    
    // Step 1: Apply device context to consumption (Wh -> kWh)
    .applyEnrichment('consumption', {
        lookupSource: 'contexts',
        joinOn: ['device_id', 'channel'],
        formulas: {
            consumption_kwh: { formula: 'r.daily_raw * c.multiplier' },
        },
        as: 'consumption_adjusted',
    })
    
    // Step 2: Coarsen battery data from minute -> hour
    .coarsen('battery', {
        from: 'minute',
        to: 'hour',
        strategy: {
            device_id: 'first',
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
    
    // Step 4: Select and order output
    .select([
        'consumption_adjusted.timestamp',
        'consumption_adjusted.device_id',
        'consumption_adjusted.consumption_kwh',
        'battery_hourly.voltage',
        'battery_hourly.current',
    ])
    .orderBy('timestamp', 'ASC');

const result = await report.build();
console.log(result.sql);  // View generated SQL
console.log(result.data); // View results
console.log(`Execution time: ${result.executionTimeMs}ms`);

await report.close();
```
