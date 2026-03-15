# duckdb-report-builder

A SQL-first report builder using DuckDB for data transformation and orchestration.

## Features

- **SQL-first architecture**: All data transformations happen in SQL (DuckDB), TypeScript handles only orchestration and I/O
- **Fluent API**: Chainable builder pattern for constructing reports
- **Provider pattern**: Abstract different data sources (in-memory, timeline, custom)
- **Rich transformations**: Pivot, LOCF (last observation carried forward), coarsen, window functions, joins, filters, timezone conversion, enrichment
- **Flexible execution**: CTE mode (single query) or temp table mode (step-by-step with callbacks)
- **Type-safe**: Full TypeScript support with comprehensive type definitions

> **Note:** This library is in early development (pre-1.0.0). Breaking changes may occur in minor version bumps (0.X.0). Pin your dependency to a specific version until 1.0.0 is released.

## Installation

```bash
npm install duckdb-report-builder
```

## Quick Start

```typescript
import { ReportWithContext, InMemoryProvider } from 'duckdb-report-builder';

// Create a report
const report = new ReportWithContext()
    .context({
        from: new Date('2024-01-01'),
        until: new Date('2024-01-31'),
        timezone: 'America/Sao_Paulo',
        entityIds: [100n, 200n],
    })
    .load('readings', new InMemoryProvider(rawData))
    .select(['timestamp', 'device_id', 'value']);

const result = await report.build();
console.log(result.data);
await report.close();
```

## Core Concepts

### Three-Layer Architecture

1. **Fluent API** - User-facing builder with chained methods
2. **Query Plan IR** - Declarative intermediate representation
3. **SQL Generator** - Emits DuckDB SQL (CTE or temp table mode)

### Data Flow

```
Providers load data → SQL generators create transforms → Final SELECT → Result
```

## API Overview

### ReportWithContext

The main entry point for building reports:

```typescript
const report = new ReportWithContext()
    // Set execution context
    .context({ from, until, timezone, ...params })
    
    // Load data sources
    .load('alias', provider, filters?)
    
    // Apply transformations
    .pivot('source', config)           // Convert rows to columns
    .locf('source', config)            // Fill gaps with last observation
    .join('left', 'right', conditions) // Join sources
    .coarsen('source', config)         // Aggregate time granularity
    .applyEnrichment('source', config) // Apply context formulas
    .timezone('source', config)        // Convert timezone
    .window('source', config)          // Window functions
    .filter('condition')               // Filter rows
    
    // Define output
    .select(['col1', 'col2'])
    .groupBy(['col1'])
    .orderBy('col1', 'ASC')
    .format(config)                    // Format output
    
    // Execute
    .build(options?);
```

### Providers

Built-in providers for loading data:

```typescript
import { InMemoryProvider, TimelineProvider } from 'duckdb-report-builder';

// Load from JavaScript arrays
const memoryProvider = new InMemoryProvider(data, 'my_table');

// Generate a complete timeline for LOCF gap filling
const timelineProvider = new TimelineProvider('hour', true);
```

### Execution Options

Control how the report is executed:

```typescript
const result = await report.build({
    strategy: 'temp_tables',  // or 'cte' (default)
    onStep: (info, conn) => {
        console.log(`Step ${info.stepNumber}: ${info.name}`);
    },
    injectSQL: async (info, conn) => {
        // Inject custom SQL between transforms
    }
});
```

## Examples

### Pivot Transform

Convert EAV (Entity-Attribute-Value) format to wide format:

```typescript
const report = new ReportWithContext()
    .context({ from, until, timezone: 'UTC' })
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
    .select(['timestamp', 'temperature', 'pressure']);
```

### LOCF (Last Observation Carried Forward)

Fill gaps in sparse data:

```typescript
const report = new ReportWithContext()
    .context({ from, until, timezone: 'UTC', deviceIds: [101] })
    .load('timeline', new TimelineProvider('hour', true))
    .load('readings', new InMemoryProvider(sparseData))
    .locf('readings', {
        baseTimeline: 'timeline',
        joinKeys: ['device_id'],
        columns: ['temperature'],
        maxLookbackSeconds: 7200,
    })
    .select(['timeline.timestamp', 'temperature']);
```

### Coarsening Time Granularity

Aggregate from fine to coarse granularity:

```typescript
const report = new ReportWithContext()
    .context({ from, until, timezone: 'UTC' })
    .load('battery', new InMemoryProvider(minuteData))
    .coarsen('battery', {
        from: 'minute',
        to: 'hour',
        strategy: {
            voltage: 'avg',
            current: 'avg',
        },
    })
    .select(['timestamp', 'voltage', 'current']);
```

### Window Functions

Calculate deltas and apply analytics:

```typescript
const report = new ReportWithContext()
    .context({ from, until, timezone: 'UTC' })
    .load('readings', new InMemoryProvider(dailyData))
    .window('readings', {
        partitionBy: ['device_id'],
        orderBy: [{ column: 'date', direction: 'ASC' }],
        windowFunctions: [{
            function: 'LAG',
            column: 'value',
            offset: 1,
            defaultValue: 0,
            outputAlias: 'prev_value',
        }],
    })
    .select(['device_id', 'date', 'value', 'prev_value']);
```

### Complex Pipeline

Combine multiple transformations:

```typescript
const report = new ReportWithContext()
    .context({ from, until, timezone: 'UTC', deviceIds: [101] })
    .load('contexts', new InMemoryProvider(deviceContexts))
    .load('consumption', new InMemoryProvider(rawConsumption))
    .load('battery', new InMemoryProvider(batteryMinute))
    .applyEnrichment('consumption', {
        lookupSource: 'contexts',
        joinOn: ['device_id', 'channel'],
        formulas: {
            consumption_kwh: { formula: 'r.daily_raw * c.multiplier' },
        },
    })
    .coarsen('battery', {
        from: 'minute',
        to: 'hour',
        strategy: { voltage: 'avg', current: 'avg' },
    })
    .join('consumption_enriched', 'battery_coarsened', {
        timestamp: 'timestamp',
        device_id: 'device_id',
    })
    .select(['timestamp', 'consumption_kwh', 'voltage', 'current']);
```

## Documentation

- [API Reference](./docs/api.md) - Complete API documentation
- [Examples](./docs/examples.md) - Detailed usage examples
- [Architecture](./docs/architecture.md) - System design and concepts

## Type Safety

The library is fully typed. Use generics for result typing:

```typescript
interface ReportRow {
    timestamp: Date;
    device_id: bigint;
    value: number;
}

const result = await report.build<ReportRow>();
// result.data is ReportRow[]
```

## License

MIT
