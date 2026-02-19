# Architecture

## Overview

The `duckdb-report-builder` is designed around a three-layer architecture that separates user-facing APIs from execution details.

```
┌─────────────────────────────────────────────────────────────┐
│                      Fluent API Layer                        │
│                    (ReportWithContext)                       │
├─────────────────────────────────────────────────────────────┤
│                    Query Plan IR Layer                       │
│         (Declarative Intermediate Representation)            │
├─────────────────────────────────────────────────────────────┤
│                    SQL Generator Layer                       │
│           (DuckDB SQL Generation & Execution)                │
└─────────────────────────────────────────────────────────────┘
```

---

## Three-Layer Pipeline

### 1. Fluent API Layer

The user-facing API that provides a chainable builder pattern for constructing reports.

**Key Responsibilities:**
- Provide an intuitive, discoverable API
- Validate inputs at the API level
- Build the Query Plan IR

**Example:**

```typescript
const report = new ReportWithContext()
    .context({ from, until, timezone })
    .load('readings', provider)
    .pivot('readings', config)
    .select(['timestamp', 'value']);
```

### 2. Query Plan IR Layer

A declarative intermediate representation that decouples "what to do" from "how to execute."

**Key Responsibilities:**
- Represent the complete report specification as a data structure
- Enable plan validation before execution
- Allow multiple execution strategies (CTE, temp tables)

**Structure:**

```typescript
interface QueryPlan {
    context: PlanContext;      // Period, timezone, params
    sources: SourceSpec[];     // Data source definitions
    transforms: TransformSpec[]; // Transform pipeline
    output: OutputSpec;        // Column selection, filtering, ordering
}
```

### 3. SQL Generator Layer

Emits DuckDB SQL and handles execution.

**Key Responsibilities:**
- Generate optimized DuckDB SQL from the Query Plan
- Support multiple execution strategies
- Manage DuckDB connections and resources

---

## Data Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Providers  │───▶│   Sources   │───▶│  Temp Tables│
│  Load Data  │    │   Loaded    │    │   Created   │
└─────────────┘    └─────────────┘    └──────┬──────┘
                                             │
┌─────────────┐    ┌─────────────┐    ┌─────▼───────┐
│   Results   │◀───│  Final SQL  │◀───│   CTEs /    │
│   (Rows)    │    │  Generated  │    │  Transforms │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Execution Flow

1. **Context Setup**: User sets period, timezone, and arbitrary parameters via `.context()`
2. **Source Loading**: Each provider loads data into DuckDB temp tables
3. **Transform Chain**: SQL is generated for each transformation step
4. **Output Selection**: Final SELECT statement is generated
5. **Execution**: Query is executed and results returned

---

## Provider Pattern

The provider pattern abstracts different data sources, allowing the same report pipeline to work with various backends.

### Built-in Providers

| Provider | Purpose |
|----------|---------|
| `InMemoryProvider` | Load data from JavaScript arrays |
| `TimelineProvider` | Generate complete timelines for LOCF |

### Creating Custom Providers

```typescript
class ClickHouseProvider extends BaseDataSourceProvider {
    readonly name = 'ClickHouse';
    
    async load(context: LoadContext): Promise<string> {
        // Fetch data from ClickHouse
        const data = await fetchFromClickHouse(context.period);
        
        // Load into DuckDB temp table
        const tableName = this.generateTableName('ch_data');
        await this.loadIntoDuckDB(context.connection, data, tableName);
        
        return tableName;
    }
}
```

---

## Execution Strategies

### CTE Mode (Default)

All transformations are combined into a single SQL query using Common Table Expressions (CTEs).

**Advantages:**
- Single query execution
- DuckDB can optimize across all operations
- No intermediate table cleanup needed

**When to use:**
- Standard reports without debugging needs
- Production workloads

```typescript
const result = await report.build({ strategy: 'cte' });
```

### Temp Table Mode

Each transformation creates a temp table, with optional callbacks between steps.

**Advantages:**
- Step-by-step execution for debugging
- Can inspect intermediate results
- SQL injection capability for custom processing

**When to use:**
- Development and debugging
- Complex pipelines requiring validation between steps

```typescript
const result = await report.build({
    strategy: 'temp_tables',
    onStep: (info, conn) => {
        console.log(`Step ${info.stepNumber}: ${info.name}`);
    }
});
```

---

## SQL Generation

### CTE Generation

Transforms are converted to CTEs in order:

```sql
WITH source_readings AS (...),         -- From provider
     readings_pivoted AS (...),        -- pivot transform
     readings_enriched AS (...),       -- applyEnrichment transform
     readings_coarsened AS (...)       -- coarsen transform
SELECT * FROM readings_coarsened;
```

### Temp Table Generation

Each transform creates a temp table:

```sql
CREATE TEMP TABLE source_readings AS ...;
CREATE TEMP TABLE readings_pivoted AS ...;
CREATE TEMP TABLE readings_enriched AS ...;
CREATE TEMP TABLE readings_coarsened AS ...;
SELECT * FROM readings_coarsened;
```

---

## Type Safety

The library provides comprehensive TypeScript support:

```typescript
// Generic type for result data
interface ReportResult<T = any> {
    data: T[];
    sql: string;
    executionTimeMs: number;
}

// Usage with typed output
interface EnergyReportRow {
    timestamp: Date;
    device_id: bigint;
    consumption_kwh: number;
}

const result = await report.build<EnergyReportRow>();
// result.data is EnergyReportRow[]
```

---

## Performance Considerations

1. **Batch Loading**: Providers should load data in batches when possible
2. **Index Usage**: DuckDB automatically creates indexes on temp tables
3. **Query Optimization**: CTE mode allows DuckDB to optimize the entire pipeline
4. **Memory Management**: Temp tables are cleaned up automatically unless `keepTempTables` is set

---

## Error Handling

Errors can occur at multiple levels:

1. **Validation Errors**: Invalid plan configuration (caught before execution)
2. **Provider Errors**: Data source connectivity or loading issues
3. **SQL Errors**: Syntax or constraint violations during execution
4. **Runtime Errors**: Memory exhaustion or resource limits

All errors include descriptive messages and context to aid debugging.
