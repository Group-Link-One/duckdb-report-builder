# API Reference

## Table of Contents

- [Core Classes](#core-classes)
  - [ReportWithContext](#reportwithcontext)
  - [ReportResult](#reportresult)
- [Providers](#providers)
  - [InMemoryProvider](#inmemoryprovider)
  - [TimelineProvider](#timelineprovider)
  - [BaseDataSourceProvider](#basedatasourceprovider)
- [Sinks](#sinks)
  - [FileSink](#filesink)
  - [StreamSink](#streamsink)
- [Execution Strategies](#execution-strategies)
- [Query Plan IR](#query-plan-ir)

---

## Core Classes

### ReportWithContext

The main entry point for building reports. Uses a fluent API pattern for chaining transformations.

```typescript
class ReportWithContext {
    constructor();
    
    // Context setup
    context(contextParams: { from: Date; until: Date; timezone?: string; [key: string]: any }): this;
    
    // Data loading
    load(alias: string, provider: IDataSourceProvider, filters?: FilterSpec[]): this;
    
    // Transformations
    pivot(sourceAlias: string, config: PivotConfig): this;
    locf(sourceAlias: string, config: LocfConfig): this;
    join(leftAlias: string, rightAlias: string, onConditions: Record<string, string>, joinType?: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'): this;
    coarsen(sourceAlias: string, config: CoarsenConfig): this;
    applyEnrichment(sourceAlias: string, config: ApplyEnrichmentConfig): this;
    timezone(sourceAlias: string, config: TimezoneConfig): this;
    window(sourceAlias: string, config: WindowConfig): this;
    filter(condition: string): this;
    
    // Output specification
    select(columns: Array<string | [string, string]>): this;
    groupBy(columns: string[]): this;
    orderBy(column: string, direction?: 'ASC' | 'DESC'): this;
    format(config: FormatConfig): this;
    
    // Execution
    build<T = any>(options?: ExecutionOptions): Promise<ReportResult<T>>;
    toSQL(): Promise<string>;
    close(): Promise<void>;
}
```

#### Configuration Types

##### PivotConfig

```typescript
interface PivotConfig {
    on: string;           // Column to pivot on (e.g., 'serie_id')
    val: string;          // Column with values (e.g., 'raw_value')
    cols: Array<{
        id: number | string;    // Value to match (e.g., serie_id=2)
        alias: string;          // Output column (e.g., 'energy_in')
        locf?: number | null;   // Optional LOCF lookback seconds
    }>;
    groupBy?: string[];   // Columns to group by
    as?: string;          // Optional output table rename
}
```

##### LocfConfig

```typescript
interface LocfConfig {
    baseTimeline: string;        // Timeline source alias
    joinKeys: string[];          // Join keys (e.g., ['device_id', 'channel'])
    columns: string[];           // Columns to carry forward
    maxLookbackSeconds?: number | null;
}
```

##### CoarsenConfig

```typescript
interface CoarsenConfig {
    from: TimeGranularity;       // Original granularity ('second' | 'minute' | 'hour' | 'day' | 'week' | 'month')
    to: TimeGranularity;         // Target granularity
    strategy: Record<string, AggregationStrategy>;  // Column -> aggregation strategy
    timestampColumn?: string;    // Default: 'timestamp'
    groupBy?: string[];          // Additional columns to group by
    as?: string;                 // Optional output table rename
}

// AggregationStrategy: 'sum' | 'avg' | 'min' | 'max' | 'first' | 'last' | 'count'
```

##### ApplyEnrichmentConfig

```typescript
interface ApplyEnrichmentConfig {
    lookupSource: string;        // Lookup/context source alias
    joinOn: string[];            // Keys to join on (e.g., ['device_id', 'channel'])
    formulas: Record<string, EnrichmentFormula>;  // Column name -> enrichment formula
    as?: string;                 // Optional output table rename
}

interface EnrichmentFormula {
    formula: string;  // SQL formula using 'r' (row) and 'c' (context) prefixes
}
```

##### TimezoneConfig

```typescript
interface TimezoneConfig {
    timestampColumns: string[];  // Columns to convert (e.g., ['timestamp', 'event_time'])
    timezone: string;            // Target timezone (e.g., 'America/Sao_Paulo')
    as?: string;                 // Optional output table rename
}
```

##### WindowConfig

```typescript
interface WindowConfig {
    partitionBy: string[];       // PARTITION BY columns
    orderBy: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
    windowFunctions: WindowFunctionSpec[];
    qualify?: string;            // QUALIFY condition (e.g., 'rn = 1')
    as?: string;                 // Optional output table rename
}

interface WindowFunctionSpec {
    function: 'LAG' | 'LEAD' | 'ROW_NUMBER' | 'RANK' | 'FIRST_VALUE' | 'LAST_VALUE' | 'ARRAY_AGG';
    column?: string;             // Column to operate on
    offset?: number;             // Offset for LAG/LEAD (default: 1)
    defaultValue?: any;          // Default value for LAG/LEAD
    outputAlias: string;         // Output column name
    orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
}
```

##### FormatConfig

```typescript
interface FormatConfig {
    locale: string;  // Locale for formatting (e.g., 'pt-BR', 'en-US')
    columns: Record<string, {
        decimalPlaces?: number;   // Number of decimal places
        unit?: string;            // Unit suffix (e.g., 'm³')
        currency?: string;        // Currency prefix (e.g., 'R$')
        dateFormat?: string;      // Date format string (DuckDB format)
    }>;
}
```

### ReportResult

```typescript
interface ReportResult<T = any> {
    data: T[];              // Query results as array of typed objects
    sql: string;            // Generated SQL for debugging
    executionTimeMs: number; // Total execution time
}
```

---

## Providers

### InMemoryProvider

Provides data from in-memory JavaScript arrays. Useful for testing and loading pre-fetched data.

```typescript
class InMemoryProvider extends BaseDataSourceProvider {
    constructor(
        data: Record<string, any>[],
        tableName?: string,
        schema?: ColumnSchema[]
    );
}

// Convenience function
function inMemoryProvider(
    data: Record<string, any>[],
    tableName?: string,
    schema?: ColumnSchema[]
): InMemoryProvider;
```

**Example:**

```typescript
const data = [
    { device_id: 101, timestamp: new Date('2024-01-01'), value: 100 },
    { device_id: 101, timestamp: new Date('2024-01-02'), value: 150 },
];

const provider = new InMemoryProvider(data, 'readings');
```

### TimelineProvider

Generates complete timelines at specified granularity for LOCF gap filling.

```typescript
class TimelineProvider extends BaseDataSourceProvider {
    constructor(
        granularity: 'minute' | 'hour' | 'day' = 'hour',
        includeEntityCrossJoin: boolean = false,
        entityIdColumn: string = 'device_id',
        channelColumn: string = 'channel'
    );
    
    getGranularity(): TimelineGranularity;
    includesEntityCrossJoin(): boolean;
}
```

**Example:**

```typescript
// Simple hourly timeline
const hourlyTimeline = new TimelineProvider('hour');

// Timeline with device cross-join for gap filling
const deviceTimeline = new TimelineProvider('hour', true, 'device_id', 'channel');
```

### BaseDataSourceProvider

Abstract base class for creating custom providers.

```typescript
abstract class BaseDataSourceProvider implements IDataSourceProvider {
    abstract readonly name: string;
    abstract load(context: LoadContext): Promise<string>;
    
    getSchema(): ColumnSchema[];
    validateColumns(columns: string[]): void;
    hasColumn(columnName: string): boolean;
    getColumnSchema(columnName: string): ColumnSchema | undefined;
}

interface LoadContext {
    connection: DuckDBConnection;
    period: { from: Date; until: Date };
    timezone: string;
    params: Record<string, any>;
    tables: Map<string, string>;
}

interface ColumnSchema {
    name: string;
    type: 'BIGINT' | 'INTEGER' | 'DOUBLE' | 'VARCHAR' | 'TIMESTAMP' | 'DATE' | 'BOOLEAN' | 'INTEGER[]';
    nullable?: boolean;
    description?: string;
}
```

---

## Sinks

### FileSink

Exports query results to files using DuckDB's COPY TO command.

```typescript
class FileSink {
    constructor(duckdb: DuckDBQueryExecutor);
    
    async copyToFile(
        query: string,
        filePath: string,
        format?: string,           // Default: 'csv'
        options?: CopyToFileOptions
    ): Promise<void>;
}

interface CopyToFileOptions {
    header?: boolean;      // Include header row
    delimiter?: string;    // CSV delimiter (',' or ';')
}
```

**Example:**

```typescript
const executor = new DuckDBQueryExecutor();
await executor.init();

const fileSink = new FileSink(executor);
await fileSink.copyToFile('SELECT * FROM readings', 'output.csv', 'csv', {
    header: true,
    delimiter: ';'
});
```

### StreamSink

Streams query results to memory (simple wrapper around executor).

```typescript
class StreamSink {
    constructor(duckdb: DuckDBQueryExecutor);
    
    async streamToMemory(query: string): Promise<any[]>;
}
```

---

## Execution Strategies

### ExecutionOptions

```typescript
interface ExecutionOptions {
    strategy: 'cte' | 'temp_tables';
    onStep?: (stepInfo: StepInfo, connection: DuckDBConnection) => Promise<void>;
    injectSQL?: (stepInfo: StepInfo, connection: DuckDBConnection) => Promise<string | void>;
    keepTempTables?: boolean;
}

interface StepInfo {
    name: string;
    tableName: string;
    stepNumber: number;
    totalSteps: number;
    position: 'after_source_load' | 'between_transforms' | 'before_output';
    sql?: string;
}
```

### Callback Helpers

```typescript
// Compose multiple callbacks
function composeCallbacks(
    ...callbacks: Array<(info: StepInfo, conn: DuckDBConnection) => Promise<void>>
): (info: StepInfo, conn: DuckDBConnection) => Promise<void>;

// Progress tracking
function createProgressCallback(reportName: string): (info: StepInfo, conn: DuckDBConnection) => Promise<void>;

// Null validation
function createNullValidationCallback(
    validations: Record<string, string[]>
): (info: StepInfo, conn: DuckDBConnection) => Promise<void>;

// Row count logging
function createRowCountCallback(): (info: StepInfo, conn: DuckDBConnection) => Promise<void>;

// Sample data logging
function createSampleDataCallback(limit?: number): (info: StepInfo, conn: DuckDBConnection) => Promise<void>;
```

**Example:**

```typescript
const result = await report.build({
    strategy: 'temp_tables',
    onStep: composeCallbacks(
        createProgressCallback('EnergyReport'),
        createRowCountCallback()
    ),
    injectSQL: async (info, conn) => {
        if (info.name === 'transform:applyEnrichment') {
            const newTable = `${info.tableName}_with_cost`;
            await conn.run(`
                CREATE TEMP TABLE ${newTable} AS
                SELECT *, adjusted_consumption * 5.5 AS cost
                FROM ${info.tableName}
            `);
            return newTable;
        }
    }
});
```

---

## Query Plan IR

The Query Plan IR (Intermediate Representation) is a declarative data structure that represents a complete report specification.

```typescript
interface QueryPlan {
    context: PlanContext;
    sources: SourceSpec[];
    transforms: TransformSpec[];
    output: OutputSpec;
}

interface PlanContext {
    period: { from: Date; until: Date };
    timezone: string;
    params: Record<string, any>;
}

interface SourceSpec {
    alias: string;
    provider: IDataSourceProvider;
    filters?: FilterSpec[];
}

interface OutputSpec {
    columns: OutputColumn[];
    filters?: FilterSpec[];
    orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
    groupBy?: string[];
}
```

### Type Guards

```typescript
function isPivotTransform(transform: TransformSpec): transform is PivotTransform;
function isLocfTransform(transform: TransformSpec): transform is LocfTransform;
function isJoinTransform(transform: TransformSpec): transform is JoinTransform;
function isFilterTransform(transform: TransformSpec): transform is FilterTransform;
function isCoarsenTransform(transform: TransformSpec): transform is CoarsenTransform;
function isApplyEnrichmentTransform(transform: TransformSpec): transform is ApplyEnrichmentTransform;
function isTimezoneTransform(transform: TransformSpec): transform is TimezoneTransform;
function isWindowTransform(transform: TransformSpec): transform is WindowTransform;
```
