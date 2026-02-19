/**
 * duckdb-report-builder
 *
 * A SQL-first report builder using DuckDB for data transformation and orchestration.
 */

// Core API
export { DuckDBQueryExecutor } from './core/duckdb-query-executor';
export {
    composeCallbacks, createNullValidationCallback, createProgressCallback, createRowCountCallback,
    createSampleDataCallback, type CallbackPosition, type ExecutionOptions,
    type ExecutionStrategy,
    type StepInfo
} from './core/execution-strategy';
export { ReportWithContext, type ReportResult } from './core/report-with-context';
// Fluent API Configuration Types
export type {
    ApplyEnrichmentConfig, CoarsenConfig, LocfConfig, PivotConfig, TimezoneConfig,
    WindowConfig
} from './core/report-with-context';
// Providers
export {
    BaseDataSourceProvider, type ColumnSchema,
    type ColumnType, type IDataSourceProvider,
    type LoadContext
} from './providers/i-data-source-provider';
export { InMemoryProvider, inMemoryProvider } from './providers/in-memory-provider';
export { TimelineProvider } from './providers/timeline-provider';
// Query Plan IR
export {
    isApplyEnrichmentTransform, isCoarsenTransform, isFilterTransform, isJoinTransform, isLocfTransform, isPivotTransform, isTimezoneTransform,
    isWindowTransform, validateQueryPlan, type AggregationStrategy, type ApplyEnrichmentTransform, type CoarsenTransform, type EnrichmentFormula, type FilterSpec, type FilterTransform, type JoinTransform, type LocfTransform, type OutputColumn, type OutputSpec, type PivotTransform, type PlanContext, type QueryPlan, type SourceSpec, type TimeGranularity, type TimezoneTransform, type TransformSpec, type WindowFunctionSpec, type WindowTransform
} from './query-plan/query-plan';
// Sinks
export { FileSink } from './sinks/file-sink';
export { StreamSink } from './sinks/stream-sink';
export { buildCTE, quoteIdentifier } from './sql-generator/cte-builder';
// Format Generator
export { FormatGenerator, type FormatConfig } from './sql-generator/format-generator';
// SQL Generator (exposed for advanced use cases)
export { SQLGenerator, type SQLGeneratorOptions } from './sql-generator/sql-generator';






