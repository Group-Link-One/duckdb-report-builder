# Changelog

## [0.3.0] - 2026-04-07

### Added
- **Last-mile formatting system**: Complete redesign of the format layer. Formatting now
  **replaces** columns (instead of appending `_formatted` suffixes) and works across all
  execution paths: `build()` (CTE and temp_tables strategies) and `buildToFile()` (CSV, JSON, Parquet).
- **Auto-detection by column type**: When `.format()` is configured, the system queries
  `information_schema.columns` to detect each column's DuckDB type and applies locale-aware
  formatting automatically:
  - `DATE` → `strftime(col, dateFormat)`
  - `TIMESTAMP` → `strftime(col, dateTimeFormat)`
  - `TIMESTAMP WITH TIME ZONE` → `strftime(col, dateTimeTZFormat)` (includes UTC offset)
  - `DOUBLE`, `FLOAT`, `DECIMAL(...)` → `format(spec, col)` (only when `decimalPlaces` is set)
  - Integer types, `VARCHAR`, others → pass-through (not auto-formatted)
- **Column renames**: `ColumnFormatConfig.rename` renames columns in output via `AS "New Name"`.
  Works for all output formats including Parquet.
- **`raw` flag**: `ColumnFormatConfig.raw` skips auto-formatting for columns that happen to be
  numeric but should stay raw (e.g., IDs). Renames still apply.
- **Locale shorthand**: `.format('pt-BR')` as shorthand for `.format({ locale: 'pt-BR' })`.
- **Global format defaults**: `FormatConfig` now supports `dateFormat`, `dateTimeFormat`,
  `dateTimeTZFormat`, and `decimalPlaces` at the top level, applied to all matching columns
  unless overridden per-column.
- **Timezone-aware date formatting**: New `dateTimeTZFormat` field for `TIMESTAMP WITH TIME ZONE`
  columns. Locale defaults include `%z` for UTC offset (e.g., `+03`, `-05`). Supports `%Z`
  for timezone abbreviation.
- **Parquet-safe formatting**: `buildToFile()` with `format: 'parquet'` applies renames only
  (no text casting), preserving native DuckDB types.
- **`generateFormatSQL()`**: New function that generates a formatting SELECT with column
  replacement, type auto-detection, and renames. Replaces `generateFormatCTE()`.
- **`generateRenameSQL()`**: New function for rename-only SELECTs (used for Parquet exports).
- **`LOCALE_DEFAULTS`**: Exported constant with locale-specific defaults for dates, timestamps,
  and number formatting.
- New exported types: `ColumnFormatConfig`, `FormatColumnSchema`, `Locale`.
- New exported function: `getLocaleCSVDelimiter()`.

### Changed
- `FormatConfig.columns` is now **optional** (was required). When omitted, all formatting
  is driven by auto-detection + global defaults.
- `.format()` method now accepts `FormatConfig | Locale` (string shorthand).
- `buildToFile()` now applies formatting when `.format()` is configured (previously ignored).

### Deprecated
- `generateFormatCTE()` — use `generateFormatSQL()` instead (replaces columns, supports
  schema auto-detection and renames).
- `formatValue()` — use `generateFormatSQL()` for full-query formatting.

## [0.2.3] - 2026-03-25

### Added
- **`buildToFile()` method**: Stream results directly to CSV, Parquet, or JSON via DuckDB `COPY TO`
  — no rows materialized in Node.js memory. Ideal for large exports. Supports `format`, `delimiter`,
  `header`, and `sorted` options (`sorted: false` skips ORDER BY for ~5x speedup on large datasets).
  Includes DuckDB progress tracking via logger events.
- **PRAGMA JSON profiling**: Replaced `EXPLAIN ANALYZE` with zero-overhead `PRAGMA profiling` that
  collects per-operator timing during normal query execution. New `ProfileResult.raw` field exposes
  the full DuckDB operator tree with cumulative metrics.
- New exported types: `PragmaProfileNode`, `PragmaProfileOutput`.
- New exported helpers: `enablePragmaProfiling()`, `disablePragmaProfiling()`, `readPragmaProfile()`,
  `formatPragmaProfile()`.

## [0.2.2] - 2026-03-25

### Added
- **DuckDB configuration**: New `.duckdb({ path, settings })` fluent method to configure the
  DuckDB instance before execution. Supports `path` (file-based DB for disk spill), `threads`,
  `memory_limit`, `temp_directory`, and any other DuckDB setting. Defaults to in-memory.

## [0.2.1] - 2026-03-25

### Added
- **Pluggable logger system**: New `ReportLogger` interface with lifecycle events (`onInit`,
  `onSourceLoad`, `onBuildComplete`, `onProviderEvent`). Ships with `silentLogger()` (default)
  and `consoleLogger()`. Integrate with any metrics/tracing/structured-logging backend by
  implementing only the events you care about.
- **Execution profiling**: Opt-in `profiling: true` in `ExecutionOptions` collects DuckDB memory
  snapshots (`duckdb_memory()`), per-step timing, and row counts during report execution.
  - **CTE mode**: captures memory before/after + `EXPLAIN ANALYZE` output for per-operator stats.
  - **temp_tables mode**: captures per-step memory deltas, duration, and row counts between each
    `CREATE TEMP TABLE`.
  - Results emitted via `onProfileComplete` logger event and returned in `ReportResult.profile`.
  - New `createProfilingCallback()` factory for manual profiling in temp_tables mode.
  - New types: `ProfileResult`, `StepProfile`, `CTEQueryProfile`, `MemorySnapshot`,
    `ProfileCompleteEvent`.
  - New helpers: `queryMemorySnapshot()`, `sumMemoryBytes()`.

### Fixed
- **Timezone conversion direction**: SQL generation now applies a two-step conversion
  (`AT TIME ZONE 'UTC' AT TIME ZONE '<target>'`) instead of a single step, fixing incorrect
  offset direction in DuckDB. Includes `EXCLUDE` to avoid duplicate columns in the SELECT.

## [0.2.0] - 2026-03-15

### Added
- **In-place LOCF mode**: `LocfTransform.baseTimelineAlias` is now optional. Omit it to run
  LOCF directly on existing rows without materializing a timeline table. Faster when your
  data already has all timestamps and only columns are sparse.
- **LOCF `as` field**: `LocfTransform` and `LocfConfig` now support `as?: string` for custom
  output CTE naming, matching all other transform types.
- **LOCF unit tests**: New 15-test spec covering both in-place and timeline-join modes,
  `maxLookbackSeconds`, multiple join keys/columns, and custom CTE naming.
- Re-exported `GeneratedSQL`, `GeneratedTempTablePlan`, `TempTableStep` types from package index.

### Changed
- `FormatGenerator` static class replaced by standalone `generateFormatCTE()` and `formatValue()`
  functions. Same signatures, matches the plain-function convention of all other sql-generator modules.
- `ExecutionOptions.strategy` is now optional (defaults to `'cte'`), aligning the type with
  the runtime behavior.
- `WindowFunctionSpec.defaultValue` narrowed from `any` to `string | number | null`.
- `PivotTransform.as` now correctly honored in `validateQueryPlan` and `getTransformOutputAlias`
  (previously ignored, unlike all other transforms).
- CTE suffix resolution in `SQLGenerator` now recognizes all transform suffixes (`_enriched`,
  `_tz`, `_windowed`) — previously missing, causing resolution failures in some pipelines.
- `InMemoryProvider.load()` deduplicates CREATE TABLE logic (was copy-pasted in both
  empty-data and non-empty-data branches).
- Extracted `preparePlan()` in `ReportWithContext` to DRY up plan construction shared by
  `build()` and `toSQL()`.
- Removed narrating comments that restated self-evident code across multiple files.

### Removed
- **`StreamSink`** class and its export. Was a single-line delegation to `runQuery()` with no
  added behavior. Call `DuckDBQueryExecutor.runQuery()` directly.
- **`generateApplyEnrichmentSQLWithColumns`** — redundant; `generateApplyEnrichmentRawSQL`
  already accepts `selectColumns?`.
- **`generateTimezoneSQLWithColumns`** — redundant; `generateTimezoneRawSQL` already accepts
  `selectColumns?`.
- **`generateInPlaceLocfSQL`** (old stub) — was never wired into the pipeline. Replaced by
  proper in-place LOCF via optional `baseTimelineAlias`.
- **`generateColumnLocfExpression`** — was a placeholder returning identifiers, not SQL.
- **`ApplyEnrichmentTransform.contextSourceAlias`** — deprecated field removed. Use
  `lookupSourceAlias`.
- **`ApplyEnrichmentTransform.adjustments`** — deprecated field removed. Use `formulas`.
- **`_adjusted` CTE suffix** — no generator ever produced it (enrichment uses `_enriched`).
- All `as any` casts for legacy field fallbacks (4 sites across 3 files).
- `console.log` from `InMemoryProvider.load()` and `FileSink.copyToFile()`.
- `console.warn` from `DuckDBQueryExecutor.init()` and temp table cleanup.
- Dead private method `SQLGenerator.formatSQL` (never called, returned input unchanged).
- Unused variable `schemaMap` in `InMemoryProvider.load()`.
- Verbose `@param/@returns` docblocks on trivial one-liner CTE name accessor functions.
