// src/sinks/stream-sink.ts
import { DuckDBQueryExecutor } from '../core/duckdb-query-executor';

export class StreamSink {
    constructor(private duckdb: DuckDBQueryExecutor) {}

    async streamToMemory(query: string): Promise<any[]> {
        return this.duckdb.runQuery(query);
    }
}
