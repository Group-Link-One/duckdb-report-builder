import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';

/**
 * DuckDB Query Executor
 *
 * Manages DuckDB instance and connection lifecycle.
 * Uses the new async API from @duckdb/node-api v1.4.3+
 */
export class DuckDBQueryExecutor {
    private instance: DuckDBInstance | null = null;
    private connection: DuckDBConnection | null = null;

    /**
     * Initialize DuckDB instance and connection
     * Must be called before using any other methods
     */
    async init(path: string = ':memory:', options?: Record<string, string>): Promise<void> {
        if (this.instance) {
            console.warn('DuckDB instance already initialized');
            return;
        }

        this.instance = await DuckDBInstance.create(path, options);
        this.connection = await this.instance.connect();
    }

    /**
     * Get the active connection
     * @throws Error if not initialized
     */
    getConnection(): DuckDBConnection {
        if (!this.connection) {
            throw new Error('DuckDB connection not initialized. Call init() first.');
        }
        return this.connection;
    }

    /**
     * Execute a SQL query and return results as JSON array
     */
    async runQuery(query: string): Promise<any[]> {
        const connection = this.getConnection();
        const result = await connection.run(query);

        // Get column names from the result
        const columnNames = Array.from({ length: result.columnCount }, (_, i) => result.columnName(i));

        // Iterate through all chunks and collect rows
        const allRows: any[] = [];
        for (let chunkIndex = 0; chunkIndex < result.chunkCount; chunkIndex++) {
            const chunk = result.getChunk(chunkIndex);
            const rows = chunk.getRowObjects(columnNames);
            allRows.push(...rows);
        }

        return allRows;
    }

    /**
     * Close connection and instance
     */
    async close(): Promise<void> {
        if (this.instance) {
            this.instance.closeSync();
            this.instance = null;
            this.connection = null;
        }
    }

    /**
     * Check if initialized
     */
    isInitialized(): boolean {
        return this.instance !== null && this.connection !== null;
    }
}
