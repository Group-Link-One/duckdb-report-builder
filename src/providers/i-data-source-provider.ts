/**
 * Data Source Provider Interface
 *
 * Providers abstract different data sources (ClickHouse, Device Context, Battery API, etc.)
 * Each provider knows how to:
 * 1. Load its data into a DuckDB temp table
 * 2. Declare its schema
 * 3. Validate column references
 */

import { DuckDBConnection } from '@duckdb/node-api';

/**
 * Column type definitions
 */
export type ColumnType = 'BIGINT' | 'INTEGER' | 'DOUBLE' | 'VARCHAR' | 'TIMESTAMP' | 'DATE' | 'BOOLEAN' | 'INTEGER[]';

/**
 * Column schema definition
 */
export interface ColumnSchema {
    name: string;
    type: ColumnType;
    nullable?: boolean;
    description?: string;
}

/**
 * Load context - provides all necessary information for a provider to load data
 */
export interface LoadContext {
    /** DuckDB connection for data loading */
    connection: DuckDBConnection;
    /** Time period for the query */
    period: {
        from: Date;
        until: Date;
    };
    /** Timezone for temporal operations */
    timezone: string;
    /** Arbitrary parameters passed via .context() */
    params: Record<string, any>;
    /** Map of previously loaded source aliases to their DuckDB table names */
    tables: Map<string, string>;
}

/**
 * Data Source Provider Interface
 *
 * Each provider implements this interface to provide data from a specific source
 */
export interface IDataSourceProvider {
    /**
     * Unique name for this provider type
     */
    readonly name: string;

    /**
     * Load data into DuckDB and return the table name
     *
     * @param context - Load context containing connection, period, timezone, params, and previously loaded tables
     * @returns Promise resolving to the table name where data was loaded
     */
    load(context: LoadContext): Promise<string>;

    /**
     * Get the schema of columns provided by this data source
     *
     * @returns Array of column schemas
     */
    getSchema(): ColumnSchema[];

    /**
     * Validate that the requested columns exist in this provider's schema
     *
     * @param columns - Column names to validate
     * @throws Error if any column is invalid
     */
    validateColumns(columns: string[]): void;

    /**
     * Check if a column exists in this provider's schema
     *
     * @param columnName - Column name to check
     * @returns true if column exists, false otherwise
     */
    hasColumn(columnName: string): boolean;

    /**
     * Get the schema for a specific column
     *
     * @param columnName - Column name
     * @returns Column schema or undefined if not found
     */
    getColumnSchema(columnName: string): ColumnSchema | undefined;
}

/**
 * Abstract base class for providers with common functionality
 */
export abstract class BaseDataSourceProvider implements IDataSourceProvider {
    abstract readonly name: string;
    protected schema: ColumnSchema[] = [];

    abstract load(context: LoadContext): Promise<string>;

    getSchema(): ColumnSchema[] {
        return this.schema;
    }

    validateColumns(columns: string[]): void {
        const schemaMap = new Map(this.schema.map((col) => [col.name, col]));

        for (const columnName of columns) {
            if (!schemaMap.has(columnName)) {
                throw new Error(
                    `Column '${columnName}' does not exist in provider '${this.name}'. ` +
                        `Available columns: ${this.schema.map((c) => c.name).join(', ')}`
                );
            }
        }
    }

    hasColumn(columnName: string): boolean {
        return this.schema.some((col) => col.name === columnName);
    }

    getColumnSchema(columnName: string): ColumnSchema | undefined {
        return this.schema.find((col) => col.name === columnName);
    }

    /**
     * Helper method to generate a unique table name
     */
    protected generateTableName(baseName: string): string {
        const timestamp = Date.now();
        const random = Math.floor(Math.random() * 10000);
        return `${baseName}_${timestamp}_${random}`;
    }
}
