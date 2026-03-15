/**
 * In-Memory Data Source Provider
 *
 * Provides data from in-memory JavaScript objects/arrays.
 * Useful for:
 * - Testing with mock data
 * - Loading pre-fetched data (e.g., device contexts from DeviceContextService)
 * - Small datasets that don't require external sources
 */

import { BaseDataSourceProvider, ColumnSchema, ColumnType, LoadContext } from './i-data-source-provider';

/**
 * In-Memory Provider
 *
 * Loads data from JavaScript arrays into DuckDB
 */
export class InMemoryProvider extends BaseDataSourceProvider {
    readonly name = 'InMemory';

    constructor(
        private data: Record<string, any>[],
        private tableName?: string,
        schema?: ColumnSchema[]
    ) {
        super();

        if (schema) {
            this.schema = schema;
        } else if (data.length > 0) {
            this.schema = this.inferSchema(data[0]);
        } else {
            this.schema = [];
        }
    }

    /**
     * Infer schema from a sample object
     */
    private inferSchema(sample: Record<string, any>): ColumnSchema[] {
        const schema: ColumnSchema[] = [];

        for (const [key, value] of Object.entries(sample)) {
            const type = this.inferType(value);
            schema.push({
                name: key,
                type,
                nullable: value === null || value === undefined,
            });
        }

        return schema;
    }

    /**
     * Infer DuckDB type from JavaScript value
     */
    private inferType(value: any): ColumnType {
        if (value === null || value === undefined) {
            return 'VARCHAR'; // Default to VARCHAR for null
        }

        const jsType = typeof value;

        switch (jsType) {
            case 'number':
                // Check if it's an integer or float
                return Number.isInteger(value) ? 'INTEGER' : 'DOUBLE';
            case 'boolean':
                return 'BOOLEAN';
            case 'bigint':
                return 'BIGINT';
            case 'string':
                return 'VARCHAR';
            case 'object':
                if (value instanceof Date) {
                    return 'TIMESTAMP';
                }
                if (Array.isArray(value)) {
                    return 'INTEGER[]'; // Assume integer array
                }
                return 'VARCHAR'; // Fallback to VARCHAR for objects
            default:
                return 'VARCHAR';
        }
    }

    /**
     * Format value for SQL INSERT
     */
    private formatValue(value: any, columnType: ColumnType): string {
        if (value === null || value === undefined) {
            return 'NULL';
        }

        switch (columnType) {
            case 'VARCHAR':
                const escaped = String(value).replace(/'/g, "''");
                return `'${escaped}'`;
            case 'TIMESTAMP':
                if (value instanceof Date) {
                    return `'${value.toISOString()}'`;
                }
                return `'${value}'`;
            case 'BIGINT':
                return String(Number(value));
            case 'INTEGER':
            case 'DOUBLE':
                return String(value);
            case 'BOOLEAN':
                return value ? 'TRUE' : 'FALSE';
            case 'INTEGER[]':
                if (Array.isArray(value)) {
                    return `ARRAY[${value.join(', ')}]`;
                }
                return 'NULL';
            default:
                return `'${value}'`;
        }
    }

    /**
     * Load in-memory data into DuckDB
     */
    async load(context: LoadContext): Promise<string> {
        const tableName = this.tableName || this.generateTableName('in_memory');
        const connection = context.connection;

        const columnDefs = this.schema
            .map((col) => `${col.name} ${col.type}${col.nullable ? '' : ' NOT NULL'}`)
            .join(',\n    ');

        await connection.run(`
            CREATE OR REPLACE TEMPORARY TABLE ${tableName} (
                ${columnDefs}
            );
        `);

        if (this.data.length === 0) {
            return tableName;
        }

        const values = this.data
            .map((row) => {
                const rowValues = this.schema.map((col) => {
                    const value = row[col.name];
                    return this.formatValue(value, col.type);
                });
                return `(${rowValues.join(', ')})`;
            })
            .join(',\n');

        const columnNames = this.schema.map((col) => col.name).join(', ');

        await connection.run(`
            INSERT INTO ${tableName} (${columnNames})
            VALUES ${values};
        `);

        return tableName;
    }
}

export function inMemoryProvider(
    data: Record<string, any>[],
    tableName?: string,
    schema?: ColumnSchema[]
): InMemoryProvider {
    return new InMemoryProvider(data, tableName, schema);
}
