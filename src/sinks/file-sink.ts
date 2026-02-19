// src/sinks/file-sink.ts
import { DuckDBConnection } from '@duckdb/node-api';
import { DuckDBQueryExecutor } from '../core/duckdb-query-executor';

export interface CopyToFileOptions {
    /** Include header row in CSV output */
    header?: boolean;

    /**
     * CSV delimiter character
     * Default: ',' (comma)
     * Brazilian locale should use ';' (semicolon) because ',' is the decimal separator
     */
    delimiter?: string;
}

export class FileSink {
    constructor(private duckdb: DuckDBQueryExecutor) {}

    /**
     * Copy query results to a file using DuckDB's COPY TO command
     *
     * @param query SQL query to execute
     * @param filePath Output file path
     * @param format Output format (default: 'csv')
     * @param options Copy options (header, delimiter)
     *
     * @example
     * ```typescript
     * // Comma-separated values (common in US/UK locales)
     * await fileSink.copyToFile(query, 'output.csv', 'csv', {
     *   header: true,
     *   delimiter: ','
     * });
     *
     * // Semicolon-separated values (common in locales where comma is decimal separator)
     * await fileSink.copyToFile(query, 'output.csv', 'csv', {
     *   header: true,
     *   delimiter: ';'
     * });
     * ```
     */
    async copyToFile(
        query: string,
        filePath: string,
        format: string = 'csv',
        options?: CopyToFileOptions
    ): Promise<void> {
        const connection: DuckDBConnection = this.duckdb.getConnection();
        const formatOption = format.toUpperCase();

        // Build COPY options
        const copyOptions: string[] = [`FORMAT ${formatOption}`];

        if (options?.header) {
            copyOptions.push('HEADER');
        }

        if (options?.delimiter) {
            copyOptions.push(`DELIMITER '${options.delimiter}'`);
        }

        const optionsString = copyOptions.join(', ');

        await connection.runAndRead(`
            COPY (${query}) TO '${filePath}' (${optionsString});
        `);
        console.log(`Copied query results to ${filePath}`);
    }
}
