/**
 * File Sink Unit Tests
 *
 * Tests for the FileSink.
 */

import { CopyToFileOptions, FileSink } from '../../../src/sinks/file-sink';

describe('FileSink', () => {
    describe('constructor', () => {
        it('should create file sink with executor', () => {
            const mockExecutor = {
                getConnection: jest.fn(),
            };
            const sink = new FileSink(mockExecutor as any);
            expect(sink).toBeDefined();
        });
    });

    describe('copyToFile', () => {
        let mockExecutor: any;
        let mockConnection: any;
        let sink: FileSink;

        beforeEach(() => {
            mockConnection = {
                runAndRead: jest.fn().mockResolvedValue(undefined),
            };
            mockExecutor = {
                getConnection: jest.fn().mockReturnValue(mockConnection),
            };
            sink = new FileSink(mockExecutor);
        });

        it('should generate COPY TO SQL with default format', async () => {
            await sink.copyToFile('SELECT * FROM test', '/output.csv');

            expect(mockConnection.runAndRead).toHaveBeenCalled();
            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain('COPY');
            expect(sql).toContain('SELECT * FROM test');
            expect(sql).toContain("TO '/output.csv'");
            expect(sql).toContain('FORMAT CSV');
        });

        it('should generate COPY TO SQL with custom format', async () => {
            await sink.copyToFile('SELECT * FROM test', '/output.parquet', 'parquet');

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain('FORMAT PARQUET');
        });

        it('should include HEADER option when specified', async () => {
            const options: CopyToFileOptions = { header: true };
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', options);

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain('HEADER');
        });

        it('should include DELIMITER option when specified', async () => {
            const options: CopyToFileOptions = { delimiter: ';' };
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', options);

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain("DELIMITER ';'");
        });

        it('should handle both HEADER and DELIMITER options', async () => {
            const options: CopyToFileOptions = { header: true, delimiter: ';' };
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', options);

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain('HEADER');
            expect(sql).toContain("DELIMITER ';'");
        });

        it('should use semicolon delimiter for Brazilian locale files', async () => {
            const options: CopyToFileOptions = { header: true, delimiter: ';' };
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', options);

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain("DELIMITER ';'");
        });

        it('should use comma delimiter for US locale files', async () => {
            const options: CopyToFileOptions = { header: true, delimiter: ',' };
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', options);

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain("DELIMITER ','");
        });

        it('should not include options when not specified', async () => {
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', {});

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).not.toContain('HEADER');
            expect(sql).not.toContain('DELIMITER');
        });

        it('should handle empty options object', async () => {
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', {});

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain('FORMAT CSV');
        });

        it('should handle undefined options', async () => {
            await sink.copyToFile('SELECT * FROM test', '/output.csv', 'csv', undefined);

            const sql = mockConnection.runAndRead.mock.calls[0][0];
            expect(sql).toContain('FORMAT CSV');
        });
    });
});
