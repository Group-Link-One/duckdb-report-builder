/**
 * Execution Strategy Unit Tests
 *
 * Tests for execution strategy utilities.
 */

import {
    composeCallbacks,
    createProgressCallback,
    createRowCountCallback,
    createSampleDataCallback, ExecutionOptions, StepInfo
} from '../../../src/core/execution-strategy';

describe('Execution Strategy', () => {
    describe('composeCallbacks', () => {
        it('should compose multiple callbacks', async () => {
            const callback1 = jest.fn().mockResolvedValue(undefined);
            const callback2 = jest.fn().mockResolvedValue(undefined);
            const callback3 = jest.fn().mockResolvedValue(undefined);

            const composed = composeCallbacks(callback1, callback2, callback3);

            const stepInfo: StepInfo = {
                name: 'test',
                tableName: 'test_table',
                stepNumber: 0,
                totalSteps: 3,
                position: 'between_transforms',
            };
            const mockConn = {} as any;

            await composed(stepInfo, mockConn);

            expect(callback1).toHaveBeenCalledWith(stepInfo, mockConn);
            expect(callback2).toHaveBeenCalledWith(stepInfo, mockConn);
            expect(callback3).toHaveBeenCalledWith(stepInfo, mockConn);
        });

        it('should execute callbacks in order', async () => {
            const order: number[] = [];
            const callback1 = jest.fn().mockImplementation(async () => {
                order.push(1);
            });
            const callback2 = jest.fn().mockImplementation(async () => {
                order.push(2);
            });
            const callback3 = jest.fn().mockImplementation(async () => {
                order.push(3);
            });

            const composed = composeCallbacks(callback1, callback2, callback3);

            const stepInfo: StepInfo = {
                name: 'test',
                tableName: 'test_table',
                stepNumber: 0,
                totalSteps: 3,
                position: 'between_transforms',
            };

            await composed(stepInfo, {} as any);

            expect(order).toEqual([1, 2, 3]);
        });

        it('should handle single callback', async () => {
            const callback = jest.fn().mockResolvedValue(undefined);
            const composed = composeCallbacks(callback);

            const stepInfo: StepInfo = {
                name: 'test',
                tableName: 'test_table',
                stepNumber: 0,
                totalSteps: 1,
                position: 'between_transforms',
            };

            await composed(stepInfo, {} as any);

            expect(callback).toHaveBeenCalledTimes(1);
        });

        it('should handle no callbacks', async () => {
            const composed = composeCallbacks();
            const stepInfo: StepInfo = {
                name: 'test',
                tableName: 'test_table',
                stepNumber: 0,
                totalSteps: 1,
                position: 'between_transforms',
            };

            await expect(composed(stepInfo, {} as any)).resolves.not.toThrow();
        });
    });

    describe('createProgressCallback', () => {
        it('should log progress percentage', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createProgressCallback('TestReport');

            const stepInfo: StepInfo = {
                name: 'transform:pivot',
                tableName: 'readings_pivoted',
                stepNumber: 0,
                totalSteps: 3,
                position: 'between_transforms',
            };

            await callback(stepInfo, {} as any);

            expect(consoleSpy).toHaveBeenCalled();
            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('[TestReport]');
            expect(logOutput).toContain('33%'); // 1/3 rounded
            expect(logOutput).toContain('transform:pivot');
            expect(logOutput).toContain('readings_pivoted');

            consoleSpy.mockRestore();
        });

        it('should calculate correct progress for last step', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createProgressCallback('TestReport');

            const stepInfo: StepInfo = {
                name: 'transform:final',
                tableName: 'final_result',
                stepNumber: 2,
                totalSteps: 3,
                position: 'between_transforms',
            };

            await callback(stepInfo, {} as any);

            expect(consoleSpy).toHaveBeenCalled();
            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('100%'); // 3/3

            consoleSpy.mockRestore();
        });

        it('should calculate correct progress for middle step', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createProgressCallback('TestReport');

            const stepInfo: StepInfo = {
                name: 'transform:middle',
                tableName: 'middle_result',
                stepNumber: 1,
                totalSteps: 4,
                position: 'between_transforms',
            };

            await callback(stepInfo, {} as any);

            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('50%'); // 2/4

            consoleSpy.mockRestore();
        });
    });

    describe('createRowCountCallback', () => {
        it('should log row count', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createRowCountCallback();

            const mockResult = {
                columnCount: 1,
                columnName: (_i: number) => 'row_count',
                chunkCount: 1,
                getChunk: (_i: number) => ({
                    getRowObjects: (_cols: string[]) => [{ row_count: 42n }],
                }),
            };

            const mockConn = {
                run: jest.fn().mockResolvedValue(mockResult),
            };

            const stepInfo: StepInfo = {
                name: 'transform:pivot',
                tableName: 'readings_pivoted',
                stepNumber: 0,
                totalSteps: 1,
                position: 'between_transforms',
            };

            await callback(stepInfo, mockConn as any);

            expect(consoleSpy).toHaveBeenCalled();
            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('[transform:pivot]');
            expect(logOutput).toContain('Row count:');
            expect(logOutput).toContain('42');

            consoleSpy.mockRestore();
        });

        it('should handle large row counts', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createRowCountCallback();

            const mockResult = {
                columnCount: 1,
                columnName: (_i: number) => 'row_count',
                chunkCount: 1,
                getChunk: (_i: number) => ({
                    getRowObjects: (_cols: string[]) => [{ row_count: 1000000n }],
                }),
            };

            const mockConn = {
                run: jest.fn().mockResolvedValue(mockResult),
            };

            const stepInfo: StepInfo = {
                name: 'transform:pivot',
                tableName: 'readings_pivoted',
                stepNumber: 0,
                totalSteps: 1,
                position: 'between_transforms',
            };

            await callback(stepInfo, mockConn as any);

            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('1,000,000');

            consoleSpy.mockRestore();
        });
    });

    describe('createSampleDataCallback', () => {
        it('should log sample data with default limit', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createSampleDataCallback();

            const mockResult = {
                columnCount: 2,
                columnName: (i: number) => (i === 0 ? 'id' : 'value'),
                chunkCount: 1,
                getChunk: (_i: number) => ({
                    getRowObjects: (_cols: string[]) => [
                        { id: 1, value: 'a' },
                        { id: 2, value: 'b' },
                        { id: 3, value: 'c' },
                    ],
                }),
            };

            const mockConn = {
                run: jest.fn().mockResolvedValue(mockResult),
            };

            const stepInfo: StepInfo = {
                name: 'transform:pivot',
                tableName: 'readings_pivoted',
                stepNumber: 0,
                totalSteps: 1,
                position: 'between_transforms',
            };

            await callback(stepInfo, mockConn as any);

            expect(consoleSpy).toHaveBeenCalled();
            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('[transform:pivot]');
            expect(logOutput).toContain('Sample data');
            expect(logOutput).toContain('3 rows');

            consoleSpy.mockRestore();
        });

        it('should log sample data with custom limit', async () => {
            const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
            const callback = createSampleDataCallback(5);

            const mockResult = {
                columnCount: 1,
                columnName: (_i: number) => 'id',
                chunkCount: 1,
                getChunk: (_i: number) => ({
                    getRowObjects: (_cols: string[]) => [{ id: 1 }],
                }),
            };

            const mockConn = {
                run: jest.fn().mockResolvedValue(mockResult),
            };

            const stepInfo: StepInfo = {
                name: 'transform:pivot',
                tableName: 'readings_pivoted',
                stepNumber: 0,
                totalSteps: 1,
                position: 'between_transforms',
            };

            await callback(stepInfo, mockConn as any);

            const logOutput = consoleSpy.mock.calls[0][0];
            expect(logOutput).toContain('5 rows');

            consoleSpy.mockRestore();
        });
    });

    describe('Type definitions', () => {
        it('should have correct ExecutionStrategy type', () => {
            // Type-only test - just ensure the types compile
            const cteOptions: ExecutionOptions = {
                strategy: 'cte',
            };
            const tempOptions: ExecutionOptions = {
                strategy: 'temp_tables',
            };

            expect(cteOptions.strategy).toBe('cte');
            expect(tempOptions.strategy).toBe('temp_tables');
        });

        it('should have correct StepInfo structure', () => {
            const stepInfo: StepInfo = {
                name: 'transform:test',
                tableName: 'test_table',
                stepNumber: 0,
                totalSteps: 5,
                position: 'between_transforms',
                sql: 'SELECT * FROM test',
            };

            expect(stepInfo.name).toBe('transform:test');
            expect(stepInfo.tableName).toBe('test_table');
            expect(stepInfo.stepNumber).toBe(0);
            expect(stepInfo.totalSteps).toBe(5);
            expect(stepInfo.position).toBe('between_transforms');
            expect(stepInfo.sql).toBe('SELECT * FROM test');
        });
    });
});
