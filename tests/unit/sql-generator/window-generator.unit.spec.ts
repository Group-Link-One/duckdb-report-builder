/**
 * Window Generator Unit Tests
 *
 * Tests for window function SQL generation.
 */

import { WindowFunctionSpec, WindowTransform } from '../../../src/query-plan/query-plan';
import {
    createArrayAggFunction, createLagFunction,
    createRowNumberFunction, generateWindowRawSQL, generateWindowSQL, getWindowCTEName,
    validateWindowTransform
} from '../../../src/sql-generator/window-generator';

describe('Window Generator', () => {
    describe('validateWindowTransform', () => {
        it('should validate valid window transform', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            expect(() => validateWindowTransform(transform)).not.toThrow();
        });

        it('should throw error when partitionBy is empty', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: [],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            expect(() => validateWindowTransform(transform)).toThrow(/partition by/i);
        });

        it('should throw error when orderBy is empty', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            expect(() => validateWindowTransform(transform)).toThrow(/order by/i);
        });

        it('should throw error when windowFunctions is empty', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [],
            };

            expect(() => validateWindowTransform(transform)).toThrow(/at least one window function/i);
        });

        it('should throw error when window function lacks column', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'LAG', outputAlias: 'prev' } as WindowFunctionSpec],
            };

            expect(() => validateWindowTransform(transform)).toThrow(/requires a column/i);
        });

        it('should throw error when window function lacks output alias', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: '' }],
            };

            expect(() => validateWindowTransform(transform)).toThrow(/output alias/i);
        });

        it('should allow ROW_NUMBER without column', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            expect(() => validateWindowTransform(transform)).not.toThrow();
        });

        it('should allow RANK without column', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'RANK', outputAlias: 'rank_num' }],
            };

            expect(() => validateWindowTransform(transform)).not.toThrow();
        });
    });

    describe('generateWindowRawSQL', () => {
        it('should generate ROW_NUMBER window SQL', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'DESC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('SELECT');
            expect(sql).toContain('*');
            expect(sql).toContain('ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) AS rn');
            expect(sql).toContain('FROM source_table');
        });

        it('should generate RANK window SQL', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['category'],
                orderBy: [{ column: 'score', direction: 'DESC' }],
                windowFunctions: [{ function: 'RANK', outputAlias: 'ranking' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('RANK() OVER (PARTITION BY category ORDER BY score DESC) AS ranking');
        });

        it('should generate LAG window SQL with default offset', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'LAG', column: 'value', outputAlias: 'prev_value' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain(
                'LAG(value, 1, NULL) OVER (PARTITION BY device_id ORDER BY timestamp ASC) AS prev_value'
            );
        });

        it('should generate LAG window SQL with custom offset and default value', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [
                    {
                        function: 'LAG',
                        column: 'value',
                        offset: 2,
                        defaultValue: 0,
                        outputAlias: 'prev_value',
                    },
                ],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain(
                'LAG(value, 2, 0) OVER (PARTITION BY device_id ORDER BY timestamp ASC) AS prev_value'
            );
        });

        it('should generate LAG with string default value', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [
                    {
                        function: 'LAG',
                        column: 'status',
                        outputAlias: 'prev_status',
                        defaultValue: 'UNKNOWN',
                    },
                ],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain("LAG(status, 1, 'UNKNOWN')");
        });

        it('should generate LEAD window SQL', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [
                    {
                        function: 'LEAD',
                        column: 'value',
                        offset: 1,
                        defaultValue: 0,
                        outputAlias: 'next_value',
                    },
                ],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain(
                'LEAD(value, 1, 0) OVER (PARTITION BY device_id ORDER BY timestamp ASC) AS next_value'
            );
        });

        it('should generate FIRST_VALUE window SQL', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'FIRST_VALUE', column: 'value', outputAlias: 'first_val' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain(
                'FIRST_VALUE(value) OVER (PARTITION BY device_id ORDER BY timestamp ASC) AS first_val'
            );
        });

        it('should generate LAST_VALUE window SQL with frame specification', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'LAST_VALUE', column: 'value', outputAlias: 'last_val' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain(
                'LAST_VALUE(value) OVER (PARTITION BY device_id ORDER BY timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_val'
            );
        });

        it('should generate ARRAY_AGG window SQL', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ARRAY_AGG', column: 'value', outputAlias: 'all_values' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain(
                'ARRAY_AGG(value) OVER (PARTITION BY device_id ORDER BY timestamp ASC) AS all_values'
            );
        });

        it('should generate multiple window functions', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [
                    { function: 'ROW_NUMBER', outputAlias: 'rn' },
                    { function: 'LAG', column: 'value', outputAlias: 'prev_value' },
                ],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('ROW_NUMBER()');
            expect(sql).toContain('LAG(value');
        });

        it('should generate multiple partition by columns', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id', 'channel'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('PARTITION BY device_id, channel');
        });

        it('should generate multiple order by columns', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [
                    { column: 'date', direction: 'DESC' },
                    { column: 'time', direction: 'ASC' },
                ],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('ORDER BY date DESC, time ASC');
        });

        it('should include QUALIFY clause when specified', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'DESC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
                qualify: 'rn = 1',
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('QUALIFY rn = 1');
        });

        it('should use function-specific orderBy when provided', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [
                    {
                        function: 'ARRAY_AGG',
                        column: 'value',
                        outputAlias: 'sorted_values',
                        orderBy: [{ column: 'value', direction: 'DESC' }],
                    },
                ],
            };

            const sql = generateWindowRawSQL(transform, 'source_table');

            expect(sql).toContain('PARTITION BY device_id ORDER BY value DESC');
        });

        it('should throw error for unsupported window function', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'UNKNOWN' as any, column: 'value', outputAlias: 'x' }],
            };

            expect(() => generateWindowRawSQL(transform, 'source_table')).toThrow(/unsupported window function/i);
        });
    });

    describe('generateWindowSQL', () => {
        it('should generate CTE wrapped window SQL', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            const sql = generateWindowSQL(transform, 'source_table');

            expect(sql).toContain('readings_windowed AS (');
            expect(sql).toContain('SELECT');
        });

        it('should use custom CTE name when specified', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
                as: 'with_row_numbers',
            };

            const sql = generateWindowSQL(transform, 'source_table');

            expect(sql).toContain('with_row_numbers AS (');
            expect(sql).not.toContain('readings_windowed');
        });
    });

    describe('getWindowCTEName', () => {
        it('should return default CTE name', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
            };

            expect(getWindowCTEName(transform)).toBe('readings_windowed');
        });

        it('should return custom CTE name when specified', () => {
            const transform: WindowTransform = {
                type: 'window',
                sourceAlias: 'readings',
                partitionBy: ['device_id'],
                orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
                as: 'custom_windowed',
            };

            expect(getWindowCTEName(transform)).toBe('custom_windowed');
        });
    });

    describe('createLagFunction', () => {
        it('should create LAG function with defaults', () => {
            const func = createLagFunction('value', 'prev_value');
            expect(func).toEqual({
                function: 'LAG',
                column: 'value',
                outputAlias: 'prev_value',
                offset: 1,
                defaultValue: 0,
            });
        });

        it('should create LAG function with custom values', () => {
            const func = createLagFunction('value', 'prev_value', 2, null);
            expect(func).toEqual({
                function: 'LAG',
                column: 'value',
                outputAlias: 'prev_value',
                offset: 2,
                defaultValue: null,
            });
        });
    });

    describe('createRowNumberFunction', () => {
        it('should create ROW_NUMBER function with default alias', () => {
            const func = createRowNumberFunction();
            expect(func).toEqual({
                function: 'ROW_NUMBER',
                outputAlias: 'rn',
            });
        });

        it('should create ROW_NUMBER function with custom alias', () => {
            const func = createRowNumberFunction('row_num');
            expect(func).toEqual({
                function: 'ROW_NUMBER',
                outputAlias: 'row_num',
            });
        });
    });

    describe('createArrayAggFunction', () => {
        it('should create ARRAY_AGG function without orderBy', () => {
            const func = createArrayAggFunction('value', 'all_values');
            expect(func).toEqual({
                function: 'ARRAY_AGG',
                column: 'value',
                outputAlias: 'all_values',
            });
        });

        it('should create ARRAY_AGG function with orderBy', () => {
            const orderBy = [{ column: 'timestamp', direction: 'ASC' as const }];
            const func = createArrayAggFunction('value', 'all_values', orderBy);
            expect(func).toEqual({
                function: 'ARRAY_AGG',
                column: 'value',
                outputAlias: 'all_values',
                orderBy,
            });
        });
    });
});
