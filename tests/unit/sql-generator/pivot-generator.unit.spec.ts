/**
 * Pivot Generator Unit Tests
 *
 * Tests for pivot SQL generation.
 */

import { PivotTransform } from '../../../src/query-plan/query-plan';
import {
    generatePivotRawSQL, generatePivotSQL, getPivotCTEName,
    inferGroupByColumns
} from '../../../src/sql-generator/pivot-generator';

describe('Pivot Generator', () => {
    describe('generatePivotRawSQL', () => {
        it('should generate pivot SQL with numeric pivot values', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [
                    { pivotValue: 2, outputAlias: 'energy_in' },
                    { pivotValue: 3, outputAlias: 'energy_out' },
                ],
                groupBy: ['timestamp', 'device_id'],
            };

            const sql = generatePivotRawSQL(transform, 'source_table');

            expect(sql).toContain('SELECT');
            expect(sql).toContain('timestamp');
            expect(sql).toContain('device_id');
            expect(sql).toContain('MAX(CASE WHEN serie_id = 2 THEN raw_value END) AS energy_in');
            expect(sql).toContain('MAX(CASE WHEN serie_id = 3 THEN raw_value END) AS energy_out');
            expect(sql).toContain('FROM source_table');
            expect(sql).toContain('GROUP BY timestamp, device_id');
        });

        it('should generate pivot SQL with string pivot values', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'type',
                valueColumn: 'value',
                columns: [
                    { pivotValue: 'A', outputAlias: 'type_a' },
                    { pivotValue: 'B', outputAlias: 'type_b' },
                ],
                groupBy: ['id'],
            };

            const sql = generatePivotRawSQL(transform, 'source_table');

            expect(sql).toContain("MAX(CASE WHEN type = 'A' THEN value END) AS type_a");
            expect(sql).toContain("MAX(CASE WHEN type = 'B' THEN value END) AS type_b");
        });

        it('should throw error when groupBy is empty', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [{ pivotValue: 2, outputAlias: 'energy_in' }],
                groupBy: [],
            };

            expect(() => generatePivotRawSQL(transform, 'source_table')).toThrow(/must specify groupBy/i);
        });

        it('should throw error when groupBy is undefined', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [{ pivotValue: 2, outputAlias: 'energy_in' }],
            } as PivotTransform;

            expect(() => generatePivotRawSQL(transform, 'source_table')).toThrow(/must specify groupBy/i);
        });

        it('should handle pivot columns with LOCF configuration', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [
                    {
                        pivotValue: 2,
                        outputAlias: 'energy_in',
                        locf: { enabled: true, maxLookbackSeconds: 300 },
                    },
                ],
                groupBy: ['timestamp'],
            };

            const sql = generatePivotRawSQL(transform, 'source_table');
            expect(sql).toContain('energy_in');
        });
    });

    describe('generatePivotSQL', () => {
        it('should generate CTE wrapped pivot SQL', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [{ pivotValue: 2, outputAlias: 'energy_in' }],
                groupBy: ['timestamp'],
            };

            const sql = generatePivotSQL(transform, 'source_table');

            expect(sql).toContain('readings_pivoted AS (');
            expect(sql).toContain('SELECT');
            expect(sql).toContain('FROM source_table');
        });

        it('should use custom CTE name when specified', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [{ pivotValue: 2, outputAlias: 'energy_in' }],
                groupBy: ['timestamp'],
                as: 'custom_pivot',
            };

            const sql = generatePivotSQL(transform, 'source_table');

            expect(sql).toContain('custom_pivot AS (');
            expect(sql).not.toContain('readings_pivoted');
        });
    });

    describe('getPivotCTEName', () => {
        it('should return default CTE name', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [],
                groupBy: ['timestamp'],
            };

            expect(getPivotCTEName(transform)).toBe('readings_pivoted');
        });

        it('should return custom CTE name when specified', () => {
            const transform: PivotTransform = {
                type: 'pivot',
                sourceAlias: 'readings',
                pivotColumn: 'serie_id',
                valueColumn: 'raw_value',
                columns: [],
                groupBy: ['timestamp'],
                as: 'my_custom_pivot',
            };

            expect(getPivotCTEName(transform)).toBe('my_custom_pivot');
        });
    });

    describe('inferGroupByColumns', () => {
        it('should infer group by columns excluding pivot and value columns', () => {
            const sourceColumns = ['timestamp', 'device_id', 'serie_id', 'raw_value'];
            const result = inferGroupByColumns(sourceColumns, 'serie_id', 'raw_value');
            expect(result).toEqual(['timestamp', 'device_id']);
        });

        it('should return empty array when only pivot and value columns exist', () => {
            const sourceColumns = ['serie_id', 'raw_value'];
            const result = inferGroupByColumns(sourceColumns, 'serie_id', 'raw_value');
            expect(result).toEqual([]);
        });

        it('should handle case where pivot column appears multiple times', () => {
            const sourceColumns = ['timestamp', 'serie_id', 'raw_value', 'extra'];
            const result = inferGroupByColumns(sourceColumns, 'serie_id', 'raw_value');
            expect(result).toEqual(['timestamp', 'extra']);
        });
    });
});
