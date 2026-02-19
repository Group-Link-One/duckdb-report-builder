/**
 * Coarsen Generator Unit Tests
 *
 * Tests for coarsen SQL generation.
 */

import { AggregationStrategy, CoarsenTransform } from '../../../src/query-plan/query-plan';
import {
    generateCoarsenRawSQL, generateCoarsenSQL, getCoarsenCTEName,
    validateCoarsenTransform
} from '../../../src/sql-generator/coarsen-generator';

describe('Coarsen Generator', () => {
    describe('validateCoarsenTransform', () => {
        it('should validate valid coarsen transform', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
            };

            expect(() => validateCoarsenTransform(transform)).not.toThrow();
        });

        it('should throw error for invalid from granularity', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'invalid' as any,
                to: 'hour',
                strategy: { value: 'avg' },
            };

            expect(() => validateCoarsenTransform(transform)).toThrow(/invalid "from" granularity/i);
        });

        it('should throw error for invalid to granularity', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'invalid' as any,
                strategy: { value: 'avg' },
            };

            expect(() => validateCoarsenTransform(transform)).toThrow(/invalid "to" granularity/i);
        });

        it('should throw error when from is coarser than to', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'hour',
                to: 'minute',
                strategy: { value: 'avg' },
            };

            expect(() => validateCoarsenTransform(transform)).toThrow(/target granularity must be coarser/i);
        });

        it('should throw error when from equals to', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'hour',
                to: 'hour',
                strategy: { value: 'avg' },
            };

            expect(() => validateCoarsenTransform(transform)).toThrow(/target granularity must be coarser/i);
        });

        it('should throw error when strategy is empty', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: {},
            };

            expect(() => validateCoarsenTransform(transform)).toThrow(/must specify at least one column strategy/i);
        });
    });

    describe('generateCoarsenRawSQL', () => {
        it('should generate coarsen SQL with AVG strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { voltage: 'avg' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('SELECT');
            expect(sql).toContain("DATE_TRUNC('hour', timestamp) AS timestamp");
            expect(sql).toContain('AVG(voltage) AS voltage');
            expect(sql).toContain('FROM source_table');
            expect(sql).toContain('GROUP BY');
            expect(sql).toContain('ORDER BY timestamp');
        });

        it('should generate coarsen SQL with SUM strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { consumption: 'sum' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('SUM(consumption) AS consumption');
        });

        it('should generate coarsen SQL with MIN strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { temperature: 'min' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('MIN(temperature) AS temperature');
        });

        it('should generate coarsen SQL with MAX strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { temperature: 'max' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('MAX(temperature) AS temperature');
        });

        it('should generate coarsen SQL with FIRST strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'first' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('FIRST(value ORDER BY timestamp) AS value');
        });

        it('should generate coarsen SQL with LAST strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'last' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('LAST(value ORDER BY timestamp) AS value');
        });

        it('should generate coarsen SQL with COUNT strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'count' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('COUNT(value) AS value');
        });

        it('should handle multiple columns with different strategies', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: {
                    consumption: 'sum',
                    voltage: 'avg',
                    soc: 'last',
                },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('SUM(consumption) AS consumption');
            expect(sql).toContain('AVG(voltage) AS voltage');
            expect(sql).toContain('LAST(soc ORDER BY timestamp) AS soc');
        });

        it('should use custom timestamp column when specified', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
                timestampColumn: 'event_time',
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain("DATE_TRUNC('hour', event_time) AS event_time");
        });

        it('should include groupBy columns when specified', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
                groupBy: ['device_id', 'channel'],
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain('device_id');
            expect(sql).toContain('channel');
            expect(sql).toContain('GROUP BY');
        });

        it('should handle hour to day coarsening', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'hour',
                to: 'day',
                strategy: { value: 'sum' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain("DATE_TRUNC('day', timestamp)");
        });

        it('should handle day to week coarsening', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'day',
                to: 'week',
                strategy: { value: 'sum' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain("DATE_TRUNC('week', timestamp)");
        });

        it('should handle day to month coarsening', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'day',
                to: 'month',
                strategy: { value: 'sum' },
            };

            const sql = generateCoarsenRawSQL(transform, 'source_table');

            expect(sql).toContain("DATE_TRUNC('month', timestamp)");
        });

        it('should throw error for unknown aggregation strategy', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'unknown' as AggregationStrategy },
            };

            expect(() => generateCoarsenRawSQL(transform, 'source_table')).toThrow(/unknown aggregation strategy/i);
        });
    });

    describe('generateCoarsenSQL', () => {
        it('should generate CTE wrapped coarsen SQL', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
            };

            const sql = generateCoarsenSQL(transform, 'source_table');

            expect(sql).toContain('readings_coarsened AS (');
            expect(sql).toContain('SELECT');
        });

        it('should use custom CTE name when specified', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
                as: 'hourly_data',
            };

            const sql = generateCoarsenSQL(transform, 'source_table');

            expect(sql).toContain('hourly_data AS (');
            expect(sql).not.toContain('readings_coarsened');
        });
    });

    describe('getCoarsenCTEName', () => {
        it('should return default CTE name', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
            };

            expect(getCoarsenCTEName(transform)).toBe('readings_coarsened');
        });

        it('should return custom CTE name when specified', () => {
            const transform: CoarsenTransform = {
                type: 'coarsen',
                sourceAlias: 'readings',
                from: 'minute',
                to: 'hour',
                strategy: { value: 'avg' },
                as: 'custom_coarsened',
            };

            expect(getCoarsenCTEName(transform)).toBe('custom_coarsened');
        });
    });
});
