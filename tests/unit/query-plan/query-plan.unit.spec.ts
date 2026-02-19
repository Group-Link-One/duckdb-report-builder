/**
 * Query Plan Unit Tests
 *
 * Tests for the QueryPlan IR types, type guards, and validation functions.
 */

import { InMemoryProvider } from '../../../src/providers/in-memory-provider';
import {
    ApplyEnrichmentTransform, CoarsenTransform, FilterTransform, isApplyEnrichmentTransform, isCoarsenTransform, isFilterTransform, isJoinTransform, isLocfTransform, isPivotTransform, isTimezoneTransform,
    isWindowTransform, JoinTransform, LocfTransform, PivotTransform, QueryPlan, TimezoneTransform, validateQueryPlan, WindowTransform
} from '../../../src/query-plan/query-plan';

describe('Query Plan', () => {
    describe('Type Guards', () => {
        const mockProvider = new InMemoryProvider([], 'test');

        describe('isPivotTransform', () => {
            it('should return true for pivot transforms', () => {
                const transform: PivotTransform = {
                    type: 'pivot',
                    sourceAlias: 'readings',
                    pivotColumn: 'serie_id',
                    valueColumn: 'raw_value',
                    columns: [{ pivotValue: 2, outputAlias: 'energy_in' }],
                    groupBy: ['timestamp'],
                };
                expect(isPivotTransform(transform)).toBe(true);
            });

            it('should return false for non-pivot transforms', () => {
                const transform: JoinTransform = {
                    type: 'join',
                    leftAlias: 'readings',
                    rightAlias: 'context',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'device_id', right: 'device_id' }],
                };
                expect(isPivotTransform(transform)).toBe(false);
            });
        });

        describe('isLocfTransform', () => {
            it('should return true for LOCF transforms', () => {
                const transform: LocfTransform = {
                    type: 'locf',
                    sourceAlias: 'readings',
                    baseTimelineAlias: 'timeline',
                    joinKeys: ['device_id'],
                    columns: ['serial_number'],
                    maxLookbackSeconds: 300,
                };
                expect(isLocfTransform(transform)).toBe(true);
            });

            it('should return false for non-LOCF transforms', () => {
                const transform: PivotTransform = {
                    type: 'pivot',
                    sourceAlias: 'readings',
                    pivotColumn: 'serie_id',
                    valueColumn: 'raw_value',
                    columns: [],
                    groupBy: ['timestamp'],
                };
                expect(isLocfTransform(transform)).toBe(false);
            });
        });

        describe('isJoinTransform', () => {
            it('should return true for join transforms', () => {
                const transform: JoinTransform = {
                    type: 'join',
                    leftAlias: 'readings',
                    rightAlias: 'context',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'device_id', right: 'device_id' }],
                };
                expect(isJoinTransform(transform)).toBe(true);
            });

            it('should return false for non-join transforms', () => {
                const transform: FilterTransform = {
                    type: 'filter',
                    sourceAlias: 'readings',
                    condition: 'value IS NOT NULL',
                };
                expect(isJoinTransform(transform)).toBe(false);
            });
        });

        describe('isFilterTransform', () => {
            it('should return true for filter transforms', () => {
                const transform: FilterTransform = {
                    type: 'filter',
                    sourceAlias: 'readings',
                    condition: 'value > 0',
                };
                expect(isFilterTransform(transform)).toBe(true);
            });

            it('should return false for non-filter transforms', () => {
                const transform: JoinTransform = {
                    type: 'join',
                    leftAlias: 'a',
                    rightAlias: 'b',
                    joinType: 'INNER',
                    onConditions: [],
                };
                expect(isFilterTransform(transform)).toBe(false);
            });
        });

        describe('isCoarsenTransform', () => {
            it('should return true for coarsen transforms', () => {
                const transform: CoarsenTransform = {
                    type: 'coarsen',
                    sourceAlias: 'readings',
                    from: 'minute',
                    to: 'hour',
                    strategy: { value: 'avg' },
                };
                expect(isCoarsenTransform(transform)).toBe(true);
            });

            it('should return false for non-coarsen transforms', () => {
                const transform: PivotTransform = {
                    type: 'pivot',
                    sourceAlias: 'readings',
                    pivotColumn: 'id',
                    valueColumn: 'val',
                    columns: [],
                    groupBy: [],
                };
                expect(isCoarsenTransform(transform)).toBe(false);
            });
        });

        describe('isApplyEnrichmentTransform', () => {
            it('should return true for apply_enrichment transforms', () => {
                const transform: ApplyEnrichmentTransform = {
                    type: 'apply_enrichment',
                    sourceAlias: 'readings',
                    lookupSourceAlias: 'context',
                    joinOn: ['device_id'],
                    formulas: { adjusted: { formula: 'raw * 2' } },
                };
                expect(isApplyEnrichmentTransform(transform)).toBe(true);
            });

            it('should return false for non-enrichment transforms', () => {
                const transform: PivotTransform = {
                    type: 'pivot',
                    sourceAlias: 'readings',
                    pivotColumn: 'id',
                    valueColumn: 'val',
                    columns: [],
                    groupBy: [],
                };
                expect(isApplyEnrichmentTransform(transform)).toBe(false);
            });
        });

        describe('isTimezoneTransform', () => {
            it('should return true for timezone transforms', () => {
                const transform: TimezoneTransform = {
                    type: 'timezone',
                    sourceAlias: 'readings',
                    timestampColumns: ['timestamp'],
                    timezone: 'America/Sao_Paulo',
                };
                expect(isTimezoneTransform(transform)).toBe(true);
            });

            it('should return false for non-timezone transforms', () => {
                const transform: PivotTransform = {
                    type: 'pivot',
                    sourceAlias: 'readings',
                    pivotColumn: 'id',
                    valueColumn: 'val',
                    columns: [],
                    groupBy: [],
                };
                expect(isTimezoneTransform(transform)).toBe(false);
            });
        });

        describe('isWindowTransform', () => {
            it('should return true for window transforms', () => {
                const transform: WindowTransform = {
                    type: 'window',
                    sourceAlias: 'readings',
                    partitionBy: ['device_id'],
                    orderBy: [{ column: 'timestamp', direction: 'ASC' }],
                    windowFunctions: [{ function: 'ROW_NUMBER', outputAlias: 'rn' }],
                };
                expect(isWindowTransform(transform)).toBe(true);
            });

            it('should return false for non-window transforms', () => {
                const transform: PivotTransform = {
                    type: 'pivot',
                    sourceAlias: 'readings',
                    pivotColumn: 'id',
                    valueColumn: 'val',
                    columns: [],
                    groupBy: [],
                };
                expect(isWindowTransform(transform)).toBe(false);
            });
        });
    });

    describe('validateQueryPlan', () => {
        const mockProvider = new InMemoryProvider([{ id: 1 }], 'test');

        const createValidPlan = (): QueryPlan => ({
            context: {
                period: {
                    from: new Date('2024-01-01'),
                    until: new Date('2024-01-31'),
                },
                timezone: 'UTC',
                params: {},
            },
            sources: [{ alias: 'readings', provider: mockProvider }],
            transforms: [],
            output: {
                columns: [{ sourceAlias: 'readings', sourceColumn: 'id', outputAlias: 'id' }],
            },
        });

        it('should validate a valid query plan', () => {
            const plan = createValidPlan();
            expect(() => validateQueryPlan(plan)).not.toThrow();
        });

        it('should throw error when period from is after until', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                context: {
                    ...createValidPlan().context,
                    period: {
                        from: new Date('2024-01-31'),
                        until: new Date('2024-01-01'),
                    },
                },
            };
            expect(() => validateQueryPlan(plan)).toThrow(/from.*must be before.*until/i);
        });

        it('should throw error when period from equals until', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                context: {
                    ...createValidPlan().context,
                    period: {
                        from: new Date('2024-01-01'),
                        until: new Date('2024-01-01'),
                    },
                },
            };
            expect(() => validateQueryPlan(plan)).toThrow(/from.*must be before.*until/i);
        });

        it('should throw error when no sources are specified', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                sources: [],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/at least one source/i);
        });

        it('should throw error for duplicate source aliases', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                sources: [
                    { alias: 'readings', provider: mockProvider },
                    { alias: 'readings', provider: mockProvider },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/duplicate source alias/i);
        });

        it('should throw error when source has no alias', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                sources: [{ alias: '', provider: mockProvider }],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/source must have an alias/i);
        });

        it('should throw error when no output columns are specified', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                output: {
                    columns: [],
                },
            };
            expect(() => validateQueryPlan(plan)).toThrow(/at least one column/i);
        });

        it('should throw error for pivot transform referencing unknown source', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                transforms: [
                    {
                        type: 'pivot',
                        sourceAlias: 'unknown',
                        pivotColumn: 'id',
                        valueColumn: 'val',
                        columns: [],
                        groupBy: [],
                    },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/unknown source/i);
        });

        it('should throw error for LOCF transform referencing unknown source', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                transforms: [
                    {
                        type: 'locf',
                        sourceAlias: 'unknown',
                        baseTimelineAlias: 'timeline',
                        joinKeys: ['id'],
                        columns: ['value'],
                        maxLookbackSeconds: null,
                    },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/unknown source/i);
        });

        it('should throw error for LOCF transform referencing unknown timeline', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                transforms: [
                    {
                        type: 'locf',
                        sourceAlias: 'readings',
                        baseTimelineAlias: 'unknown',
                        joinKeys: ['id'],
                        columns: ['value'],
                        maxLookbackSeconds: null,
                    },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/unknown timeline source/i);
        });

        it('should throw error for coarsen transform referencing unknown source', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                transforms: [
                    {
                        type: 'coarsen',
                        sourceAlias: 'unknown',
                        from: 'minute',
                        to: 'hour',
                        strategy: { value: 'avg' },
                    },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/unknown source/i);
        });

        it('should throw error for join transform referencing unknown left source', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                sources: [
                    { alias: 'readings', provider: mockProvider },
                    { alias: 'context', provider: mockProvider },
                ],
                transforms: [
                    {
                        type: 'join',
                        leftAlias: 'unknown',
                        rightAlias: 'context',
                        joinType: 'LEFT',
                        onConditions: [{ left: 'id', right: 'id' }],
                    },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/unknown left source/i);
        });

        it('should throw error for join transform referencing unknown right source', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                sources: [
                    { alias: 'readings', provider: mockProvider },
                    { alias: 'context', provider: mockProvider },
                ],
                transforms: [
                    {
                        type: 'join',
                        leftAlias: 'readings',
                        rightAlias: 'unknown',
                        joinType: 'LEFT',
                        onConditions: [{ left: 'id', right: 'id' }],
                    },
                ],
            };
            expect(() => validateQueryPlan(plan)).toThrow(/unknown right source/i);
        });

        it('should validate plan with valid pivot transform', () => {
            const plan: QueryPlan = {
                ...createValidPlan(),
                transforms: [
                    {
                        type: 'pivot',
                        sourceAlias: 'readings',
                        pivotColumn: 'serie_id',
                        valueColumn: 'raw_value',
                        columns: [{ pivotValue: 2, outputAlias: 'energy_in' }],
                        groupBy: ['timestamp'],
                    },
                ],
                output: {
                    columns: [
                        {
                            sourceAlias: 'readings_pivoted',
                            sourceColumn: 'energy_in',
                            outputAlias: 'energy_in',
                        },
                    ],
                },
            };
            expect(() => validateQueryPlan(plan)).not.toThrow();
        });
    });
});
