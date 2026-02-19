/**
 * Enrichment Generator Unit Tests
 *
 * Tests for enrichment SQL generation.
 */

import { ApplyEnrichmentTransform } from '../../../src/query-plan/query-plan';
import {
    generateApplyEnrichmentRawSQL, generateApplyEnrichmentSQL, generateApplyEnrichmentSQLWithColumns, getApplyEnrichmentCTEName,
    validateApplyEnrichmentTransform
} from '../../../src/sql-generator/enrichment-generator';

describe('Enrichment Generator', () => {
    describe('validateApplyEnrichmentTransform', () => {
        it('should validate valid enrichment transform', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {
                    adjusted: { formula: 'raw * 2' },
                },
            };

            expect(() => validateApplyEnrichmentTransform(transform)).not.toThrow();
        });

        it('should throw error when lookupSourceAlias is missing', () => {
            const transform = {
                type: 'apply_enrichment' as const,
                sourceAlias: 'readings',
                joinOn: ['device_id'],
                formulas: { adjusted: { formula: 'raw * 2' } },
            } as unknown as ApplyEnrichmentTransform;

            expect(() => validateApplyEnrichmentTransform(transform)).toThrow(/lookupSourceAlias/i);
        });

        it('should throw error when joinOn is empty', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: [],
                formulas: { adjusted: { formula: 'raw * 2' } },
            };

            expect(() => validateApplyEnrichmentTransform(transform)).toThrow(/at least one join key/i);
        });

        it('should throw error when formulas is empty', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {},
            };

            expect(() => validateApplyEnrichmentTransform(transform)).toThrow(/at least one formula/i);
        });

        it('should throw error when formula is empty', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {
                    adjusted: { formula: '' },
                },
            };

            expect(() => validateApplyEnrichmentTransform(transform)).toThrow(/non-empty formula/i);
        });
    });

    describe('generateApplyEnrichmentRawSQL', () => {
        it('should generate enrichment SQL with single join key', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {
                    adjusted: { formula: 'r.raw_value * c.multiplier' },
                },
            };

            const sql = generateApplyEnrichmentRawSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('SELECT');
            expect(sql).toContain('r.*');
            expect(sql).toContain('r.raw_value * c.multiplier AS adjusted');
            expect(sql).toContain('FROM readings_table AS r');
            expect(sql).toContain('LEFT JOIN context_table AS c');
            expect(sql).toContain('r.device_id = c.device_id');
        });

        it('should generate enrichment SQL with multiple join keys', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id', 'channel'],
                formulas: {
                    adjusted: { formula: 'r.raw_value * c.multiplier' },
                },
            };

            const sql = generateApplyEnrichmentRawSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('r.device_id = c.device_id');
            expect(sql).toContain('r.channel = c.channel');
            expect(sql).toContain('AND');
        });

        it('should generate enrichment SQL with multiple formulas', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {
                    adjusted_value: { formula: 'r.raw_value * c.multiplier' },
                    adjusted_cost: { formula: 'r.daily_raw * c.multiplier' },
                },
            };

            const sql = generateApplyEnrichmentRawSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('r.raw_value * c.multiplier AS adjusted_value');
            expect(sql).toContain('r.daily_raw * c.multiplier AS adjusted_cost');
        });

        it('should support legacy contextSourceAlias field', () => {
            const transform = {
                type: 'apply_enrichment' as const,
                sourceAlias: 'readings',
                contextSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {
                    adjusted: { formula: 'r.raw_value * c.multiplier' },
                },
            } as unknown as ApplyEnrichmentTransform;

            const sql = generateApplyEnrichmentRawSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('FROM readings_table AS r');
            expect(sql).toContain('LEFT JOIN context_table AS c');
        });

        it('should support legacy adjustments field', () => {
            const transform = {
                type: 'apply_enrichment' as const,
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                adjustments: {
                    adjusted: { formula: 'r.raw_value * c.multiplier' },
                },
            } as unknown as ApplyEnrichmentTransform;

            const sql = generateApplyEnrichmentRawSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('r.raw_value * c.multiplier AS adjusted');
        });

        it('should use explicit column selection when provided', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {
                    adjusted: { formula: 'r.raw_value * c.multiplier' },
                },
            };

            const sql = generateApplyEnrichmentRawSQL(transform, 'readings_table', 'context_table', [
                'timestamp',
                'device_id',
                'raw_value',
            ]);

            expect(sql).toContain('r.timestamp');
            expect(sql).toContain('r.device_id');
            expect(sql).toContain('r.raw_value');
            expect(sql).not.toContain('r.*');
        });
    });

    describe('generateApplyEnrichmentSQL', () => {
        it('should generate CTE wrapped enrichment SQL', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: { adjusted: { formula: 'r.raw * c.m' } },
            };

            const sql = generateApplyEnrichmentSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('readings_enriched AS (');
            expect(sql).toContain('SELECT');
        });

        it('should use custom CTE name when specified', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: { adjusted: { formula: 'r.raw * c.m' } },
                as: 'custom_enriched',
            };

            const sql = generateApplyEnrichmentSQL(transform, 'readings_table', 'context_table');

            expect(sql).toContain('custom_enriched AS (');
            expect(sql).not.toContain('readings_enriched');
        });
    });

    describe('generateApplyEnrichmentSQLWithColumns', () => {
        it('should generate enrichment SQL with explicit columns', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: { adjusted: { formula: 'r.raw * c.m' } },
            };

            const sql = generateApplyEnrichmentSQLWithColumns(transform, 'readings_table', 'context_table', [
                'timestamp',
                'value',
            ]);

            expect(sql).toContain('readings_enriched AS (');
            expect(sql).toContain('r.timestamp');
            expect(sql).toContain('r.value');
        });
    });

    describe('getApplyEnrichmentCTEName', () => {
        it('should return default CTE name', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {},
            };

            expect(getApplyEnrichmentCTEName(transform)).toBe('readings_enriched');
        });

        it('should return custom CTE name when specified', () => {
            const transform: ApplyEnrichmentTransform = {
                type: 'apply_enrichment',
                sourceAlias: 'readings',
                lookupSourceAlias: 'context',
                joinOn: ['device_id'],
                formulas: {},
                as: 'custom_enriched',
            };

            expect(getApplyEnrichmentCTEName(transform)).toBe('custom_enriched');
        });
    });
});
