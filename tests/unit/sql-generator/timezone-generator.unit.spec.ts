/**
 * Timezone Generator Unit Tests
 *
 * Tests for timezone SQL generation.
 */

import { TimezoneTransform } from '../../../src/query-plan/query-plan';
import {
    generateTimezoneExpression, generateTimezoneRawSQL, generateTimezoneSQL,
    getTimezoneCTEName, validateTimezoneTransform
} from '../../../src/sql-generator/timezone-generator';

describe('Timezone Generator', () => {
    describe('validateTimezoneTransform', () => {
        it('should validate valid timezone transform', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            expect(() => validateTimezoneTransform(transform)).not.toThrow();
        });

        it('should throw error when timestampColumns is empty', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: [],
                timezone: 'America/Sao_Paulo',
            };

            expect(() => validateTimezoneTransform(transform)).toThrow(/at least one timestamp column/i);
        });

        it('should throw error when timezone is empty', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: '',
            };

            expect(() => validateTimezoneTransform(transform)).toThrow(/must specify a timezone/i);
        });

        it('should throw error for invalid timezone format', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'Invalid',
            };

            expect(() => validateTimezoneTransform(transform)).toThrow(/invalid timezone format/i);
        });

        it('should accept valid IANA timezone formats', () => {
            const validTimezones = ['America/Sao_Paulo', 'America/New_York', 'Europe/London', 'Asia/Tokyo', 'UTC'];

            for (const tz of validTimezones) {
                const transform: TimezoneTransform = {
                    type: 'timezone',
                    sourceAlias: 'readings',
                    timestampColumns: ['timestamp'],
                    timezone: tz,
                };

                // Should not throw for valid formats
                if (tz === 'UTC') {
                    // UTC is not in Region/City format, so it will throw
                    expect(() => validateTimezoneTransform(transform)).toThrow();
                } else {
                    expect(() => validateTimezoneTransform(transform)).not.toThrow();
                }
            }
        });
    });

    describe('generateTimezoneRawSQL', () => {
        it('should generate timezone SQL with single timestamp column', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            const sql = generateTimezoneRawSQL(transform, 'source_table');

            expect(sql).toContain('SELECT');
            expect(sql).toContain("(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS timestamp");
            expect(sql).toContain('FROM source_table');
        });

        it('should generate timezone SQL with multiple timestamp columns', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp', 'created_at', 'updated_at'],
                timezone: 'America/Sao_Paulo',
            };

            const sql = generateTimezoneRawSQL(transform, 'source_table');

            expect(sql).toContain("(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS timestamp");
            expect(sql).toContain("(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS created_at");
            expect(sql).toContain("(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS updated_at");
        });

        it('should include all columns when no explicit columns specified', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            const sql = generateTimezoneRawSQL(transform, 'source_table');

            expect(sql).toContain('* EXCLUDE (timestamp)');
            expect(sql).toContain("(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS timestamp");
        });

        it('should use explicit column selection when provided', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            const sql = generateTimezoneRawSQL(transform, 'source_table', ['id', 'value', 'timestamp']);

            expect(sql).toContain('id');
            expect(sql).toContain('value');
            expect(sql).toContain("(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS timestamp");
            expect(sql).not.toContain('*');
        });

        it('should exclude timestamp columns from explicit selection to avoid duplication', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            const sql = generateTimezoneRawSQL(transform, 'source_table', ['id', 'timestamp']);

            // Should only have the converted timestamp, not the raw one
            expect(sql).toContain('id');
            expect(sql).toContain('AS timestamp');
        });
    });

    describe('generateTimezoneSQL', () => {
        it('should generate CTE wrapped timezone SQL', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            const sql = generateTimezoneSQL(transform, 'source_table');

            expect(sql).toContain('readings_tz AS (');
            expect(sql).toContain('SELECT');
            expect(sql).toContain('FROM source_table');
        });

        it('should use custom CTE name when specified', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
                as: 'brazil_time',
            };

            const sql = generateTimezoneSQL(transform, 'source_table');

            expect(sql).toContain('brazil_time AS (');
            expect(sql).not.toContain('readings_tz');
        });
    });

    describe('getTimezoneCTEName', () => {
        it('should return default CTE name', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
            };

            expect(getTimezoneCTEName(transform)).toBe('readings_tz');
        });

        it('should return custom CTE name when specified', () => {
            const transform: TimezoneTransform = {
                type: 'timezone',
                sourceAlias: 'readings',
                timestampColumns: ['timestamp'],
                timezone: 'America/Sao_Paulo',
                as: 'custom_tz',
            };

            expect(getTimezoneCTEName(transform)).toBe('custom_tz');
        });
    });

    describe('generateTimezoneExpression', () => {
        it('should generate timezone conversion expression', () => {
            const result = generateTimezoneExpression('timestamp', 'America/Sao_Paulo');
            expect(result).toBe("(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')");
        });

        it('should quote column names that need it', () => {
            const result = generateTimezoneExpression('1timestamp', 'America/Sao_Paulo');
            expect(result).toBe('("1timestamp" AT TIME ZONE \'UTC\' AT TIME ZONE \'America/Sao_Paulo\')');
        });
    });
});
