import { LocfTransform } from '../../../src/query-plan/query-plan';
import { generateLocfRawSQL, generateLocfSQL, getLocfCTEName } from '../../../src/sql-generator/locf-generator';

describe('LOCF Generator', () => {
    const baseTransform: LocfTransform = {
        type: 'locf',
        sourceAlias: 'readings',
        joinKeys: ['device_id'],
        columns: ['serial_number'],
        maxLookbackSeconds: null,
    };

    describe('getLocfCTEName', () => {
        it('should return default CTE name', () => {
            expect(getLocfCTEName(baseTransform)).toBe('readings_locf');
        });

        it('should return custom CTE name when as is specified', () => {
            expect(getLocfCTEName({ ...baseTransform, as: 'filled_readings' })).toBe('filled_readings');
        });
    });

    describe('timeline-join mode', () => {
        it('should generate SQL with timeline LEFT JOIN', () => {
            const sql = generateLocfRawSQL(baseTransform, 'readings_table', 'timeline_table');

            expect(sql).toContain('FROM timeline_table base');
            expect(sql).toContain('LEFT JOIN readings_table source');
            expect(sql).toContain('base.timestamp = source.timestamp');
            expect(sql).toContain('base.device_id');
            expect(sql).toContain('COALESCE(');
            expect(sql).toContain('AS serial_number');
        });

        it('should include maxLookbackSeconds when specified', () => {
            const transform = { ...baseTransform, maxLookbackSeconds: 300 };
            const sql = generateLocfRawSQL(transform, 'readings', 'timeline');

            expect(sql).toContain("INTERVAL '300 seconds'");
        });

        it('should not include lookback when null', () => {
            const sql = generateLocfRawSQL(baseTransform, 'readings', 'timeline');

            expect(sql).not.toContain('INTERVAL');
        });

        it('should handle multiple join keys', () => {
            const transform = { ...baseTransform, joinKeys: ['device_id', 'channel'] };
            const sql = generateLocfRawSQL(transform, 'readings', 'timeline');

            expect(sql).toContain('base.device_id');
            expect(sql).toContain('base.channel');
            expect(sql).toContain('prev.device_id = base.device_id');
            expect(sql).toContain('prev.channel = base.channel');
        });

        it('should handle multiple LOCF columns', () => {
            const transform = { ...baseTransform, columns: ['serial_number', 'firmware_version'] };
            const sql = generateLocfRawSQL(transform, 'readings', 'timeline');

            expect(sql).toContain('AS serial_number');
            expect(sql).toContain('AS firmware_version');
        });

        it('should wrap in CTE', () => {
            const sql = generateLocfSQL(baseTransform, 'readings', 'timeline');

            expect(sql).toContain('readings_locf AS (');
            expect(sql).toContain('FROM timeline base');
        });
    });

    describe('in-place mode', () => {
        it('should generate SQL without timeline join', () => {
            const sql = generateLocfRawSQL(baseTransform, 'readings_table');

            expect(sql).toContain('FROM readings_table curr');
            expect(sql).not.toContain('LEFT JOIN');
            expect(sql).not.toContain('base.');
            expect(sql).toContain('COALESCE(');
            expect(sql).toContain('AS serial_number');
        });

        it('should use EXCLUDE to avoid duplicate columns', () => {
            const sql = generateLocfRawSQL(baseTransform, 'readings_table');

            expect(sql).toContain('* EXCLUDE (serial_number)');
        });

        it('should exclude multiple columns', () => {
            const transform = { ...baseTransform, columns: ['serial_number', 'firmware'] };
            const sql = generateLocfRawSQL(transform, 'readings_table');

            expect(sql).toContain('* EXCLUDE (serial_number, firmware)');
        });

        it('should include maxLookbackSeconds when specified', () => {
            const transform = { ...baseTransform, maxLookbackSeconds: 600 };
            const sql = generateLocfRawSQL(transform, 'readings');

            expect(sql).toContain("INTERVAL '600 seconds'");
        });

        it('should reference curr alias in correlated subquery', () => {
            const sql = generateLocfRawSQL(baseTransform, 'readings');

            expect(sql).toContain('prev.device_id = curr.device_id');
            expect(sql).toContain('prev.timestamp <= curr.timestamp');
        });

        it('should wrap in CTE', () => {
            const sql = generateLocfSQL(baseTransform, 'readings');

            expect(sql).toContain('readings_locf AS (');
            expect(sql).toContain('FROM readings curr');
            expect(sql).not.toContain('LEFT JOIN');
        });

        it('should use custom CTE name when as is specified', () => {
            const transform = { ...baseTransform, as: 'filled' };
            const sql = generateLocfSQL(transform, 'readings');

            expect(sql).toContain('filled AS (');
        });
    });
});
