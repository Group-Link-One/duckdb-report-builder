/**
 * Timeline Provider Unit Tests
 *
 * Tests for the TimelineProvider.
 */

import { TimelineProvider } from '../../../src/providers/timeline-provider';

describe('TimelineProvider', () => {
    describe('constructor', () => {
        it('should create provider with default granularity', () => {
            const provider = new TimelineProvider();
            expect(provider.name).toBe('Timeline');
            expect(provider.getGranularity()).toBe('hour');
            expect(provider.includesEntityCrossJoin()).toBe(false);
        });

        it('should create provider with custom granularity', () => {
            const provider = new TimelineProvider('minute');
            expect(provider.getGranularity()).toBe('minute');
        });

        it('should create provider with entity cross join', () => {
            const provider = new TimelineProvider('hour', true);
            expect(provider.includesEntityCrossJoin()).toBe(true);
        });

        it('should create provider with custom entity column names', () => {
            const provider = new TimelineProvider('hour', true, 'entity_id', 'channel_id');
            const schema = provider.getSchema();
            expect(schema.some((s) => s.name === 'entity_id')).toBe(true);
            expect(schema.some((s) => s.name === 'channel_id')).toBe(true);
        });
    });

    describe('schema', () => {
        it('should have timestamp column in base schema', () => {
            const provider = new TimelineProvider();
            const schema = provider.getSchema();
            const timestampCol = schema.find((s) => s.name === 'timestamp');
            expect(timestampCol).toBeDefined();
            expect(timestampCol?.type).toBe('TIMESTAMP');
            expect(timestampCol?.nullable).toBe(false);
        });

        it('should not have entity columns without cross join', () => {
            const provider = new TimelineProvider('hour', false);
            const schema = provider.getSchema();
            expect(schema).toHaveLength(1);
            expect(schema[0].name).toBe('timestamp');
        });

        it('should have entity columns with cross join', () => {
            const provider = new TimelineProvider('hour', true);
            const schema = provider.getSchema();
            expect(schema).toHaveLength(3);
            expect(schema.some((s) => s.name === 'device_id')).toBe(true);
            expect(schema.some((s) => s.name === 'channel')).toBe(true);
        });

        it('should have correct entity column types', () => {
            const provider = new TimelineProvider('hour', true);
            const schema = provider.getSchema();
            const deviceIdCol = schema.find((s) => s.name === 'device_id');
            const channelCol = schema.find((s) => s.name === 'channel');
            expect(deviceIdCol?.type).toBe('BIGINT');
            expect(channelCol?.type).toBe('INTEGER');
        });
    });

    describe('BaseDataSourceProvider methods', () => {
        const provider = new TimelineProvider('hour', true);

        describe('getSchema', () => {
            it('should return the schema', () => {
                const schema = provider.getSchema();
                expect(schema.length).toBeGreaterThan(0);
            });
        });

        describe('validateColumns', () => {
            it('should validate existing columns', () => {
                expect(() => provider.validateColumns(['timestamp', 'device_id'])).not.toThrow();
            });

            it('should throw for invalid columns', () => {
                expect(() => provider.validateColumns(['invalid'])).toThrow();
            });
        });

        describe('hasColumn', () => {
            it('should return true for timestamp column', () => {
                expect(provider.hasColumn('timestamp')).toBe(true);
            });

            it('should return true for entity columns when included', () => {
                expect(provider.hasColumn('device_id')).toBe(true);
                expect(provider.hasColumn('channel')).toBe(true);
            });

            it('should return false for non-existing columns', () => {
                expect(provider.hasColumn('invalid')).toBe(false);
            });
        });

        describe('getColumnSchema', () => {
            it('should return schema for timestamp', () => {
                const schema = provider.getColumnSchema('timestamp');
                expect(schema).toBeDefined();
                expect(schema?.type).toBe('TIMESTAMP');
            });

            it('should return undefined for non-existing column', () => {
                const schema = provider.getColumnSchema('invalid');
                expect(schema).toBeUndefined();
            });
        });
    });

    describe('getter methods', () => {
        it('should return granularity via getGranularity', () => {
            const provider = new TimelineProvider('day');
            expect(provider.getGranularity()).toBe('day');
        });

        it('should return entity cross join status via includesEntityCrossJoin', () => {
            const providerWith = new TimelineProvider('hour', true);
            const providerWithout = new TimelineProvider('hour', false);
            expect(providerWith.includesEntityCrossJoin()).toBe(true);
            expect(providerWithout.includesEntityCrossJoin()).toBe(false);
        });

        it('should support deprecated includesDeviceIds method', () => {
            const provider = new TimelineProvider('hour', true);
            expect(provider.includesDeviceIds()).toBe(true);
        });
    });

    describe('granularity variations', () => {
        it('should support minute granularity', () => {
            const provider = new TimelineProvider('minute');
            expect(provider.getGranularity()).toBe('minute');
        });

        it('should support hour granularity', () => {
            const provider = new TimelineProvider('hour');
            expect(provider.getGranularity()).toBe('hour');
        });

        it('should support day granularity', () => {
            const provider = new TimelineProvider('day');
            expect(provider.getGranularity()).toBe('day');
        });
    });
});
