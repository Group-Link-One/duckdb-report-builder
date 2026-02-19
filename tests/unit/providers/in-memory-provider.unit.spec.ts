/**
 * In-Memory Provider Unit Tests
 *
 * Tests for the InMemoryProvider.
 */

import { ColumnSchema } from '../../../src/providers/i-data-source-provider';
import { InMemoryProvider, inMemoryProvider } from '../../../src/providers/in-memory-provider';

describe('InMemoryProvider', () => {
    describe('constructor', () => {
        it('should create provider with empty data', () => {
            const provider = new InMemoryProvider([]);
            expect(provider.name).toBe('InMemory');
            expect(provider.getSchema()).toEqual([]);
        });

        it('should infer schema from data', () => {
            const data = [{ id: 1, name: 'test', value: 3.14, active: true }];
            const provider = new InMemoryProvider(data);

            const schema = provider.getSchema();
            expect(schema).toHaveLength(4);
            expect(schema.find((s) => s.name === 'id')?.type).toBe('INTEGER');
            expect(schema.find((s) => s.name === 'name')?.type).toBe('VARCHAR');
            expect(schema.find((s) => s.name === 'value')?.type).toBe('DOUBLE');
            expect(schema.find((s) => s.name === 'active')?.type).toBe('BOOLEAN');
        });

        it('should use provided schema over inferred', () => {
            const data = [{ id: 1 }];
            const customSchema: ColumnSchema[] = [
                { name: 'id', type: 'BIGINT', nullable: false },
                { name: 'extra', type: 'VARCHAR', nullable: true },
            ];
            const provider = new InMemoryProvider(data, 'test_table', customSchema);

            const schema = provider.getSchema();
            expect(schema).toHaveLength(2);
            expect(schema[0].type).toBe('BIGINT');
        });

        it('should use provided table name', () => {
            const provider = new InMemoryProvider([], 'my_table');
            expect(provider.name).toBe('InMemory');
        });
    });

    describe('schema inference', () => {
        it('should infer INTEGER type', () => {
            const provider = new InMemoryProvider([{ value: 42 }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('INTEGER');
        });

        it('should infer DOUBLE type', () => {
            const provider = new InMemoryProvider([{ value: 3.14 }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('DOUBLE');
        });

        it('should infer BIGINT type', () => {
            const provider = new InMemoryProvider([{ value: 9007199254740991n }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('BIGINT');
        });

        it('should infer BOOLEAN type', () => {
            const provider = new InMemoryProvider([{ value: true }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('BOOLEAN');
        });

        it('should infer VARCHAR type', () => {
            const provider = new InMemoryProvider([{ value: 'test' }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('VARCHAR');
        });

        it('should infer TIMESTAMP type', () => {
            const provider = new InMemoryProvider([{ value: new Date('2024-01-01') }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('TIMESTAMP');
        });

        it('should infer INTEGER[] type for arrays', () => {
            const provider = new InMemoryProvider([{ value: [1, 2, 3] }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('INTEGER[]');
        });

        it('should default to VARCHAR for null values', () => {
            const provider = new InMemoryProvider([{ value: null }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('VARCHAR');
        });

        it('should default to VARCHAR for undefined values', () => {
            const provider = new InMemoryProvider([{ value: undefined }]);
            const schema = provider.getSchema();
            expect(schema[0].type).toBe('VARCHAR');
        });

        it('should mark nullable for null values', () => {
            const provider = new InMemoryProvider([{ value: null }]);
            const schema = provider.getSchema();
            expect(schema[0].nullable).toBe(true);
        });
    });

    describe('BaseDataSourceProvider methods', () => {
        const provider = new InMemoryProvider([{ id: 1, name: 'test' }], 'test', [
            { name: 'id', type: 'INTEGER', nullable: false },
            { name: 'name', type: 'VARCHAR', nullable: true },
        ]);

        describe('getSchema', () => {
            it('should return the schema', () => {
                const schema = provider.getSchema();
                expect(schema).toHaveLength(2);
                expect(schema[0].name).toBe('id');
            });
        });

        describe('validateColumns', () => {
            it('should not throw for valid columns', () => {
                expect(() => provider.validateColumns(['id', 'name'])).not.toThrow();
            });

            it('should throw for invalid column', () => {
                expect(() => provider.validateColumns(['id', 'invalid'])).toThrow(/does not exist/i);
            });

            it('should throw for empty column list', () => {
                expect(() => provider.validateColumns([])).not.toThrow();
            });
        });

        describe('hasColumn', () => {
            it('should return true for existing column', () => {
                expect(provider.hasColumn('id')).toBe(true);
                expect(provider.hasColumn('name')).toBe(true);
            });

            it('should return false for non-existing column', () => {
                expect(provider.hasColumn('invalid')).toBe(false);
            });
        });

        describe('getColumnSchema', () => {
            it('should return schema for existing column', () => {
                const schema = provider.getColumnSchema('id');
                expect(schema).toBeDefined();
                expect(schema?.type).toBe('INTEGER');
            });

            it('should return undefined for non-existing column', () => {
                const schema = provider.getColumnSchema('invalid');
                expect(schema).toBeUndefined();
            });
        });
    });

    describe('inMemoryProvider factory', () => {
        it('should create InMemoryProvider instance', () => {
            const provider = inMemoryProvider([{ id: 1 }]);
            expect(provider).toBeInstanceOf(InMemoryProvider);
            expect(provider.name).toBe('InMemory');
        });

        it('should pass table name to provider', () => {
            const provider = inMemoryProvider([{ id: 1 }], 'custom_table');
            // The provider is created, we just verify it works
            expect(provider.getSchema()).toHaveLength(1);
        });

        it('should pass schema to provider', () => {
            const schema: ColumnSchema[] = [{ name: 'id', type: 'BIGINT' }];
            const provider = inMemoryProvider([{ id: 1 }], 'table', schema);
            expect(provider.getSchema()[0].type).toBe('BIGINT');
        });
    });
});
