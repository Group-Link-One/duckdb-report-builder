/**
 * Join Generator Unit Tests
 *
 * Tests for join SQL generation.
 */

import { JoinTransform } from '../../../src/query-plan/query-plan';
import {
    buildJoinTree, generateFromClauseWithJoins, generateJoinClause, generateSimpleJoinSQL, validateJoins
} from '../../../src/sql-generator/join-generator';

describe('Join Generator', () => {
    describe('generateJoinClause', () => {
        it('should generate LEFT JOIN clause', () => {
            const transform: JoinTransform = {
                type: 'join',
                leftAlias: 'readings',
                rightAlias: 'context',
                joinType: 'LEFT',
                onConditions: [{ left: 'device_id', right: 'device_id' }],
            };

            const sql = generateJoinClause(transform, 'readings_table', 'context_table');

            expect(sql).toContain('LEFT JOIN context_table AS context');
            expect(sql).toContain('ON readings.device_id = context.device_id');
        });

        it('should generate INNER JOIN clause', () => {
            const transform: JoinTransform = {
                type: 'join',
                leftAlias: 'orders',
                rightAlias: 'customers',
                joinType: 'INNER',
                onConditions: [{ left: 'customer_id', right: 'id' }],
            };

            const sql = generateJoinClause(transform, 'orders_table', 'customers_table');

            expect(sql).toContain('INNER JOIN customers_table AS customers');
            expect(sql).toContain('ON orders.customer_id = customers.id');
        });

        it('should generate multiple join conditions', () => {
            const transform: JoinTransform = {
                type: 'join',
                leftAlias: 'readings',
                rightAlias: 'context',
                joinType: 'LEFT',
                onConditions: [
                    { left: 'device_id', right: 'device_id' },
                    { left: 'channel', right: 'channel' },
                ],
            };

            const sql = generateJoinClause(transform, 'readings_table', 'context_table');

            expect(sql).toContain('readings.device_id = context.device_id');
            expect(sql).toContain('readings.channel = context.channel');
            expect(sql).toContain('AND');
        });

        it('should generate RIGHT JOIN clause', () => {
            const transform: JoinTransform = {
                type: 'join',
                leftAlias: 'a',
                rightAlias: 'b',
                joinType: 'RIGHT',
                onConditions: [{ left: 'id', right: 'id' }],
            };

            const sql = generateJoinClause(transform, 'a_table', 'b_table');
            expect(sql).toContain('RIGHT JOIN');
        });

        it('should generate FULL JOIN clause', () => {
            const transform: JoinTransform = {
                type: 'join',
                leftAlias: 'a',
                rightAlias: 'b',
                joinType: 'FULL',
                onConditions: [{ left: 'id', right: 'id' }],
            };

            const sql = generateJoinClause(transform, 'a_table', 'b_table');
            expect(sql).toContain('FULL JOIN');
        });
    });

    describe('generateFromClauseWithJoins', () => {
        it('should generate FROM clause with single join', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'readings',
                    rightAlias: 'context',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'device_id', right: 'device_id' }],
                },
            ];

            const tableMap = new Map<string, string>([
                ['readings', 'readings_table'],
                ['context', 'context_table'],
            ]);

            const sql = generateFromClauseWithJoins('readings_table', 'readings', joins, tableMap);

            expect(sql).toContain('FROM readings_table AS readings');
            expect(sql).toContain('LEFT JOIN context_table AS context');
        });

        it('should generate FROM clause with multiple joins', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'orders',
                    rightAlias: 'customers',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'customer_id', right: 'id' }],
                },
                {
                    type: 'join',
                    leftAlias: 'orders',
                    rightAlias: 'products',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'product_id', right: 'id' }],
                },
            ];

            const tableMap = new Map<string, string>([
                ['orders', 'orders_table'],
                ['customers', 'customers_table'],
                ['products', 'products_table'],
            ]);

            const sql = generateFromClauseWithJoins('orders_table', 'orders', joins, tableMap);

            expect(sql).toContain('FROM orders_table AS orders');
            expect(sql).toContain('customers_table AS customers');
            expect(sql).toContain('products_table AS products');
        });
    });

    describe('buildJoinTree', () => {
        it('should return joins as-is (simple left-to-right order)', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'a',
                    rightAlias: 'b',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'id', right: 'id' }],
                },
                {
                    type: 'join',
                    leftAlias: 'a',
                    rightAlias: 'c',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'id', right: 'id' }],
                },
            ];

            const result = buildJoinTree(joins);
            expect(result).toEqual(joins);
        });

        it('should handle empty joins array', () => {
            const result = buildJoinTree([]);
            expect(result).toEqual([]);
        });
    });

    describe('validateJoins', () => {
        it('should validate valid joins', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'readings',
                    rightAlias: 'context',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'device_id', right: 'device_id' }],
                },
            ];

            const availableTables = new Set<string>(['readings', 'context']);

            expect(() => validateJoins(joins, availableTables)).not.toThrow();
        });

        it('should throw error for unknown left table', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'unknown',
                    rightAlias: 'context',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'id', right: 'id' }],
                },
            ];

            const availableTables = new Set<string>(['context']);

            expect(() => validateJoins(joins, availableTables)).toThrow(/unknown left table/i);
        });

        it('should throw error for unknown right table', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'readings',
                    rightAlias: 'unknown',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'id', right: 'id' }],
                },
            ];

            const availableTables = new Set<string>(['readings']);

            expect(() => validateJoins(joins, availableTables)).toThrow(/unknown right table/i);
        });

        it('should throw error for join with no conditions', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'a',
                    rightAlias: 'b',
                    joinType: 'LEFT',
                    onConditions: [],
                },
            ];

            const availableTables = new Set<string>(['a', 'b']);

            expect(() => validateJoins(joins, availableTables)).toThrow(/no on conditions/i);
        });

        it('should throw error for circular dependencies', () => {
            const joins: JoinTransform[] = [
                {
                    type: 'join',
                    leftAlias: 'a',
                    rightAlias: 'b',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'id', right: 'id' }],
                },
                {
                    type: 'join',
                    leftAlias: 'b',
                    rightAlias: 'a',
                    joinType: 'LEFT',
                    onConditions: [{ left: 'id', right: 'id' }],
                },
            ];

            const availableTables = new Set<string>(['a', 'b']);

            expect(() => validateJoins(joins, availableTables)).toThrow(/circular dependency/i);
        });
    });

    describe('generateSimpleJoinSQL', () => {
        it('should generate simple join SQL', () => {
            const sql = generateSimpleJoinSQL('orders_table', 'orders', 'customers_table', 'customers', 'LEFT', [
                { left: 'customer_id', right: 'id' },
            ]);

            expect(sql).toContain('orders_table AS orders');
            expect(sql).toContain('LEFT JOIN customers_table AS customers');
            expect(sql).toContain('orders.customer_id = customers.id');
        });

        it('should handle multiple conditions', () => {
            const sql = generateSimpleJoinSQL('readings_table', 'r', 'context_table', 'c', 'INNER', [
                { left: 'device_id', right: 'device_id' },
                { left: 'channel', right: 'channel' },
            ]);

            expect(sql).toContain('INNER JOIN');
            expect(sql).toContain('r.device_id = c.device_id');
            expect(sql).toContain('r.channel = c.channel');
            expect(sql).toContain(' AND ');
        });
    });
});
