/**
 * CTE Builder Unit Tests
 *
 * Tests for CTE building utilities.
 */

import {
    buildColumnReference, buildCTE,
    buildWithClause, CTE, indent, parseColumnReference, quoteIdentifier
} from '../../../src/sql-generator/cte-builder';

describe('CTE Builder', () => {
    describe('buildCTE', () => {
        it('should build a basic CTE', () => {
            const name = 'my_cte';
            const sql = 'SELECT * FROM table1';
            const result = buildCTE(name, sql);
            expect(result).toContain('my_cte AS (');
            expect(result).toContain('SELECT * FROM table1');
        });

        it('should indent the SQL content', () => {
            const name = 'my_cte';
            const sql = 'SELECT id, name\nFROM users\nWHERE active = true';
            const result = buildCTE(name, sql);
            const lines = result.split('\n');
            // First line should not be indented (the CTE name)
            expect(lines[0]).toBe('my_cte AS (');
            // Content lines should be indented
            expect(lines[1]).toMatch(/^    SELECT/);
        });
    });

    describe('indent', () => {
        it('should indent non-empty lines', () => {
            const sql = 'SELECT *\nFROM table\nWHERE id = 1';
            const result = indent(sql, 4);
            const lines = result.split('\n');
            expect(lines[0]).toBe('    SELECT *');
            expect(lines[1]).toBe('    FROM table');
            expect(lines[2]).toBe('    WHERE id = 1');
        });

        it('should not indent empty lines', () => {
            const sql = 'SELECT *\n\nFROM table';
            const result = indent(sql, 4);
            const lines = result.split('\n');
            expect(lines[0]).toBe('    SELECT *');
            expect(lines[1]).toBe('');
            expect(lines[2]).toBe('    FROM table');
        });

        it('should handle whitespace-only lines as empty', () => {
            const sql = 'SELECT *\n   \nFROM table';
            const result = indent(sql, 2);
            const lines = result.split('\n');
            expect(lines[0]).toBe('  SELECT *');
            expect(lines[1]).toBe('');
            expect(lines[2]).toBe('  FROM table');
        });
    });

    describe('buildWithClause', () => {
        it('should build empty WITH clause for no CTEs', () => {
            const result = buildWithClause([]);
            expect(result).toBe('');
        });

        it('should build WITH clause with single CTE', () => {
            const ctes: CTE[] = [{ name: 'cte1', sql: 'SELECT 1' }];
            const result = buildWithClause(ctes);
            expect(result).toContain('WITH');
            expect(result).toContain('cte1 AS (');
        });

        it('should build WITH clause with multiple CTEs', () => {
            const ctes: CTE[] = [
                { name: 'cte1', sql: 'SELECT 1' },
                { name: 'cte2', sql: 'SELECT 2' },
            ];
            const result = buildWithClause(ctes);
            expect(result).toContain('WITH');
            expect(result).toContain('cte1 AS (');
            expect(result).toContain('cte2 AS (');
            // CTEs should be separated by comma
            expect(result).toContain('),\ncte2');
        });
    });

    describe('quoteIdentifier', () => {
        it('should not quote simple identifiers', () => {
            expect(quoteIdentifier('id')).toBe('id');
            expect(quoteIdentifier('user_name')).toBe('user_name');
            expect(quoteIdentifier('table1')).toBe('table1');
        });

        it('should quote identifiers with uppercase letters', () => {
            expect(quoteIdentifier('UserName')).toBe('UserName');
            expect(quoteIdentifier('TABLE')).toBe('TABLE');
        });

        it('should quote identifiers starting with numbers', () => {
            expect(quoteIdentifier('1column')).toBe('"1column"');
        });

        it('should quote identifiers with special characters', () => {
            expect(quoteIdentifier('column-name')).toBe('"column-name"');
            expect(quoteIdentifier('column.name')).toBe('"column.name"');
            expect(quoteIdentifier('column$name')).toBe('"column$name"');
        });

        it('should quote identifiers with spaces', () => {
            expect(quoteIdentifier('column name')).toBe('"column name"');
        });

        it('should quote identifiers with quotes', () => {
            expect(quoteIdentifier('column"name')).toBe('"column"name"');
        });
    });

    describe('parseColumnReference', () => {
        it('should parse simple column reference', () => {
            const result = parseColumnReference('id');
            expect(result).toEqual({ column: 'id' });
        });

        it('should parse qualified column reference', () => {
            const result = parseColumnReference('users.id');
            expect(result).toEqual({ table: 'users', column: 'id' });
        });

        it('should parse qualified reference with complex table name', () => {
            const result = parseColumnReference('my_schema.table.column');
            // Only handles one level of qualification
            expect(result).toEqual({ table: 'my_schema', column: 'table.column' });
        });

        it('should throw error for invalid column reference', () => {
            expect(() => parseColumnReference('a.b.c.d')).toThrow(/invalid column reference/i);
        });
    });

    describe('buildColumnReference', () => {
        it('should build simple column reference', () => {
            const result = buildColumnReference(undefined, 'id');
            expect(result).toBe('id');
        });

        it('should build qualified column reference', () => {
            const result = buildColumnReference('users', 'id');
            expect(result).toBe('users.id');
        });

        it('should quote identifiers that need it', () => {
            const result = buildColumnReference('my-table', '1column');
            expect(result).toBe('"my-table"."1column"');
        });
    });
});
