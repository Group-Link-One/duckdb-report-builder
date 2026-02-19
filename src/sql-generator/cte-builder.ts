/**
 * CTE Builder Utilities
 *
 * Helper functions for constructing Common Table Expressions (CTEs)
 */

/**
 * CTE definition
 */
export interface CTE {
    name: string;
    sql: string;
}

/**
 * Build a CTE string from name and SQL
 */
export function buildCTE(name: string, sql: string): string {
    return `${name} AS (\n${indent(sql, 4)}\n)`;
}

/**
 * Indent SQL string by a number of spaces
 */
export function indent(sql: string, spaces: number): string {
    const indentation = ' '.repeat(spaces);
    return sql
        .split('\n')
        .map((line) => (line.trim() ? indentation + line : ''))
        .join('\n');
}

/**
 * Build a WITH clause from an array of CTEs
 */
export function buildWithClause(ctes: CTE[]): string {
    if (ctes.length === 0) {
        return '';
    }

    const cteStrings = ctes.map((cte) => buildCTE(cte.name, cte.sql));
    return 'WITH\n' + cteStrings.join(',\n');
}

/**
 * Quote an identifier if needed
 */
export function quoteIdentifier(identifier: string): string {
    // Only quote if contains special characters
    if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(identifier)) {
        return identifier;
    }
    return `"${identifier}"`;
}

/**
 * Parse a possibly-qualified column reference (e.g., "table.column")
 */
export function parseColumnReference(ref: string): { table?: string; column: string } {
    const parts = ref.split('.');
    if (parts.length === 1) {
        return { column: parts[0] };
    } else if (parts.length === 2) {
        return { table: parts[0], column: parts[1] };
    } else if (parts.length === 3) {
        // For 3 parts, treat first as table, rest as column (table.col1.col2)
        return { table: parts[0], column: `${parts[1]}.${parts[2]}` };
    } else {
        throw new Error(`Invalid column reference: ${ref}`);
    }
}

/**
 * Build a qualified column reference
 */
export function buildColumnReference(table: string | undefined, column: string): string {
    if (table) {
        return `${quoteIdentifier(table)}.${quoteIdentifier(column)}`;
    }
    return quoteIdentifier(column);
}
