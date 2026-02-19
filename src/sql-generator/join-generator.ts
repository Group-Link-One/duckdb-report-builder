/**
 * Join SQL Generator
 *
 * Generates SQL for multi-source joins.
 *
 * Example:
 *   Input:  readings (device_id, timestamp, energy_in)
 *           device_context (device_id, channel, device_name)
 *
 *   Output: readings LEFT JOIN device_context
 *           ON readings.device_id = device_context.device_id
 *           AND readings.channel = device_context.channel
 */

import { JoinTransform } from '../query-plan/query-plan';
import { quoteIdentifier } from './cte-builder';

/**
 * Generate JOIN clause for a join transform
 *
 * @param transform - Join transform specification
 * @param leftTable - Left table or CTE name
 * @param rightTable - Right table or CTE name
 * @returns SQL JOIN clause
 */
export function generateJoinClause(transform: JoinTransform, leftTable: string, rightTable: string): string {
    const joinType = transform.joinType;
    const leftAlias = transform.leftAlias;
    const rightAlias = transform.rightAlias;

    // Build ON conditions
    const onConditions = transform.onConditions
        .map((cond) => {
            const leftCol = `${quoteIdentifier(leftAlias)}.${quoteIdentifier(cond.left)}`;
            const rightCol = `${quoteIdentifier(rightAlias)}.${quoteIdentifier(cond.right)}`;
            return `${leftCol} = ${rightCol}`;
        })
        .join('\n    AND ');

    return `
${joinType} JOIN ${quoteIdentifier(rightTable)} AS ${quoteIdentifier(rightAlias)}
    ON ${onConditions}
    `.trim();
}

/**
 * Generate complete FROM clause with joins
 *
 * @param baseTable - Base table name
 * @param baseAlias - Base table alias
 * @param joins - Array of join transforms
 * @param tableMap - Map of alias to table name
 * @returns SQL FROM clause with all joins
 */
export function generateFromClauseWithJoins(
    baseTable: string,
    baseAlias: string,
    joins: JoinTransform[],
    tableMap: Map<string, string>
): string {
    let fromClause = `FROM ${quoteIdentifier(baseTable)} AS ${quoteIdentifier(baseAlias)}`;

    for (const join of joins) {
        const rightTable = tableMap.get(join.rightAlias) || join.rightAlias;
        const joinClause = generateJoinClause(join, baseTable, rightTable);
        fromClause += '\n' + joinClause;
    }

    return fromClause;
}

/**
 * Build a JOIN tree from multiple join transforms
 *
 * This function analyzes join dependencies and builds an optimal join order.
 * For now, it uses a simple left-to-right approach.
 *
 * @param joins - Array of join transforms
 * @returns Ordered array of joins
 */
export function buildJoinTree(joins: JoinTransform[]): JoinTransform[] {
    // For now, return joins as-is
    // In the future, this could optimize join order based on selectivity
    return joins;
}

/**
 * Validate join transforms
 *
 * Ensures that:
 * 1. All referenced tables exist
 * 2. Join conditions are valid
 * 3. No circular dependencies
 *
 * @param joins - Array of join transforms
 * @param availableTables - Set of available table/CTE names
 * @throws Error if validation fails
 */
export function validateJoins(joins: JoinTransform[], availableTables: Set<string>): void {
    for (const join of joins) {
        if (!availableTables.has(join.leftAlias)) {
            throw new Error(`Join references unknown left table: ${join.leftAlias}`);
        }
        if (!availableTables.has(join.rightAlias)) {
            throw new Error(`Join references unknown right table: ${join.rightAlias}`);
        }
        if (join.onConditions.length === 0) {
            throw new Error(`Join between ${join.leftAlias} and ${join.rightAlias} has no ON conditions`);
        }
    }

    // Check for circular dependencies
    // Build adjacency list
    const adjacency = new Map<string, Set<string>>();
    for (const join of joins) {
        if (!adjacency.has(join.leftAlias)) {
            adjacency.set(join.leftAlias, new Set());
        }
        adjacency.get(join.leftAlias)!.add(join.rightAlias);
    }

    // DFS to detect cycles
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    function hasCycle(node: string): boolean {
        visited.add(node);
        recursionStack.add(node);

        const neighbors = adjacency.get(node) || new Set();
        for (const neighbor of neighbors) {
            if (!visited.has(neighbor)) {
                if (hasCycle(neighbor)) {
                    return true;
                }
            } else if (recursionStack.has(neighbor)) {
                return true;
            }
        }

        recursionStack.delete(node);
        return false;
    }

    for (const table of availableTables) {
        if (!visited.has(table)) {
            if (hasCycle(table)) {
                throw new Error('Circular dependency detected in joins');
            }
        }
    }
}

/**
 * Generate SQL for a simple two-table join
 *
 * @param leftTable - Left table name
 * @param leftAlias - Left table alias
 * @param rightTable - Right table name
 * @param rightAlias - Right table alias
 * @param joinType - Type of join (INNER, LEFT, RIGHT, FULL)
 * @param onConditions - Array of join conditions
 * @returns SQL for the join
 */
export function generateSimpleJoinSQL(
    leftTable: string,
    leftAlias: string,
    rightTable: string,
    rightAlias: string,
    joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL',
    onConditions: Array<{ left: string; right: string }>
): string {
    const onClause = onConditions
        .map((cond) => {
            const leftCol = `${quoteIdentifier(leftAlias)}.${quoteIdentifier(cond.left)}`;
            const rightCol = `${quoteIdentifier(rightAlias)}.${quoteIdentifier(cond.right)}`;
            return `${leftCol} = ${rightCol}`;
        })
        .join(' AND ');

    return `
${quoteIdentifier(leftTable)} AS ${quoteIdentifier(leftAlias)}
${joinType} JOIN ${quoteIdentifier(rightTable)} AS ${quoteIdentifier(rightAlias)}
    ON ${onClause}
    `.trim();
}
