/**
 * Timeline Data Source Provider
 *
 * Generates complete timelines (hourly/daily) for LOCF gap filling.
 * This provider creates a series of timestamps at regular intervals,
 * which can be used as a base timeline to fill gaps in sparse data.
 */

import { BaseDataSourceProvider, ColumnSchema, LoadContext } from './i-data-source-provider';

/**
 * Timeline granularity
 */
export type TimelineGranularity = 'minute' | 'hour' | 'day';

/**
 * Timeline Provider
 *
 * Generates a complete timeline at specified granularity
 */
export class TimelineProvider extends BaseDataSourceProvider {
    readonly name = 'Timeline';

    constructor(
        private granularity: TimelineGranularity = 'hour',
        private includeEntityCrossJoin: boolean = false,
        private entityIdColumn: string = 'device_id',
        private channelColumn: string = 'channel'
    ) {
        super();

        // Define schema
        const baseSchema: ColumnSchema[] = [
            {
                name: 'timestamp',
                type: 'TIMESTAMP',
                nullable: false,
                description: 'Timeline timestamp',
            },
        ];

        // If includeEntityCrossJoin is true, add entity ID and channel columns
        if (includeEntityCrossJoin) {
            baseSchema.push({
                name: this.entityIdColumn,
                type: 'BIGINT',
                nullable: false,
                description: 'Entity identifier (cartesian product with timeline)',
            });
            baseSchema.push({
                name: this.channelColumn,
                type: 'INTEGER',
                nullable: false,
                description: 'Channel identifier (0 if not specified)',
            });
        }

        this.schema = baseSchema;
    }

    /**
     * Load timeline into DuckDB
     *
     * Generates a series of timestamps at the specified granularity
     */
    async load(context: LoadContext): Promise<string> {
        const connection = context.connection;
        const tableName = this.generateTableName('timeline');

        // Determine interval based on granularity
        const interval = this.getInterval();

        if (this.includeEntityCrossJoin) {
            // Try to find a previously loaded entity table in context.tables
            let entityTable: string | null = null;
            for (const [alias, table] of context.tables) {
                // Look for tables that might contain entity data (e.g., device_context, entities, etc.)
                if (
                    alias.toLowerCase().includes('context') ||
                    alias.toLowerCase().includes('device') ||
                    alias.toLowerCase().includes('entity')
                ) {
                    entityTable = table;
                    break;
                }
            }

            if (entityTable) {
                // Use pre-loaded entity table from context
                await connection.run(`
                    CREATE OR REPLACE TEMPORARY TABLE ${tableName} AS
                    WITH timeline AS (
                        SELECT UNNEST(generate_series(
                            TIMESTAMP '${context.period.from.toISOString()}',
                            TIMESTAMP '${context.period.until.toISOString()}',
                            INTERVAL '${interval}'
                        )) AS timestamp
                    )
                    SELECT
                        t.timestamp,
                        d.${this.entityIdColumn},
                        COALESCE(d.${this.channelColumn}, 0) AS ${this.channelColumn}
                    FROM timeline t
                    CROSS JOIN ${entityTable} d
                    ORDER BY d.${this.entityIdColumn}, d.${this.channelColumn}, t.timestamp;
                `);
            } else {
                // Generate inline entities CTE from context.params
                const entityIds = context.params.entityIds || context.params.deviceIds || [];
                if (!Array.isArray(entityIds) || entityIds.length === 0) {
                    throw new Error(
                        'TimelineProvider with includeEntityCrossJoin=true requires either:\n' +
                            '  1. A previously loaded table with entity data in context.tables, OR\n' +
                            '  2. entityIds or deviceIds array in context.params'
                    );
                }
                await connection.run(`
                    CREATE OR REPLACE TEMPORARY TABLE ${tableName} AS
                    WITH timeline AS (
                        SELECT UNNEST(generate_series(
                            TIMESTAMP '${context.period.from.toISOString()}',
                            TIMESTAMP '${context.period.until.toISOString()}',
                            INTERVAL '${interval}'
                        )) AS timestamp
                    ),
                    entities AS (
                        SELECT
                            UNNEST([${entityIds.map((id: any) => Number(id)).join(', ')}]::BIGINT[]) AS ${this.entityIdColumn},
                            0 AS ${this.channelColumn}
                    )
                    SELECT
                        t.timestamp,
                        e.${this.entityIdColumn},
                        e.${this.channelColumn}
                    FROM timeline t
                    CROSS JOIN entities e
                    ORDER BY e.${this.entityIdColumn}, e.${this.channelColumn}, t.timestamp;
                `);
            }
        } else {
            // Generate simple timeline
            await connection.run(`
                CREATE OR REPLACE TEMPORARY TABLE ${tableName} AS
                SELECT UNNEST(generate_series(
                    TIMESTAMP '${context.period.from.toISOString()}',
                    TIMESTAMP '${context.period.until.toISOString()}',
                    INTERVAL '${interval}'
                )) AS timestamp
                ORDER BY timestamp;
            `);
        }

        // Emit provider event via logger (if available)
        if (context.logger?.onProviderEvent) {
            const countResult = await connection.run(`SELECT COUNT(*) as count FROM ${tableName}`);
            const columnNames = Array.from({ length: countResult.columnCount }, (_, i) => countResult.columnName(i));
            const chunk = countResult.getChunk(0);
            const rows = chunk.getRowObjects(columnNames);
            const count = rows[0]?.count || 0;
            context.logger.onProviderEvent({
                provider: this.name,
                tableName,
                message: `Generated ${count} entries at ${this.granularity} granularity`,
            });
        }

        return tableName;
    }

    /**
     * Get interval string for DuckDB based on granularity
     */
    private getInterval(): string {
        switch (this.granularity) {
            case 'minute':
                return '1 minute';
            case 'hour':
                return '1 hour';
            case 'day':
                return '1 day';
            default:
                throw new Error(`Unknown granularity: ${this.granularity}`);
        }
    }

    /**
     * Get granularity
     */
    getGranularity(): TimelineGranularity {
        return this.granularity;
    }

    /**
     * Check if timeline includes entity cross join
     */
    includesEntityCrossJoin(): boolean {
        return this.includeEntityCrossJoin;
    }

    /**
     * @deprecated Use includesEntityCrossJoin() instead
     */
    includesDeviceIds(): boolean {
        return this.includeEntityCrossJoin;
    }
}
