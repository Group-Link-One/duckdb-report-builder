/**
 * Logger Integration Tests
 *
 * Verifies that logger events fire correctly during report build lifecycle.
 */

import {
    InMemoryProvider,
    ReportWithContext,
    ReportLogger,
    InitEvent,
    SourceLoadEvent,
    BuildCompleteEvent,
} from 'duckdb-report-builder';

describe('Logger Integration', () => {
    it('should fire init, sourceLoad, and buildComplete events', async () => {
        const events: Array<{ type: string; payload: any }> = [];

        const logger: ReportLogger = {
            onInit: (e) => events.push({ type: 'init', payload: e }),
            onSourceLoad: (e) => events.push({ type: 'sourceLoad', payload: e }),
            onBuildComplete: (e) => events.push({ type: 'buildComplete', payload: e }),
        };

        const data = [
            { device_id: 1n, value: 100 },
            { device_id: 2n, value: 200 },
        ];

        const report = new ReportWithContext()
            .logger(logger)
            .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
            .load('readings', new InMemoryProvider(data, 'readings'))
            .select(['device_id', 'value']);

        await report.build();
        await report.close();

        // Should have: 1 init + 1 sourceLoad + 1 buildComplete
        expect(events.filter(e => e.type === 'init')).toHaveLength(1);
        expect(events.filter(e => e.type === 'sourceLoad')).toHaveLength(1);
        expect(events.filter(e => e.type === 'buildComplete')).toHaveLength(1);

        // Verify init
        const init = events.find(e => e.type === 'init')!.payload as InitEvent;
        expect(init.durationMs).toBeGreaterThanOrEqual(0);

        // Verify sourceLoad
        const src = events.find(e => e.type === 'sourceLoad')!.payload as SourceLoadEvent;
        expect(src.alias).toBe('readings');
        expect(src.provider).toBe('InMemory');
        expect(src.durationMs).toBeGreaterThanOrEqual(0);

        // Verify buildComplete
        const build = events.find(e => e.type === 'buildComplete')!.payload as BuildCompleteEvent;
        expect(build.rows).toBe(2);
        expect(build.totalMs).toBeGreaterThanOrEqual(0);
        expect(build.strategy).toBe('cte');
    });

    it('should fire sourceLoad for each source', async () => {
        const sources: string[] = [];
        const logger: ReportLogger = {
            onSourceLoad: (e) => sources.push(e.alias),
        };

        const report = new ReportWithContext()
            .logger(logger)
            .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
            .load('readings', new InMemoryProvider([{ id: 1n, val: 1 }], 'readings'))
            .load('contexts', new InMemoryProvider([{ id: 1n, name: 'A' }], 'contexts'))
            .select(['readings.id', 'readings.val']);

        await report.build();
        await report.close();

        expect(sources).toEqual(['readings', 'contexts']);
    });

    it('should be silent by default (no errors without logger)', async () => {
        const report = new ReportWithContext()
            .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
            .load('data', new InMemoryProvider([{ x: 1 }], 'data'))
            .select(['x']);

        // Should not throw — default is silentLogger
        const result = await report.build();
        expect(result.data).toHaveLength(1);
        await report.close();
    });

    it('should report temp_tables strategy in buildComplete', async () => {
        let strategy: string | undefined;
        const logger: ReportLogger = {
            onBuildComplete: (e) => { strategy = e.strategy; },
        };

        const report = new ReportWithContext()
            .logger(logger)
            .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
            .load('data', new InMemoryProvider([{ x: 1 }], 'data'))
            .select(['x']);

        await report.build({ strategy: 'temp_tables' });
        await report.close();

        expect(strategy).toBe('temp_tables');
    });
});
