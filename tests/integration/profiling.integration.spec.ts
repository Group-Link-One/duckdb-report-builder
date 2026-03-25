/**
 * Profiling Integration Tests
 *
 * Verifies that profiling collects memory, timing, and row count data
 * for both CTE and temp_tables execution strategies.
 */

import {
    InMemoryProvider,
    ReportWithContext,
    ReportLogger,
    type ProfileResult,
    type ProfileCompleteEvent,
} from 'duckdb-report-builder';

function makeTestData() {
    return [
        { device_id: 1n, channel: 0, serie_id: 2, raw_value: 100, timestamp: new Date('2024-01-01T10:00:00Z') },
        { device_id: 1n, channel: 0, serie_id: 3, raw_value: 50, timestamp: new Date('2024-01-01T10:00:00Z') },
        { device_id: 1n, channel: 0, serie_id: 2, raw_value: 110, timestamp: new Date('2024-01-01T11:00:00Z') },
        { device_id: 1n, channel: 0, serie_id: 3, raw_value: 55, timestamp: new Date('2024-01-01T11:00:00Z') },
    ];
}

function makeContextData() {
    return [
        { device_id: 1n, channel: 0, multiplier: 2.0 },
    ];
}

function buildReport(logger?: ReportLogger) {
    const report = new ReportWithContext();
    if (logger) report.logger(logger);

    return report
        .context({ from: new Date('2024-01-01'), until: new Date('2024-01-02'), timezone: 'UTC' })
        .load('readings', new InMemoryProvider(makeTestData(), 'readings'))
        .load('contexts', new InMemoryProvider(makeContextData(), 'contexts'))
        .pivot('readings', {
            on: 'serie_id',
            val: 'raw_value',
            groupBy: ['device_id', 'channel', 'timestamp'],
            cols: [
                { id: 2, alias: 'energy_in' },
                { id: 3, alias: 'energy_out' },
            ],
        })
        .applyEnrichment('readings', {
            lookupSource: 'contexts',
            joinOn: ['device_id', 'channel'],
            formulas: {
                adjusted_in: { formula: 'r.energy_in * c.multiplier' },
            },
        })
        .select(['device_id', 'timestamp', 'energy_in', 'energy_out', 'adjusted_in']);
}

describe('Profiling Integration', () => {
    describe('CTE mode', () => {
        it('should return profile with queryProfile when profiling is enabled', async () => {
            const report = buildReport();
            const result = await report.build({ profiling: true });
            await report.close();

            expect(result.profile).toBeDefined();
            const profile = result.profile!;

            expect(profile.strategy).toBe('cte');
            expect(profile.totalRows).toBe(result.data.length);
            expect(profile.totalDurationMs).toBeGreaterThanOrEqual(0);
            expect(profile.memoryPeakBytes).toBeGreaterThanOrEqual(0);

            // CTE mode has queryProfile, not steps
            expect(profile.queryProfile).toBeDefined();
            expect(profile.steps).toBeUndefined();

            const qp = profile.queryProfile!;
            expect(qp.explainAnalyze).toBeTruthy();
            expect(qp.rowCount).toBe(result.data.length);
            expect(qp.memoryBefore).toBeInstanceOf(Array);
            expect(qp.memoryAfter).toBeInstanceOf(Array);
            expect(typeof qp.memoryDeltaBytes).toBe('number');
        });

        it('should emit onProfileComplete event', async () => {
            let profileEvent: ProfileCompleteEvent | undefined;
            const logger: ReportLogger = {
                onProfileComplete: (e) => { profileEvent = e; },
            };

            const report = buildReport(logger);
            await report.build({ profiling: true });
            await report.close();

            expect(profileEvent).toBeDefined();
            expect(profileEvent!.profile.strategy).toBe('cte');
            expect(profileEvent!.profile.queryProfile).toBeDefined();
        });

        it('should NOT return profile when profiling is disabled', async () => {
            const report = buildReport();
            const result = await report.build();
            await report.close();

            expect(result.profile).toBeUndefined();
        });

        it('should NOT emit onProfileComplete when profiling is disabled', async () => {
            let called = false;
            const logger: ReportLogger = {
                onProfileComplete: () => { called = true; },
            };

            const report = buildReport(logger);
            await report.build();
            await report.close();

            expect(called).toBe(false);
        });
    });

    describe('temp_tables mode', () => {
        it('should return profile with steps when profiling is enabled', async () => {
            const report = buildReport();
            const result = await report.build({ strategy: 'temp_tables', profiling: true });
            await report.close();

            expect(result.profile).toBeDefined();
            const profile = result.profile!;

            expect(profile.strategy).toBe('temp_tables');
            expect(profile.totalRows).toBe(result.data.length);
            expect(profile.totalDurationMs).toBeGreaterThanOrEqual(0);
            expect(profile.memoryPeakBytes).toBeGreaterThanOrEqual(0);

            // temp_tables mode has steps, not queryProfile
            expect(profile.steps).toBeDefined();
            expect(profile.queryProfile).toBeUndefined();

            const steps = profile.steps!;
            expect(steps.length).toBeGreaterThan(0);

            for (const step of steps) {
                expect(step.name).toBeTruthy();
                expect(step.tableName).toBeTruthy();
                expect(typeof step.stepNumber).toBe('number');
                expect(step.durationMs).toBeGreaterThanOrEqual(0);
                expect(step.rowCount).toBeGreaterThanOrEqual(0);
                expect(step.memoryBefore).toBeInstanceOf(Array);
                expect(step.memoryAfter).toBeInstanceOf(Array);
                expect(typeof step.memoryDeltaBytes).toBe('number');
                expect(step.sql).toBeTruthy();
            }
        });

        it('should emit onProfileComplete event with steps', async () => {
            let profileEvent: ProfileCompleteEvent | undefined;
            const logger: ReportLogger = {
                onProfileComplete: (e) => { profileEvent = e; },
            };

            const report = buildReport(logger);
            await report.build({ strategy: 'temp_tables', profiling: true });
            await report.close();

            expect(profileEvent).toBeDefined();
            expect(profileEvent!.profile.strategy).toBe('temp_tables');
            expect(profileEvent!.profile.steps!.length).toBeGreaterThan(0);
        });

        it('should have step row counts matching actual table contents', async () => {
            const report = buildReport();
            const result = await report.build({ strategy: 'temp_tables', profiling: true });
            await report.close();

            const steps = result.profile!.steps!;
            // The pivot step should produce 2 rows (2 unique timestamps)
            const pivotStep = steps.find(s => s.name === 'transform:pivot');
            expect(pivotStep).toBeDefined();
            expect(pivotStep!.rowCount).toBe(2);
        });

        it('should NOT return profile when profiling is disabled', async () => {
            const report = buildReport();
            const result = await report.build({ strategy: 'temp_tables' });
            await report.close();

            expect(result.profile).toBeUndefined();
        });

        it('should work alongside onStep callback', async () => {
            const stepNames: string[] = [];
            const report = buildReport();
            const result = await report.build({
                strategy: 'temp_tables',
                profiling: true,
                onStep: async (info) => {
                    stepNames.push(info.name);
                },
            });
            await report.close();

            // Both profiling and onStep should work
            expect(result.profile).toBeDefined();
            expect(result.profile!.steps!.length).toBe(stepNames.length);
        });
    });

    describe('comparison between strategies', () => {
        it('should produce comparable data from both strategies', async () => {
            const reportCTE = buildReport();
            const cteResult = await reportCTE.build({ profiling: true });
            await reportCTE.close();

            const reportTmp = buildReport();
            const tmpResult = await reportTmp.build({ strategy: 'temp_tables', profiling: true });
            await reportTmp.close();

            // Both should return the same data
            expect(cteResult.data.length).toBe(tmpResult.data.length);

            // Both profiles should have memory data
            expect(cteResult.profile!.memoryPeakBytes).toBeGreaterThanOrEqual(0);
            expect(tmpResult.profile!.memoryPeakBytes).toBeGreaterThanOrEqual(0);

            // Both should have totalRows
            expect(cteResult.profile!.totalRows).toBe(tmpResult.profile!.totalRows);
        });
    });
});
