/**
 * Logger Unit Tests
 *
 * Tests for the pluggable observability layer.
 */

import { consoleLogger, silentLogger, ReportLogger } from '../../../src/core/logger';

describe('Logger', () => {
    describe('silentLogger', () => {
        it('should have no methods defined', () => {
            expect(silentLogger.onInit).toBeUndefined();
            expect(silentLogger.onSourceLoad).toBeUndefined();
            expect(silentLogger.onBuildComplete).toBeUndefined();
            expect(silentLogger.onProviderEvent).toBeUndefined();
        });

        it('should be frozen', () => {
            expect(Object.isFrozen(silentLogger)).toBe(true);
        });
    });

    describe('consoleLogger', () => {
        it('should implement all logger methods', () => {
            const logger = consoleLogger();
            expect(typeof logger.onInit).toBe('function');
            expect(typeof logger.onSourceLoad).toBe('function');
            expect(typeof logger.onBuildComplete).toBe('function');
            expect(typeof logger.onProviderEvent).toBe('function');
        });

        it('should log init event', () => {
            const spy = jest.spyOn(console, 'log').mockImplementation();
            const logger = consoleLogger();
            logger.onInit({ durationMs: 42 });
            expect(spy).toHaveBeenCalledWith('[report-builder] init: 42ms');
            spy.mockRestore();
        });

        it('should log source load event', () => {
            const spy = jest.spyOn(console, 'log').mockImplementation();
            const logger = consoleLogger();
            logger.onSourceLoad({ alias: 'readings', provider: 'ClickHouse', durationMs: 850 });
            expect(spy).toHaveBeenCalledWith('[report-builder] load "readings" (ClickHouse): 850ms');
            spy.mockRestore();
        });

        it('should log build complete event', () => {
            const spy = jest.spyOn(console, 'log').mockImplementation();
            const logger = consoleLogger();
            logger.onBuildComplete({ prepareMs: 100, executeMs: 50, totalMs: 150, rows: 1000, strategy: 'cte' });
            expect(spy).toHaveBeenCalledWith(
                '[report-builder] prepare: 100ms | duckdb-exec: 50ms | total: 150ms | rows: 1000 | strategy: cte',
            );
            spy.mockRestore();
        });

        it('should support custom prefix', () => {
            const spy = jest.spyOn(console, 'log').mockImplementation();
            const logger = consoleLogger('my-report');
            logger.onInit({ durationMs: 10 });
            expect(spy).toHaveBeenCalledWith('[my-report] init: 10ms');
            spy.mockRestore();
        });
    });

    describe('custom logger', () => {
        it('should allow partial implementation', () => {
            const events: string[] = [];
            const logger: ReportLogger = {
                onSourceLoad: (e) => events.push(`${e.alias}:${e.durationMs}`),
            };

            // Should not throw when calling methods that exist
            logger.onSourceLoad?.({ alias: 'readings', provider: 'InMemory', durationMs: 5 });
            // Should not throw when calling methods that don't exist
            logger.onInit?.({ durationMs: 10 });
            logger.onBuildComplete?.({ prepareMs: 1, executeMs: 2, totalMs: 3, rows: 0, strategy: 'cte' });

            expect(events).toEqual(['readings:5']);
        });
    });
});
