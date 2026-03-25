/**
 * Profiling Unit Tests
 *
 * Tests for memory snapshot parsing, sum helpers, and the profiling callback factory.
 */

import { sumMemoryBytes, type MemorySnapshot } from 'duckdb-report-builder';

describe('Profiling Helpers', () => {
    describe('sumMemoryBytes', () => {
        it('should sum memoryUsageBytes across all snapshots', () => {
            const snapshots: MemorySnapshot[] = [
                { tag: 'BASE_TABLE', memoryUsageBytes: 1024, temporaryMemoryBytes: 0 },
                { tag: 'HASH_TABLE', memoryUsageBytes: 2048, temporaryMemoryBytes: 512 },
                { tag: 'ART_INDEX', memoryUsageBytes: 512, temporaryMemoryBytes: 0 },
            ];

            expect(sumMemoryBytes(snapshots)).toBe(3584);
        });

        it('should return 0 for empty snapshots', () => {
            expect(sumMemoryBytes([])).toBe(0);
        });

        it('should handle single snapshot', () => {
            const snapshots: MemorySnapshot[] = [
                { tag: 'BASE_TABLE', memoryUsageBytes: 4096, temporaryMemoryBytes: 100 },
            ];

            expect(sumMemoryBytes(snapshots)).toBe(4096);
        });
    });
});
