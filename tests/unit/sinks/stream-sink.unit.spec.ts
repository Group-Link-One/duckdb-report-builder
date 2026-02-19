/**
 * Stream Sink Unit Tests
 *
 * Tests for the StreamSink.
 */

import { StreamSink } from '../../../src/sinks/stream-sink';

describe('StreamSink', () => {
    describe('constructor', () => {
        it('should create stream sink with executor', () => {
            const mockExecutor = {
                runQuery: jest.fn(),
            };
            const sink = new StreamSink(mockExecutor as any);
            expect(sink).toBeDefined();
        });
    });

    describe('streamToMemory', () => {
        it('should execute query and return results', async () => {
            const mockResults = [
                { id: 1, value: 'a' },
                { id: 2, value: 'b' },
            ];
            const mockExecutor = {
                runQuery: jest.fn().mockResolvedValue(mockResults),
            };
            const sink = new StreamSink(mockExecutor as any);

            const results = await sink.streamToMemory('SELECT * FROM test');

            expect(mockExecutor.runQuery).toHaveBeenCalledWith('SELECT * FROM test');
            expect(results).toEqual(mockResults);
        });

        it('should handle empty results', async () => {
            const mockExecutor = {
                runQuery: jest.fn().mockResolvedValue([]),
            };
            const sink = new StreamSink(mockExecutor as any);

            const results = await sink.streamToMemory('SELECT * FROM empty_table');

            expect(results).toEqual([]);
        });

        it('should pass through query to executor', async () => {
            const mockExecutor = {
                runQuery: jest.fn().mockResolvedValue([]),
            };
            const sink = new StreamSink(mockExecutor as any);
            const query = 'SELECT id, name FROM users WHERE active = true';

            await sink.streamToMemory(query);

            expect(mockExecutor.runQuery).toHaveBeenCalledWith(query);
        });
    });
});
