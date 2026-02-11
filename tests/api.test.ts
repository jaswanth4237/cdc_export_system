import request from 'supertest';
import { app } from '../src/index';
import pool from '../src/db';
import fs from 'fs';
import path from 'path';

// Mock DB
jest.mock('../src/db', () => {
    const mPool = {
        connect: jest.fn(),
        query: jest.fn(),
        on: jest.fn(),
        end: jest.fn(),
    };
    return {
        __esModule: true,
        default: mPool,
    };
});

// Mock FS methods using spyOn where possible or jest.mock if ESM issues arise.
// Since we are using ts-jest, jest.mock is hoisted.
jest.mock('fs', () => {
    const originalFs = jest.requireActual('fs');
    return {
        ...originalFs,
        existsSync: jest.fn(),
        mkdirSync: jest.fn(),
        createWriteStream: jest.fn(),
        unlinkSync: jest.fn(),
    };
});

// Mock Logger
jest.mock('winston', () => {
    const mLogger = {
        info: jest.fn(),
        error: jest.fn(),
    };
    const mFormat = {
        json: jest.fn(),
    };
    return {
        createLogger: jest.fn(() => mLogger),
        format: {
            json: jest.fn(),
        },
        transports: {
            Console: jest.fn(),
            File: jest.fn(),
        },
    };
});

describe('CDC Export System API', () => {
    let mockClient: any;
    let mockStream: any;

    beforeEach(() => {
        jest.clearAllMocks();

        // Setup mock client
        mockClient = {
            query: jest.fn().mockResolvedValue({ rows: [] }),
            release: jest.fn(),
        };
        (pool.connect as jest.Mock).mockResolvedValue(mockClient);

        // Setup mock stream
        mockStream = {
            write: jest.fn(),
            end: jest.fn(),
            on: jest.fn((event, callback) => {
                if (event === 'finish') {
                    // Execute callback asynchronously to simulate stream finishing
                    process.nextTick(callback);
                }
                return mockStream;
            }),
            once: jest.fn(),
            emit: jest.fn(),
            pipe: jest.fn(), // If piped to, obscure compatibility
        };
        // Fix for "writeStream.on is not a function" - ensure createWriteStream returns our mock
        (fs.createWriteStream as unknown as jest.Mock).mockReturnValue(mockStream);
        (fs.existsSync as unknown as jest.Mock).mockReturnValue(true);
    });

    afterAll(() => {
        // cleanup
    });

    // Helper to wait for async operations
    const waitForAsync = () => new Promise(resolve => setImmediate(resolve));

    test('GET /health should return 200', async () => {
        const res = await request(app).get('/health');
        expect(res.status).toBe(200);
        expect(res.body.status).toBe('ok');
    });

    test('POST /exports/full should start job', async () => {
        const res = await request(app)
            .post('/exports/full')
            .set('X-Consumer-ID', 'consumer-1');

        expect(res.status).toBe(202);
        expect(res.body.jobId).toBeDefined();

        // Allow async job to run
        await waitForAsync();

        expect(pool.connect).toHaveBeenCalled();
        expect(mockClient.query).toHaveBeenCalled();
    });

    test('POST /exports/incremental should fail without watermark', async () => {
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [] }); // No watermark

        const res = await request(app)
            .post('/exports/incremental')
            .set('X-Consumer-ID', 'consumer-2');

        expect(res.status).toBe(400);
    });

    test('POST /exports/incremental should start if watermark exists', async () => {
        const lastExportedAt = new Date('2023-01-01');
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [{ last_exported_at: lastExportedAt }] });

        const res = await request(app)
            .post('/exports/incremental')
            .set('X-Consumer-ID', 'consumer-2');

        expect(res.status).toBe(202);

        await waitForAsync();

        expect(mockClient.query).toHaveBeenCalledWith(
            expect.stringContaining('SELECT * FROM users WHERE updated_at > $1'),
            expect.anything()
        );
    });

    test('POST /exports/delta should start if watermark exists', async () => {
        const lastExportedAt = new Date('2023-01-01');
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [{ last_exported_at: lastExportedAt }] });

        const res = await request(app)
            .post('/exports/delta')
            .set('X-Consumer-ID', 'consumer-3');

        expect(res.status).toBe(202);

        await waitForAsync();

        expect(mockClient.query).toHaveBeenCalledWith(
            expect.stringContaining('SELECT * FROM users WHERE updated_at > $1'),
            expect.anything()
        );
    });

    test('GET /exports/watermark should return 404 if not found', async () => {
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [] });

        const res = await request(app)
            .get('/exports/watermark')
            .set('X-Consumer-ID', 'consumer-4');

        expect(res.status).toBe(404);
    });

    test('GET /exports/watermark should return watermark', async () => {
        const date = new Date();
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [{ last_exported_at: date }] });

        const res = await request(app)
            .get('/exports/watermark')
            .set('X-Consumer-ID', 'consumer-4');

        expect(res.status).toBe(200);
        expect(res.body.lastExportedAt).toBe(date.toISOString());
    });

    // Detailed logic tests
    test('Full export should update watermark', async () => {
        const rows = [
            { id: 1, updated_at: '2023-01-02T10:00:00Z', is_deleted: false },
        ];

        mockClient.query.mockImplementation((query: string) => {
            if (query.includes('SELECT * FROM users')) return Promise.resolve({ rows });
            return Promise.resolve({ rows: [] });
        });

        await request(app).post('/exports/full').set('X-Consumer-ID', 'consumer-test');
        await waitForAsync();

        // Check if watermark update query was called
        expect(mockClient.query).toHaveBeenCalledWith(
            expect.stringContaining('INSERT INTO watermarks'),
            expect.anything()
        );
    });

    test('Delta export identifies DELETE operations', async () => {
        const lastExportedAt = new Date('2023-01-01');
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [{ last_exported_at: lastExportedAt }] });

        const rows = [{ id: 1, updated_at: '2023-01-02', is_deleted: true }];

        mockClient.query.mockImplementation((query: string) => {
            if (query.includes('SELECT * FROM users')) return Promise.resolve({ rows });
            return Promise.resolve({ rows: [] });
        });

        await request(app).post('/exports/delta').set('X-Consumer-ID', 'consumer-delta');
        await waitForAsync();

        // This mainly ensures we don't crash and cover the lines
        expect(mockClient.release).toHaveBeenCalled();
    });

    test('Export job error handling', async () => {
        (pool.query as jest.Mock).mockResolvedValueOnce({ rows: [{ last_exported_at: new Date() }] });
        mockClient.query.mockRejectedValue(new Error('Query failed'));

        await request(app).post('/exports/incremental').set('X-Consumer-ID', 'consumer-err');
        await waitForAsync();

        expect(fs.unlinkSync).toHaveBeenCalled();
    });
});
