import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import pool from './db';
import { stringify } from 'csv-stringify';
import fs from 'fs';
import path from 'path';
import winston from 'winston';

export const app = express();
const port = process.env.PORT || 8080;

app.use(express.json());

// Logger
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'logs/combined.log' })
    ],
});

// Ensure output dir exists
const outputDir = path.join(__dirname, '../output');
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
    console.log(`Created output directory at ${outputDir}`);
}

// Ensure logs dir exists if you want logging to a file
const logsDir = path.join(__dirname, '../logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir);
}

// Health Check
app.get('/health', async (req, res) => {
    try {
        const client = await pool.connect();
        client.release();
        res.json({
            status: 'ok',
            timestamp: new Date().toISOString(),
        });
    } catch (error) {
        logger.error('Health check failed', error);
        res.status(503).json({ status: 'error' });
    }
});

interface ExportJob {
    jobId: string;
    consumerId: string;
    exportType: 'full' | 'incremental' | 'delta';
    outputFilename: string;
}

// Export Endpoints
app.post('/exports/full', async (req, res) => {
    const consumerId = req.header('X-Consumer-ID');
    if (!consumerId) {
        return res.status(400).json({ error: 'X-Consumer-ID header required' });
    }

    const jobId = uuidv4();
    const outputFilename = `full_${consumerId}_${Date.now()}.csv`;

    logger.info('Export job started', {
        jobId,
        consumerId,
        exportType: 'full'
    });

    // Start async job
    runFullExport(jobId, consumerId, outputFilename).catch(err => {
        logger.error('Full export job failed', { jobId, error: err.message });
    });

    res.status(202).json({
        jobId,
        status: 'started',
        exportType: 'full',
        outputFilename
    });
});

app.post('/exports/incremental', async (req, res) => {
    const consumerId = req.header('X-Consumer-ID');
    if (!consumerId) {
        return res.status(400).json({ error: 'X-Consumer-ID header required' });
    }

    // Check if watermark exists
    try {
        const watermarkRes = await pool.query('SELECT last_exported_at FROM watermarks WHERE consumer_id = $1', [consumerId]);
        if (watermarkRes.rows.length === 0) {
            return res.status(400).json({ error: 'No watermark found for consumer. Perform a full export first.' });
        }

        const lastExportedAt = watermarkRes.rows[0].last_exported_at;
        const jobId = uuidv4();
        const outputFilename = `incremental_${consumerId}_${Date.now()}.csv`;

        logger.info('Export job started', {
            jobId,
            consumerId,
            exportType: 'incremental'
        });

        runIncrementalExport(jobId, consumerId, outputFilename, lastExportedAt).catch(err => {
            logger.error('Incremental export job failed', { jobId, error: err.message });
        });

        res.status(202).json({
            jobId,
            status: 'started',
            exportType: 'incremental',
            outputFilename
        });
    } catch (err) {
        logger.error('Error fetching watermark', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/exports/delta', async (req, res) => {
    const consumerId = req.header('X-Consumer-ID');
    if (!consumerId) {
        return res.status(400).json({ error: 'X-Consumer-ID header required' });
    }

    // Check if watermark exists
    try {
        const watermarkRes = await pool.query('SELECT last_exported_at FROM watermarks WHERE consumer_id = $1', [consumerId]);
        if (watermarkRes.rows.length === 0) {
            return res.status(400).json({ error: 'No watermark found for consumer. Perform a full export first.' });
        }

        const lastExportedAt = watermarkRes.rows[0].last_exported_at;
        const jobId = uuidv4();
        const outputFilename = `delta_${consumerId}_${Date.now()}.csv`;

        logger.info('Export job started', {
            jobId,
            consumerId,
            exportType: 'delta'
        });

        runDeltaExport(jobId, consumerId, outputFilename, lastExportedAt).catch(err => {
            logger.error('Delta export job failed', { jobId, error: err.message });
        });

        res.status(202).json({
            jobId,
            status: 'started',
            exportType: 'delta',
            outputFilename
        });
    } catch (err) {
        logger.error('Error fetching watermark', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/exports/watermark', async (req, res) => {
    const consumerId = req.header('X-Consumer-ID');
    if (!consumerId) {
        return res.status(400).json({ error: 'X-Consumer-ID header required' });
    }

    try {
        const result = await pool.query('SELECT last_exported_at FROM watermarks WHERE consumer_id = $1', [consumerId]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Watermark not found' });
        }
        res.json({
            consumerId,
            lastExportedAt: result.rows[0].last_exported_at
        });
    } catch (err) {
        logger.error('Error fetching watermark', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Implementation of export functions
async function runFullExport(jobId: string, consumerId: string, filename: string) {
    const client = await pool.connect();
    const outputFilePath = path.join(outputDir, filename);
    const writeStream = fs.createWriteStream(outputFilePath);
    const stringifier = stringify({ header: true, columns: ['id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted'] });

    stringifier.pipe(writeStream);

    const startTime = Date.now();
    let maxUpdatedAt: Date | null = null;
    let rowCount = 0;

    try {
        const query = 'SELECT * FROM users WHERE is_deleted = FALSE ORDER BY updated_at ASC';
        const result = await client.query(query);

        for (const row of result.rows) {
            stringifier.write(row);
            rowCount++;
            const currentUpdatedAt = new Date(row.updated_at);
            if (!maxUpdatedAt || currentUpdatedAt > maxUpdatedAt) {
                maxUpdatedAt = currentUpdatedAt;
            }
        }

        stringifier.end();

        await new Promise((resolve, reject) => {
            writeStream.on('finish', () => resolve(undefined));
            writeStream.on('error', reject);
        });

        if (maxUpdatedAt) {
            await updateWatermark(client, consumerId, maxUpdatedAt);
        }

        const duration = Date.now() - startTime;
        logger.info('Export job completed', {
            jobId,
            rowsExported: rowCount,
            duration
        });

    } catch (error) {
        logger.error('Export errored out. Deleting file.', error);
        if (fs.existsSync(outputFilePath)) {
            fs.unlinkSync(outputFilePath);
        }
        throw error;
    } finally {
        client.release();
    }
}

async function runIncrementalExport(jobId: string, consumerId: string, filename: string, lastExportedAt: Date) {
    const client = await pool.connect();
    const outputFilePath = path.join(outputDir, filename);
    const writeStream = fs.createWriteStream(outputFilePath);
    const stringifier = stringify({ header: true, columns: ['id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted'] });

    stringifier.pipe(writeStream);

    const startTime = Date.now();
    let maxUpdatedAt: Date | null = null;
    let rowCount = 0;

    try {
        const query = 'SELECT * FROM users WHERE updated_at > $1 AND is_deleted = FALSE ORDER BY updated_at ASC';
        const result = await client.query(query, [lastExportedAt]);

        for (const row of result.rows) {
            stringifier.write(row);
            rowCount++;
            const currentUpdatedAt = new Date(row.updated_at);
            if (!maxUpdatedAt || currentUpdatedAt > maxUpdatedAt) {
                maxUpdatedAt = currentUpdatedAt;
            }
        }

        stringifier.end();

        await new Promise((resolve, reject) => {
            writeStream.on('finish', () => resolve(undefined));
            writeStream.on('error', reject);
        });

        if (maxUpdatedAt) {
            await updateWatermark(client, consumerId, maxUpdatedAt);
        }

        const duration = Date.now() - startTime;
        logger.info('Export job completed', {
            jobId,
            rowsExported: rowCount,
            duration
        });

    } catch (error) {
        logger.error('Export errored out. Deleting file.', error);
        if (fs.existsSync(outputFilePath)) {
            fs.unlinkSync(outputFilePath);
        }
        throw error;
    } finally {
        client.release();
    }
}

async function runDeltaExport(jobId: string, consumerId: string, filename: string, lastExportedAt: Date) {
    const client = await pool.connect();
    const outputFilePath = path.join(outputDir, filename);
    const writeStream = fs.createWriteStream(outputFilePath);
    // Delta export includes 'operation' column
    const stringifier = stringify({ header: true, columns: ['operation', 'id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted'] });

    stringifier.pipe(writeStream);

    const startTime = Date.now();
    let maxUpdatedAt: Date | null = null;
    let rowCount = 0;

    try {
        // Delta includes deleted records too
        const query = 'SELECT * FROM users WHERE updated_at > $1 ORDER BY updated_at ASC';
        const result = await client.query(query, [lastExportedAt]);

        for (const row of result.rows) {
            let operation = 'UPDATE';
            if (row.is_deleted) {
                operation = 'DELETE';
            } else if (new Date(row.created_at).getTime() === new Date(row.updated_at).getTime()) {
                operation = 'INSERT';
            }

            stringifier.write({ ...row, operation });
            rowCount++;
            if (!maxUpdatedAt || new Date(row.updated_at) > maxUpdatedAt) {
                maxUpdatedAt = new Date(row.updated_at);
            }
        }

        stringifier.end();

        await new Promise((resolve, reject) => {
            writeStream.on('finish', () => resolve(undefined));
            writeStream.on('error', reject);
        });

        if (maxUpdatedAt) {
            await updateWatermark(client, consumerId, maxUpdatedAt);
        }

        const duration = Date.now() - startTime;
        logger.info('Export job completed', {
            jobId,
            rowsExported: rowCount,
            duration
        });

    } catch (error) {
        logger.error('Export errored out. Deleting file.', error);
        if (fs.existsSync(outputFilePath)) {
            fs.unlinkSync(outputFilePath);
        }
        throw error;
    } finally {
        client.release();
    }
}

async function updateWatermark(client: any, consumerId: string, lastExportedAt: Date) {
    const query = `
        INSERT INTO watermarks (consumer_id, last_exported_at, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (consumer_id)
        DO UPDATE SET last_exported_at = EXCLUDED.last_exported_at, updated_at = NOW();
    `;
    await client.query(query, [consumerId, lastExportedAt]);
}

// Only listen if this file is run directly (not imported)
if (require.main === module) {
    app.listen(port, () => {
        logger.info(`Server is running on port ${port}`);
    });
}
