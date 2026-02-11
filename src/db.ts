import { Pool, PoolConfig } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const config: PoolConfig = {
    connectionString: process.env.DATABASE_URL,
};

const pool = new Pool(config);

pool.on('error', (err) => {
    console.error('Unexpected error on idle client', err);
    process.exit(-1);
});

export default pool;
