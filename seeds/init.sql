-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);

-- Create index on updated_at
CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);

-- Create watermarks table
CREATE TABLE IF NOT EXISTS watermarks (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(255) NOT NULL UNIQUE,
    last_exported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Seed data if empty
DO $$
DECLARE
    i INT;
    start_date TIMESTAMP := NOW() - INTERVAL '7 days';
    random_created INTERVAL;
    random_updated INTERVAL;
    is_del BOOLEAN;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM users LIMIT 1) THEN
        RAISE NOTICE 'Seeding 100,000 users...';
        FOR i IN 1..100000 LOOP
            random_created := (random() * 7 * 24 * 60 * 60) * INTERVAL '1 second';
            -- Update happens between creation and now, let's just say up to 1 day after creation or straight away
            -- to keep it simple, updated_at >= created_at
            random_updated := random_created + ((random() * 24 * 60 * 60) * INTERVAL '1 second');
            
            is_del := (random() < 0.02);
            
            INSERT INTO users (name, email, created_at, updated_at, is_deleted)
            VALUES (
                'User ' || i,
                'user' || i || '@example.com',
                start_date + random_created,
                start_date + random_updated,
                is_del
            );
        END LOOP;
        RAISE NOTICE 'Seeding complete.';
    END IF;
END $$;
