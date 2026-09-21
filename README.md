
# CDC Data Export System

This project is a containerized backend service that implements a Change Data Capture (CDC) style data export system.
It supports full,incremental, and delta exports of user data using watermarking.

## Prerequisites

- Docker
- Docker Compose

## Quick Start
1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd cdc_export_system
    ```

2.  **Start the application**:
    ```bash
    docker-compose up --build
    ```
    This will start the PostgreSQL database and the Node.js application.
    The database will be automatically seeded with 100,000 user records (this may take a few seconds on first run).

3.  **Verify Health**:
    ```bash
    curl http://localhost:8080/health
    ```
    You should see `{"status":"ok", ...}`.

## API Usage

### 1. Trigger Full Export
Initiates a full export of all non-deleted users.
```bash
curl -X POST http://localhost:8080/exports/full -H "X-Consumer-ID: my-consumer-1"
```
The response will include a `jobId` and `outputFilename`. The file will appear in the `./output` directory on your host machine.

### 2. Get Watermark
Check the last exported timestamp for a consumer.
```bash
curl http://localhost:8080/exports/watermark -H "X-Consumer-ID: my-consumer-1"
```

### 3. Trigger Incremental Export
Exports only records updated since the last watermark.
```bash
curl -X POST http://localhost:8080/exports/incremental -H "X-Consumer-ID: my-consumer-1"
```

### 4. Trigger Delta Export
Exports records updated since the last watermark, including the operation type (INSERT, UPDATE, DELETE).
```bash
curl -X POST http://localhost:8080/exports/delta -H "X-Consumer-ID: my-consumer-1"
```

## Running Tests
To run the test suite and check coverage (ensure the container is running or you have a local instance):

**Ideally, run inside the container to access the test DB:**
1.  Get the container ID or name (e.g., `cdc_export_system_app_1`).
2.  Run:
    ```bash
    docker-compose exec app npm run test
    ```

This will run Jest with coverage reports.

## Architecture
- **Language**: TypeScript / Node.js
- **Database**: PostgreSQL
- **Watermarking**: Stored in `watermarks` table.
- **Exports**: Written to `output/` volume as CSV.
- **Logging**: Console and file logs in `logs/`.

## Environment Variables
See `.env.example`.
- `DATABASE_URL`: Connection string for Postgres.
- `PORT`: Application port.

