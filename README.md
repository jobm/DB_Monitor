# Database Change Data Capture (CDC) Monitor

A robust Change Data Capture (CDC) monitoring system built with FastAPI, Kafka, and Debezium. This project enables real-time tracking and auditing of database changes across multiple databases, making it ideal for audit trails, data synchronization, and compliance monitoring.

## Architecture

```mermaid
graph LR
    subgraph Source Databases
        DB1[Order DB]
        DB2[Catalog DB]
        DB3[Shipping DB]
    end

    subgraph CDC Layer
        D[Debezium Connectors]
    end

    subgraph Message Bus
        K[Kafka]
    end

    subgraph Monitor Application
        C[Consumer Service]
        A[FastAPI App]
        MD[(Monitor DB)]
    end

    DB1 --> D
    DB2 --> D
    DB3 --> D
    D --> K
    K --> C
    C --> MD
    A --> MD

    style Source Databases fill:#f9f,stroke:#333,stroke-width:2px
    style CDC Layer fill:#bbf,stroke:#333,stroke-width:2px
    style Message Bus fill:#bfb,stroke:#333,stroke-width:2px
    style Monitor Application fill:#fbb,stroke:#333,stroke-width:2px
```

## Features

- Real-time database change monitoring
- Support for multiple source databases
- Configurable CDC connectors using Debezium
- Asynchronous event processing
- RESTful API for querying captured events
- Health check endpoints
- Docker containerization
- Graceful shutdown handling

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- PostgreSQL 15
- Kafka
- Debezium

## Project Structure

```
├── app/
│   ├── consumer/           # Kafka consumer implementation
│   ├── __init__.py
│   ├── config.py          # Configuration settings
│   ├── consumer_service.py # Kafka consumer service
│   ├── extensions.py      # Database extensions
│   ├── lifecycle_manager.py# Application lifecycle management
│   ├── main.py           # FastAPI application entry point
│   ├── models.py         # Database models
│   ├── routes.py         # API routes
│   └── requirements.txt   # Python dependencies
├── connectors/           # Debezium connector configurations
│   ├── catalogdb-connector.json
│   ├── orderdb-connector.json
│   └── shippingdb-connector.json
├── init/                # Database initialization scripts
│   ├── catalog.sql
│   ├── order.sql
│   └── shipping.sql
├── docker-compose.yml   # Docker services configuration
├── Dockerfile.register  # Connector registration container
└── register-connectors.sh # Connector registration script
```

## Setup and Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd DB_Monitor
   ```

2. Create a `.env` file in the root directory:
   ```env
   KAFKA_BROKER=kafka:9092
   KAFKA_TOPIC=orderdb.public.orders
   POSTGRES_URL=postgresql+asyncpg://postgres:postgres@postgres-monitor:5432/postgres
   ```

3. Start the infrastructure services:
   ```bash
   docker-compose up -d
   ```

4. Register the Debezium connectors:
   ```bash
   ./register-connectors.sh
   ```

5. The application will be available at:
   - FastAPI Application: http://localhost:8000
   - Kafka UI: http://localhost:8080
   - Kafka Connect UI: http://localhost:8083

## Available Endpoints

- `GET /events` - Retrieve all captured database events
- `GET /health` - Check application health status
- `GET /info` - Get application information

## Monitoring Setup

1. The system monitors three source databases:
   - Order Database (Port 5434)
   - Catalog Database (Port 5435)
   - Shipping Database (Port 5436)

2. Events are captured and stored in:
   - Monitor Database (Port 5437)

3. Kafka and Zookeeper are accessible at:
   - Kafka: localhost:9093 (External), kafka:9092 (Internal)
   - Zookeeper: localhost:2181

## Development

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   ```

2. Install dependencies:
   ```bash
   cd app
   pip install -r requirements.txt
   ```

3. Run the application locally:
   ```bash
   uvicorn main:app --reload
   ```

## Production Deployment

The application is containerized and can be deployed using Docker Compose. For production deployment:

1. Update the environment variables in the `.env` file
2. Adjust the `docker-compose.yml` file for production settings
3. Deploy using Docker Compose:
   ```bash
   docker-compose -f docker-compose.prod.yml up -d
   ```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.