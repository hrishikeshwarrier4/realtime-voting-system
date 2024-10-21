# Real-Time Voting System

The Real-Time Voting System is a distributed, fault-tolerant voting system designed to process, aggregate, and visualize votes in real time. This project utilizes Apache Kafka for event streaming, Apache Spark for data processing, and PostgreSQL for persistence of voter and candidate data. The system is built with microservices architecture, leveraging Docker for containerization.

## Features

- **Real-Time Vote Aggregation**: Processes incoming votes in real time from various polling stations using Kafka and Spark Streaming.
- **Data Storage**: Utilizes PostgreSQL to persist information about voters, candidates, and voting results.
- **Data Visualization**: Displays real-time voting statistics via a Streamlit-based web application.
- **Fault Tolerance**: Ensures resilience through distributed components running in containers using Docker and Docker-Compose.
- **Scalability**: Can scale easily by adding more Kafka brokers and Spark nodes.

## Tech Stack

- **Apache Kafka**: For real-time streaming of voting data.
- **Apache Spark**: For processing and aggregating votes in real time.
- **PostgreSQL**: As a relational database to store voter and candidate information.
- **Streamlit**: To create a web-based dashboard for real-time visualizations of voting data.
- **Docker & Docker-Compose**: For containerizing the application services and simplifying deployment.

## Setup Instructions

### Prerequisites

- **Docker**: Ensure Docker and Docker-Compose are installed on your machine.
- **Python**: Python 3.7+ installed on your local machine.
- **Kafka**: A running Kafka instance (you can use the Docker setup to bring this up).

### Steps to Setup the Project

1. **Clone the repository**:
   ```bash
   git clone https://github.com/hrishikeshwarrier4/realtime-voting-system.git
   cd realtime-voting-system
2. ** Build and Start Services using Docker-Compose: Ensure that Kafka, PostgreSQL, and the Spark containers are running:**
   ```bash
   docker-compose up --build
3. **Run the main, voting and spark_streaming code:**
   ```bash
    python main.py
    python voting.py
    python spark_streaming.py
4. **Launch the Streamlit Web Application:**
   ```bash
   streamlit run streamlit-app.py
5. **Access the Application:**
   - Streamlit Web App: Open your browser and go to http://localhost:8501.
   - Kafka: Kafka will be running at localhost:9092.
   - PostgreSQL: Access the database at localhost:5432 (default user: postgres, default password: postgres).
 
 ## Configuration
 
 All environment-specific configurations are stored in the .env file, including database credentials and Kafka configurations.

## License
This project is licensed under the MIT License 
