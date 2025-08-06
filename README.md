# Revenue Analysis Pipeline

An automated evenue analysis pipeline that processes Excel data to generate daily/weekly/monthly revenue reports and sends them via email.

## Project Structure
```
lesson_revenue/
│
├── dags/
│   ├── Data_autouploaded_new.py    # Main DAG file
│   ├── config.py                   # Configuration
│   └── lesson_data/                # Data directory
│
├── docker-compose.yaml
└── Dockerfile
```
## Key Features

- **Automated Data Collection**: Extracts latest revenue data from Excel files
- **Multi-Currency Support**: Automatic conversion between USD, EUR, RUB
- **Revenue Analysis**: Daily, weekly, and monthly revenue aggregation
- **Automated Reporting**: Generates CSV reports and sends via email

## Tech Stack

- **Apache Airflow**: Workflow automation
- **MySQL**: Data storage
- **Python**: Data processing
  - pandas: Data analysis
  - requests: API communication
- **Docker**: Containerization

## Database Schema

### income Table
- `id`: INT (Primary Key, Auto Increment)
- `student_id`: VARCHAR(255)
- `lesson_date`: DATE
- `lesson_type`: VARCHAR(255)
- `student_name`: VARCHAR(255)

### client Table
- `student_id`: VARCHAR(255) (Primary Key)
- `fee`: DECIMAL
- `currency`: VARCHAR(10)

## Installation

1. Clone repository
```bash
git clone [https://github.com/seongmingong1/revenue_project/]
```
2. Run Docker
```
docker compose up -d
```
3. Access Airflow UI
* URL: localhost:8080

## DAG Structure
```
upload_task >> daily_task >> weekly_task >> monthly_task >> csv_task >> send_email_task
```
* upload_task: Extract data from Excel and save to DB
* daily_task: Calculate daily revenue
* weekly_task: Aggregate weekly revenue
* monthly_task: Aggregate monthly revenue
* csv_task: Generate CSV files
* send_email_task: Send results via email

## Configuration
Required environment variables:

MYSQL_PASSWORD
SMTP settings (for email)
API_KEY (for exchange rates)

## Execution Results 
Runs automatically at midnight daily, generating:

* daily_revenue.csv
* weekly_revenue.csv
* monthly_revenue.csv
