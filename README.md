# Data Pipeline

## Overview
This project implements a modular data processing pipeline.  
It ingests marketing data from a CSV file, processes and enriches it through multiple stages, and writes the results into a PostgreSQL database.  
The pipeline is fully object-oriented, designed for extensibility and reusability.
It can be triggered locally through FastAPI or emulated as AWS Lambda functions using Serverless Framework.

---

## Architecture

### 1. Reader
Responsible for loading data into memory as a pandas DataFrame.  
- Reads CSVs.
- Example: 'CSVReader'  
  - Reads from a given path  
  - Configurable separator, encoding, and chunk size

### 2. Processors
Each processor inherits from a common 'Processor' base class and performs one transformation on the data.  
They are orchestrated sequentially by the 'Orchestrator'.

| Processor | Description |
|------------|--------------|
| **MissingValuesProcessor** | Fills missing values in 'time_spent_seconds' using mean or median. |
| **ConversionProcessor** | Creates a 'converted' column (1 if purchase > 0 else 0). |
| **StateAbbreviationProcessor** | Maps full state names to US state abbreviations using the 'us' library. |
| **PercentileProcessor** | Calculates 85th percentile of purchases per state and nationally. |
| **NormalizationProcessor** | Scales numeric values for further analysis (e.g., min-max). |

Each processor:
- Inherits from 'Processor(Task)'
- Logs its actions
- Returns a transformed DataFrame

---

### 3. Writer
Persists the final DataFrame into PostgreSQL using SQLAlchemy.

**Writer class:** `PostgreSQLStorage`  
Config parameters:
 ```json
{
  "dsn": "postgresql+psycopg://user:password@localhost:5432/test_db",
  "schema": "analytics",
  "table": "customer_engagement",
  "if_exists": "replace"
}
```

### 4. Orchestrator

The **Orchestrator** is the central controller of the entire data pipeline.  
It manages the full sequence of operations — starting from reading the dataset, passing it through all defined processors, and finally writing the transformed data to the database.

The orchestrator ensures that:
1. Each task (Reader → Processors → Writer) runs in the correct order.  
2. Logs are produced for every step to make debugging and monitoring easier.  
3. Any failure in a processor stops the execution gracefully and reports the error.

####  Workflow
[Reader] → [Processors] → [Writer]

###  Setup guide
First, clone this repository to your local machine and move into the project directory:

```bash
git clone https://github.com/Linda-Hasanaj/DataPipeline.git
cd DataPipeline
```

---

### Create and activate a virtual environment
It’s strongly recommended to use a virtual environment to isolate dependencies.

```bash
# Create virtual environment
python -m venv .venv

# Activate the environment
# On Linux/Mac:
source .venv/bin/activate
# On Windows (PowerShell):
.venv\Scripts\activate
```

Once activated, your terminal should show `(.venv)` before the prompt — that means the environment is active.

---

### Install dependencies
Make sure pip is up-to-date, then install all required Python packages:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

### Configure environment variables
This project uses a '.env' file to store database credentials and configuration.

Create one by copying the provided example:
```bash
cp .env.example .env
```

Then open '.env' and fill in your local PstgreSQL configuration:

PGUSER=user
PGPASSWORD=password
PGHOST=localhost
PGPORT=5432
PGDATABASE=Pipeline

These variables are automatically used to build the PostgreSQL connection string.

---


### Run the FastAPI server with Uvicorn
```bash
uvicorn api:app --reload
```

### Serverless setup
The project also supports deployment and local testing via the Serverless Framework, which emulates AWS Lambda and API Gateway locally.
1. Install Serverless
```
npm install -g serverless
npm install
```
2. Start serverless offline
```
npx sls offline
```

### Running tests
Unit tests are located in tests/ directory
Run them using pytest
```
pytest -v
```
