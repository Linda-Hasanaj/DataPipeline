# Data Pipeline

## Overview
This project implements a modular data processing pipeline.  
It ingests marketing data from a CSV file, processes and enriches it through multiple stages, and writes the results into a PostgreSQL database.  
The pipeline is fully object-oriented, designed for extensibility and reusability, and can be triggered through a FastAPI endpoint.

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

### 4. Orchestrator

The **Orchestrator** is the central controller of the entire data pipeline.  
It manages the full sequence of operations — starting from reading the dataset, passing it through all defined processors, and finally writing the transformed data to the database.

The orchestrator ensures that:
1. Each task (Reader → Processors → Writer) runs in the correct order.  
2. Logs are produced for every step to make debugging and monitoring easier.  
3. Any failure in a processor stops the execution gracefully and reports the error.

####  Workflow
[Reader] → [Processors] → [Writer]
