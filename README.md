# Airflow Workflow Setup Guide

This guide outlines the steps to set up an Apache Airflow environment using a Python virtual environment, PostgreSQL as the metadata store, and a custom configuration.

---

## 🔧 Setup Instructions

### 1. Create a Virtual Environment

```bash
virtualenv .venv
source .venv/bin/activate
````

### 2. Install Required Python Packages

```bash
pip install -r requirements.txt
```

---

## ⚙️ Airflow Configuration

### 3. Set `AIRFLOW_HOME` Environment Variable

Set `AIRFLOW_HOME` to your project directory (or desired path):

```bash
export AIRFLOW_HOME=$(pwd)
```

You can also add this to your shell config (`~/.bashrc` or `~/.zshrc`) for persistence.

---

### 4. Verify Airflow Installation

Run the following command to confirm Airflow is installed:

```bash
airflow
```

---

### 5. Configure Airflow

#### a. Edit `airflow.cfg`:

Open the `airflow.cfg` file located in `$AIRFLOW_HOME`.

* **Change default webserver port to avoid Spark conflict**:

  ```ini
  web_server_port = 8081
  ```

* **Disable example DAGs**:

  ```ini
  load_examples = False
  ```

* **Use PostgreSQL as the metadata database**:

  Ensure your PostgreSQL database is running and accessible.

  Replace the default `sqlite` connection with:

  ```ini
  sql_alchemy_conn = postgres://postgres:postgres@localhost:5432/airflow_meta?sslmode=prefer
  ```

> Note: Make sure `airflow_meta` database exists in PostgreSQL.

* **Use LocalExecutor for parallel execution**:

  ```ini
  executor = LocalExecutor
  ```

---

### 6. Initialize and Run Airflow

Run the standalone setup to automatically initialize the database and start all necessary components:

```bash
airflow standalone
```

This will start:

* Webserver (on port `8081`)
* Scheduler
* Workers (using `LocalExecutor`)

---