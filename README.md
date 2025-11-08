# 🚀 ETL Project with Docker & Airflow

## 🧾 Overview
This **ETL (Extract, Transform, Load)** project automates data processing using **Apache Airflow** in a **Dockerized environment**.  
It extracts data from multiple sources, transforms it for consistency, and loads it into a target database for analytics and reporting.

---

## ✨ Features
- **Data Extraction:** Pulls data from multiple sources including JSON files and databases.  
- **Data Transformation:** Cleans, formats, and processes data to ensure consistency and usability.  
- **Data Loading:** Loads processed data into target databases (e.g., MongoDB, SQL).  
- **Airflow DAGs:** ETL workflows are orchestrated using Airflow Directed Acyclic Graphs (DAGs).  
- **Dockerized Environment:** Runs seamlessly in containers for reproducibility and easy deployment.  
- **Logging & Monitoring:** Airflow UI provides DAG monitoring and execution logs.

---

## 🧰 Tech Stack
| Component | Technology |
|------------|-------------|
| **Programming Language** | Python / PySpark |
| **Workflow Orchestration** | Apache Airflow |
| **Containerization** | Docker & Docker Compose |
| **Databases** | MongoDB, SQL Server / SQLite *(local MinIO bucket used as database)* |
| **Libraries & Tools** | Pandas, JSON, Logging |

---

## 📁 Project Structure
