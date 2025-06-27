# B2C 3 Bikes Monthly Report Automation

Automated reporting pipeline for generating and emailing monthly B2C vehicle swap & mileage reports.

---

## 🚀 Overview

- Pulls swap and SoC data from MySQL.
- Calculates monthly battery usage and distance.
- Saves results to DBFS and local directory.
- Emails the CSV report to recipients.

---

## 🛠 Requirements

- Python 3.8+
- Libraries: `pymysql`, `pandas`, `yagmail`, `pyspark`, `dateutil`
- Spark environment (e.g. Databricks)
- DBFS write permission
- Valid Gmail App Password for SMTP

---

## 📂 Files

| File | Purpose |
|------|---------|
| `report_save.py` | Extracts data from MySQL, aggregates results, and saves report |
| `mail_generate.py` | Sends the generated report to stakeholders via email |

---

## 🔐 Configuration

Update the following before use:

- `DB_CONFIG`: Fill with actual MySQL credentials (avoid pushing secrets to GitHub).
- `EMAIL_CONFIG`: Use a Gmail App Password (not normal password) for secure sending.

---

## 📈 Output

- Report saved to: `dbfs:/FileStore/reports/B2C/`
- Local path: `~/shared_reports/B2C_3_Bikes_Report_<DATE_RANGE>.csv`
- Email subject: **"B2C Bikes Monthly Report"**

---

## 📬 Example Recipients

- harsha@oyika.com
- ...
- ...

---

---
## 🔁 Scheduling (Optional)

Use Databricks Jobs or Apache Airflow to run both scripts monthly.

---

## 👤 Author

Harshavardhan, Oyika



# Monthly Electricity Usage Report

This repository automates the process of extracting electricity meter readings from MySQL, saving the results in Databricks, and distributing monthly reports via email.

---

## 🔧 Components

| File             | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| `report_save.py` | Extracts data from MySQL, transforms it, and appends to Databricks Delta.   |
| `mail_generate.py` | Queries the Databricks table, generates a CSV report, and emails it.     |

---

## 🛠️ Setup

1. **Environment**:
    - Databricks (with Delta support)
    - Python 3.8+
    - Spark Session on Databricks
    - MySQL (ApsaraDB)

2. **Install Libraries**:
```bash
pip install pandas pymysql mysql-connector-python yagmail


