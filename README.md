![Lets VIBE Together Banner](assets/banner.png)

# 🚀 ETL_PIPELINE (DYNAMIC)
### A Dynamic ETL Pipeline with Automatic Schema Evolution

---

## 📌 About
Lets_VIBE_together is a Python-based ETL (Extract, Transform, Load) pipeline.

It automatically:
- Reads raw data files from a folder
- Detects file type
- Parses the data
- Evolves the database schema dynamically
- Loads processed data into MongoDB
- Prevents re-processing of files

---

## 🧠 What is ETL?
ETL stands for:
- **Extract** – Read raw data files
- **Transform** – Parse and structure the data
- **Load** – Store the data into a database

---

## ✨ Features
- Automatic file discovery
- Supports multiple file formats
- Dynamic schema evolution
- MongoDB integration
- Modular pipeline design
- Error-safe processing

---

## 🗂 Project Structure
```
Lets_VIBE_together/
│
├── data/
│ └── raw/
│
├── src/
│ ├── pipeline/
│ │ ├── extractor.py
│ │ ├── parsers.py
│ │ ├── schema.py
│ │ └── loader.py
│ │
│ └── config.py
│
├── main.py
├── requirements.txt
└── README.md
```

---

## ⚙️ How It Works
1. Finds raw files inside `data/raw`
2. Reads and parses each file
3. Automatically evolves schema if needed
4. Loads processed data into MongoDB
5. Moves processed files to avoid duplication

---

## 🛠 Requirements
- Python 3.9+
- MongoDB
- pip

---

## 📥 Installation

Clone the repository:
```
git clone https://github.com/ShivenduShivu/Lets_VIBE_together.git
cd Lets_VIBE_together
```
---
```
Create and activate virtual environment:

python -m venv venv


Windows:

venv\Scripts\activate


Mac / Linux:

source venv/bin/activate


Install dependencies:

pip install -r requirements.txt

▶️ Run the Project

Add files to:

data/raw/


Run:

python main.py
```
---

## ⚠️ Error Handling

Errors are caught per file

Pipeline continues processing remaining files

---

## 📜 License

This project is licensed under the MIT License.

---

## 📬 Author

Created by ShivenduShivu
