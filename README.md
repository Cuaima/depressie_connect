# Depression Connect Pipeline
This repository contains the code and resources for the Depression Connect Project, which aims to analyze and connect various aspects of depression through data-driven approaches.
This repository contains a data processing and anonymization pipeline for forum-style datasets (accounts, groups, topics, messages). The pipeline cleans raw CSV data, anonymizes user identifiers and message text, and produces analytics-ready outputs.

## Project Structure

project/
├── data/
│   ├── accounts.csv
│   ├── groups.csv
│   ├── topics.csv
│   ├── messages.csv
│
├── output/
│   ├── *_cleaned_anonymized.csv
│   ├── anonymization_mapping.csv
│   ├── anonymized_top_100.csv
│   ├── topics_with_post_counts.csv
│   ├── word_count_message_text.csv
│   └── ...
│
├── src/
│   ├── processor.py          # Main Pandas-based pipeline
│   ├── spark_anonymizer.py   # PySpark MessageText anonymization
│   ├── api.py
│   ├── app.py
│   └── analysis/
│
├── tests/
│   └── test_processor.py
│
├── requirements.txt
├── Makefile
├── CITATION.cff
├── LICENSE
├── .gitignore
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
└── README.md

## System Requirements
- Python 3.8 or higher
- Java 8 or higher (for PySpark)
- Git
- Docker (optional, for containerized execution)
- Anaconda (optional, for managing Python environments)

## Getting Started
To set up the project environment, install dependencies, and run the data processing pipeline, follow these steps:
1. **Clone the repository**:
- Using HTTPS:

   ```bash
   git clone https://github.com/Cuaima/depressie_connect.git
   ```

- Using SSH ():

   ```bash
   git clone git@github.com:Cuaima/depressie_connect.git
   ```

2. **Navigate to the project directory**:
   ```bash
   cd depressie_connect_project
   ```

3. **Create a virtual environment and install dependencies**:
   ```bash
   make install
   ```