# Depression Connect Pipeline
This repository contains the code and resources for the Depression Connect Project, which aims to proccess, and analyze forum-style datasets (accounts, groups, topics, messages). The pipeline cleans raw CSV data, anonymizes user identifiers and message text, and produces analytics-ready outputs. The main branch is `main`. There is also a `dev` branch for developing features.

## Project Structure

<pre>
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
<pre>

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

4. **Prepare your data**:
   Place your raw CSV files (accounts.csv, groups.csv, topics.csv, messages.csv) in the `data/` directory. If you don't have the data folder, you can create it by running:
   ```bash
   mkdir data
   ```
5. **Run the data processing pipeline**:
   ```bash
   make run
   ```
   This will execute the data processing pipeline, generating cleaned and anonymized CSV files in the `output/` directory. It may take some time depending on the size of your dataset.

6. **start the API server:**
> [!WARNING]
> The API server requires the processed data to be present in the `output/` directory. Ensure you have run the data processing pipeline before starting the API.

> [!IMPORTANT]
> At this time, the API server only works locally and is not configured for deployment. This feature only exists as a placeholder for future development.

   ```bash
   make api
   ```
   The API server will be accessible at `http://localhost:8000/docs`.

7. **start the Streamlit app:**
   ```bash
   make app
   ```
   The Streamlit app will be accessible at `http://localhost:8501`.