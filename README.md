# Data Engineering Challenge

This repository contains my solution for the data engineering challenge.

## Project Structure
```data_engineering_challenge/
├── data/
│   ├── sales.csv
│   ├── products.csv
│   └── stores.csv
├── src/
│   ├── __init__.py
│   ├── data_preparation.py
│   ├── data_transformation.py
│   ├── data_export.py
|   ├── pipeline.py
│   └── utils.py
├── tests/
│   ├── __init__.py
│   ├── test_data_preparation.py
│   ├── test_data_transformation.py
│   ├── test_data_export.py
│   └── test_utils.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── logs/
│   └── app.log
├── docs/
│   └── README.md
├── output/
│   ├── enriched_data/
│   └── revenue_insights/
├── .gitignore
├── requirements.txt
├── setup.py
└── main.py
```

## Setup

1. **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2. **Set Up PySpark**:
    Follow the instructions to install PySpark from the official documentation.

3. **Configuration**:
    Ensure the configuration files (`config/tables_config.json` and `config/output_config.json`) are correctly set up.

## Running the Pipeline

To run the data processing pipeline, execute the following command:
```
python main.py
```

## Testing
To run the tests, use the following command:
```
pytest
```

## Logging
Logs are stored in the logs/app.log file. The logging configuration is set up in the utils.py module.

## Continuous Integration
The project includes a GitHub Actions workflow for continuous integration. The workflow is defined in .github/workflows/ci.yml.

## Documentation
Detailed documentation for each module can be found in the docs/README.md file.

