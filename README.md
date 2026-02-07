# Hackathon -- Enterprise-AI

     · Enterprise Data Cleaning Pipeline

About Project -
the project is related to the how raw enterprise data can be cleaned and prepared for AI / analytics pipelines.
In real world, data is never clean. This pipeline shows how raw CSV files are processed step-by-step and converted into usable cleaned datasets.

Why this project -
While working with datasets, we noticed common problems like:
   · Missing values
   · Different column name formats
   · Mixed date formats
   · Inconsistent text values

These problems make it difficult to directly use data for ML or analysis.
This project focuses on fixing these issues.

 · Folder Structure -
raw_data/        -> Original CSV files
cleaned/         -> Cleaned output files
features/        -> Feature level datasets
reports/         -> Logs / reports
data_exploration.ipynb -> Data checking & comparison
enterprise_pipeline.py -> Main pipeline code

Step-by-Step Instructions to Run the Pipeline
 1. Prerequisites -
Make sure the following are installed:
Python 3.9 or above
Required Python libraries:
  pip install pandas numpy

3. Place Input Files
Copy the four CSV files provided by the hackathon organizers into the raw_data/ directory:
 weather.csv
 station_region.csv
 activity_logs.csv
 reference_units.csv

4. Run the Pipeline
  From the project root directory, execute:
      python enterprise_pipeline.py

5. Verify Outputs
 After successful execution:
  Cleaned datasets will be available in:
      output/cleaned/
Unified merged dataset:
output/unified_dataset.csv

Feature datasets:

output/features/features_v1.csv
output/features/features_v2.csv
Governance and audit metadata:
output/metadata/pipeline_metadata.json
Execution logs are stored in pipeline_execution.log.

 What the pipeline does -
    · Loads raw CSV files using pandas
    · Checks missing values and schema
    · Cleans columns (case, naming, formats)
    · Handles null values
    · Converts dates into standard format
    · Saves cleaned datasets

Data Verification -
 Jupyter Notebook is used to:
     · Load raw data
     · View first few rows
     · Compare raw vs cleaned data
     ·  Verify column names and values
 This helps to clearly understand how data is transformed.

 How to run -  
    python enterprise_pipeline.py
 Cleaned CSV files will be generated automatically.

 ·Tools Used - 
  Python
  Pandas
  Jupyter Notebook
  VS Code

 · Failure Scenario and Recovery Approach-
    Example Failure Scenario
       Missing or incorrectly named input CSV file
       If one or more required CSV files are missing from the raw_data/ directory or have incorrect filenames, the pipeline      will fail during the ingestion stage and log an error message.

 · Recovery Approach -

     Verify that all required CSV files are present in raw_data/
     Ensure filenames exactly match those expected by the pipeline
     Re-run the pipeline
     The pipeline is idempotent, meaning it can be safely re-executed without causing duplicate or inconsistent outputs.

  · Known Limitations and Potential Improvements
      Known Limitations -
Activity and weather datasets are merged only on common identifiers; advanced temporal joins are not implemented.
Unit conversion references are stored as metadata and not fully applied during feature generation.
Outlier handling is based on basic domain rules and not statistical anomaly detection.

     Potential Improvements -

Add schema validation using tools like Pandera or Great Expectations
Introduce automated unit conversion during feature engineering
Enhance feature engineering with seasonal trends and anomaly detection
Add pipeline orchestration using Airflow or Prefect for production readiness
