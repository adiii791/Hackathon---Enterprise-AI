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

Tools Used - 
  Python
  Pandas
  Jupyter Notebook
  VS Code

· This project is focused on data cleaning and preparation, which is a required first step before applying AI or ML models in enterprise systems.
