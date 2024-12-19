# Data-Factory-Databricks-Multi-Layer-ETL-Pipeline-Azure
An ETL pipeline using Azure Data Factory and Databricks will be used to implement the Bronze-Silver-Gold architecture. Processes raw data (Bronze), cleans it (Silver), and prepares it for analytics (Gold). Features Delta Lake transformations, Azure integration, and detailed documentation with screenshots.

# Databricks-Bronze-Silver-Gold-ETL-Pipeline  

This repository demonstrates a robust and scalable **Azure Data Factory (ADF)** pipeline, integrating seamlessly with **Databricks** to implement the **Bronze, Silver, and Gold** data architecture for efficient data processing and transformation.  

## 🚀 Overview  
This pipeline is designed to handle data ingestion, processing, and transformation in three layers:  
- **Bronze Layer**: Raw, unprocessed data ingested from various sources.  
- **Silver Layer**: Cleaned and standardized data, prepared for downstream analysis.  
- **Gold Layer**: Curated datasets optimized for reporting, analytics, and business intelligence.  

## 🛠️ Features  
- **Azure Data Factory Integration**: Orchestrates and automates data movement and transformation workflows.  
- **Databricks Layers**: Implements the Bronze-Silver-Gold architecture using Delta Lake.  
- **Scalability**: Designed for high-volume data processing with distributed computing in Databricks.  
- **Flexibility**: Supports multiple data sources and destinations.  
- **Extensibility**: Easily customizable for additional transformations and business logic.  


## 🖼️ Screenshots  

### 1. Azure Data Factory Pipeline Overview
![image](https://github.com/user-attachments/assets/63496fc5-97bd-4318-a689-6252926e2ae3)


### 2. Merge both Pipelines for Transformation  
![image](https://github.com/user-attachments/assets/186ce10f-fd43-498d-82b4-45a038e6965d)

### 3. Pipeline for Transformation in Databricks Notebooks
![image](https://github.com/user-attachments/assets/fa10b368-9be0-42c1-b84a-d2235d7a6d3b)

## 🛠️ Tools & Technologies  
- **Azure Data Factory**: For orchestrating the pipeline.  
- **Databricks**: For data transformation using Delta Lake.  
- **Delta Lake**: To store data in Bronze, Silver, and Gold layers.  
- **Azure Data Lake Storage Gen2**: For scalable and secure storage.  

## 🧩 Pipeline Workflow  
1. **Data Ingestion**: Data is ingested into the Bronze layer through Azure Data Factory.  
2. **Bronze Layer**: Stores raw data as-is.  
3. **Silver Layer**: Applies cleaning, filtering, and standardization in Databricks.  
4. **Gold Layer**: Prepares data for analytics and reporting.    


