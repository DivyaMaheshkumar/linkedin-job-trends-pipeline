# linkedin-job-trends-pipeline
This project is implemented using serverless AWS services to ingest, transform and analyze LinkedIn job posts data for the year 2024 across different geographic locations, varied experience levels and industry domains to understand different job market trends for informed decision making for workforce analytics and career forecasting.

```mermaid
flowchart TD
    %% Define Nodes
    Sources[Raw Data Sources: JSON, CSV, Parquet]
    S3Landing[AWS S3: Landing Zone]
    Lambda[AWS Lambda Function]
    CloudWatch[AWS CloudWatch Logs]
    Glue[AWS Glue ETL Job: PySpark]
    S3Integration[AWS S3: Integration Layer]
    Catalog[AWS Glue Data Catalog]
    Athena[AWS Athena: SQL Analytics]
    QuickSight[Amazon QuickSight: BI Dashboards]

    %% Define Flow
    Sources --> S3Landing
    S3Landing -->|File Upload Event| Lambda
    Lambda -->|Logs Execution & Run ID| CloudWatch
    Lambda -->|Triggers On Demand| Glue
    
    subgraph Glue Transformations
        Glue --> T1[Flatten Nested JSON]
        Glue --> T2[Schema Normalization & Deduplication]
        Glue --> T3[Dimension & Fact Modeling]
    end

    Glue --> S3Integration
    S3Integration --> Catalog
    Catalog --> Athena
    Athena --> QuickSight
