# News Data ETL: AI-Powered Emotion Analyzer 📰
A modular multi-source ETL following the **Medallion Architectur**e. It extracts news data from global APIs, transforms it into structured analytical tables via **PySpark**, and prepares high-quality datasets for **AI-driven** sentiment and emotion analysis.

## Description ℹ️

![Architecture](https://github.com/itsfelipe-dev/NewsDataEmotionAnalyzerETL/blob/master/docs/assets/ELT_architecture.gif?raw=true)

This is an Extract, Transform, and Load (ETL) pipeline designed to gather news articles from [The Washington Post](https://www.washingtonpost.com/),  [The New York Time](https://www.nytimes.com) , and [The Guardian](https://www.theguardian.com/).
 
Implements a modular design lifecycle managed via source-agnostic (allow for adding new news sources by just creating a new adapter class):

**Bronze:** 
- **Data Extraction:** Get articles using a Strategy Pattern with dedicated adapters for The Washington Post, The New York Times, and The Guardian.

- **Data Storage:** Persists raw JSON responses in Amazon S3 using Hive-style partitioning 

**Silver:** 
- **Data Processing:** Employs PySpark to transform raw JSON into optimized Delta Lake tables.

- **Purification:** Handles HTML stripping (BeautifulSoup), text normalization, and schema enforcement.

- **Referential Integrity:** Generates deterministic Surrogate Keys (SHA-256) to tables across the lake.

**Gold**: (WIP) 
- **Emotion Analysis:** Applies an AI model to analyze the emotional content and sentiment of sanitized articles.

- **Data Persistence:** Stores the final high-value emotional insights in DynamoDB for fast, low-latency access by the dashboard.

- **Aggregated Views:** Creates trend reports and cross-source sentiment comparisons.




## Data Model 🗄️:
To ensure scalability, traceability, and efficient querying, the project uses a Star Schema data model optimized for analytical workloads.
The schema is designed to:
- Support multiple news sources
- Preserve source lineage across all entities
- Enable incremental ingestion, reprocessing, and historical replay

**[Database Diagram](https://dbdiagram.io/d/NewsDataEmotionAnalyzerETL-6988d72cbd82f5fce207e755)**

## Purpose 🎯:

This project aims to provide a comprehensive ETL pipeline for gathering, processing, and analyzing news data from reputable sources. By incorporating emotional analysis, it enables deeper insights into the sentiment and tone of news articles over time. The use of Docker containers ensures portability and scalability, while Apache Spark and Apache Airflow offer robust orchestration and management capabilities.
