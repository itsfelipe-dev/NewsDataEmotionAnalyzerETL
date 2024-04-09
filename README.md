# News Data ETL with Emotion Analysis 📰

## Initial Development Phase 🛠️:
The project is currently in its initial development phase, with the first portion implemented. At this stage, data extraction from articles post has been successfully completed, and the extracted data is stored in Amazon S3. The next steps will involve expanding the extraction process to include additional news sources, implementing data processing using PySpark, and integrating emotion analysis with an AI model. Contributions and feedback are welcome as the project progresses. Stay tuned for updates!

## Description ℹ️

Extract, Transform, and Load (ETL) pipeline designed to gather news articles from [The Washington Post](https://www.washingtonpost.com/),  [The New York Time](https://www.nytimes.com) , and [The Guardian](https://www.theguardian.com/). The extracted data is stored in Amazon S3, then processed using PySpark for cleaning and sanitization. The cleaned data is further analyzed for emotional content using an AI model, and the results are stored in DynamoDB. The entire pipeline is orchestrated and managed using Apache Spark and Apache Airflow within Docker containers. 🚀

## Components 🔧:

- **Data Extraction**: Scrapes articles from The Washington Post, The New York Times, and The Guardian.
- **Data Storage**: Stores extracted articles in Amazon S3.
  Data Processing: Utilizes PySpark for cleaning and sanitization of the extracted data.
- **Emotion Analysis**: Applies an AI model to analyze the emotional content of each article.
- **Data Persistence**: Stores the emotional analysis results in DynamoDB.
  Orchestration: Manages the entire pipeline using Apache Spark and Apache Airflow within Docker containers.

## Purpose 🎯:

This project aims to provide a comprehensive ETL pipeline for gathering, processing, and analyzing news data from reputable sources. By incorporating emotional analysis, it enables deeper insights into the sentiment and tone of news articles over time. The use of Docker containers ensures portability and scalability, while Apache Spark and Apache Airflow offer robust orchestration and management capabilities.
