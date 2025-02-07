# BASIC DATA LAKEHOUSE
## Basic research project on Data Lakehouse and the tools used for Data Lakehouse.
## This is the architecture of a Data Lakehouse.
![image](https://github.com/user-attachments/assets/c7c8a51e-d0ec-4cbd-82a5-23f71182ea75)


+ Docker <br>
Acts as a containerization platform, ensuring that all components run in isolated environments.
Helps in deploying scalable and portable applications.
+ MinIO (Data Source & Storage Layer)<br>
A high-performance, S3-compatible object storage system.
Used to store raw data in the Bronze layer, processed data in the Silver layer, and refined data in the Gold layer.
+ Apache Spark (Processing Engine)<br>
A powerful distributed computing framework for big data processing.
Processes raw data from MinIO and moves it through different layers (Bronze → Silver → Gold).
+ Apache Hive Metastore<br>
Serves as a metadata repository, storing information about table schemas and partitions.
Essential for managing structured data in the Data Lakehouse.
+ Apache Airflow (Scheduling & Workflow Management)<br>
Used for orchestrating and automating data workflows.
Ensures that data processing and movement between layers happen systematically.
+ Trino (Query Engine)<br>
A high-performance distributed SQL engine for querying large datasets.
Enables fast and interactive data exploration across structured and semi-structured data.
+ Apache Superset (Visualization Tool) <br>
An open-source data visualization and business intelligence tool.
Allows users to create interactive dashboards and analyze data from multiple sources, including Trino.
Helps in making data-driven decisions with an intuitive UI and various visualization options.

### Schema Diagram <br>

![Screenshot 2025-02-07 101012](https://github.com/user-attachments/assets/16c64b98-e5d1-4992-9d2c-7d9e1ae4aee7)











