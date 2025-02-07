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

Benefit <br>
1. Clear and Intuitive Structure
Data is divided into Fact Tables and Dimension Tables, making it easy to understand and manage.
Each dimension table stores relevant details, reducing data redundancy.
2. High Query Performance for Analytics
Fact Table (fact_movie) contains transactional and statistical data such as budget, revenue, and popularity, enabling easy aggregation.
Dimension Tables (dim_movie, dim_cast, dim_crew, dim_genre, dim_keyword, dimdate) organize data into meaningful categories for faster queries.
Foreign keys (id) link tables efficiently, optimizing data retrieval.
3. Supports Multi-Dimensional Analysis
dimdate enables analysis based on days, months, years, quarters, weeks, etc.
dimgenre and movie_genres facilitate genre-based analysis.
dimcast and dimcrew track actors and production crew members.
4. Scalable and Flexible
New dimension tables can be added without significantly impacting the system.
If additional analysis criteria (e.g., country of production) are required, a new dim_country table can be introduced without altering the existing structure.
5. Supports Complex Queries with High Performance
Bridge tables (movie_cast, movie_crew, movie_genres, movie_keyword) efficiently handle many-to-many relationships while maintaining query speed.
SQL functions such as SUM(), AVG(), and COUNT() can be used seamlessly for aggregation without complex processing.

### Some visualized charts have been created.
#### Chart: Distribution of High-Revenue Movie Genres Across Four Seasons <br>
![image](https://github.com/user-attachments/assets/8cfbf7a9-74af-43fd-8fa8-2296388fc595)
#### Wordcloud Chart of Popular Keywords. <br>
![image](https://github.com/user-attachments/assets/13d99260-033f-4f77-8417-28705494f9c5)
Chart: Top 20 Actors with the Highest Total Vote Count and Average Vote Score. <br>
![image](https://github.com/user-attachments/assets/36eaa027-3a5d-485d-85aa-d33761b929f6)












