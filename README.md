# AWS Lakehouse Solution Developer

This respository contains the **final project of `Spark and Data Lakes` of Udacity.** 

Also, some excercises and quizzes are hosted here.

If you want to see all the course material, [click here](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises).

## Project description

Designed and implemented a scalable lakehouse solution on AWS, leveraging AWS Glue, S3, Python, and Spark to process and curate semi-structured data for data science workflows.

### Key Accomplishments

* Built semi-structured Glue tables for data landing zones (customer_landing, step_trainer_landing, accelerometer_landing) and queried them using Athena for preliminary insights.
    
* Developed ETL pipelines in AWS Glue to sanitize customer and accelerometer data, storing results in trusted zones (customer_trusted, accelerometer_trusted).

 * Resolved critical data quality issues by cross-referencing step trainer IoT data with accelerometer and customer records to curate a clean and linked dataset (customers_curated).
    
* Created aggregated machine learning-ready datasets (machine_learning_curated) combining step trainer and accelerometer readings.
    
    
* Automated data processing workflows using AWS Glue Studio jobs to populate trusted and curated zones.

## Repository structure

```plaintext

├── notebooks : containing excercises of course, using Pyspark
├── project : project evidences such as Athena queries screenshot, python scripts for Glue jobs.
├── src : some scripts of sql and python of some excercises along the course
└── README.md


```

## Tech stack

With this course I practiced and improved your skills in

* Hadoop ecosystem
* Pyspark
* AWS Glue, AWS S3 and AWS Athena services
