
![Logo](https://1000logos.net/wp-content/uploads/2021/04/Adobe-logo.png)




# Data Accelerator Code Repository

Our objective is to develop a middleware solution capable of seamlessly transforming diverse customer data formats into standardized formats compatible with Adobe Experience Platform (AEP), specifically XDM JSON, XDM Parquet, or delimited files. This solution aims to streamline the process of data ingestion and preprocessing, providing essential ETL (Extract, Transform, Load) functionalities, advanced data profiling, and customizable transformation capabilities to address specific business requirements.

This solution is designed for organizations that receive data from customers in varying formats and need to unify and standardize it for further analysis or integration with AEP. It's suitable for scenarios where data needs to be cleansed, standardized, and enriched with additional insights before being ingested into AEP.


<br />


> [!NOTE]
> The sample input comprises raw data files representing various customer-related information, product details, order records, and other relevant data. These files may come in formats like CSV, JSON, or Parquet, reflecting the diverse sources from which data is collected.



<br />


## Code Transformation
Our code repository showcases PySpark transformations tailored for data preprocessing and standardization. It covers essential steps such as data type conversion, handling missing values, applying custom transformations (e.g., extracting primary identifiers), managing timestamp columns, filtering records based on specified conditions, and aggregating data for meaningful insights. Additionally, it demonstrates grouping data by key identifiers, aggregating on specific column or list of columns, and structuring outputs for seamless integration with downstream systems.



<br />

> ### Why SageMaker?
> We chose Amazon SageMaker for its comprehensive suite of ETL (Extract, Transform, Load) features, which perfectly align with our requirements. SageMaker provides capabilities for data collection, file lookup, incremental loads, out-of-the-box (OOTB) custom code transformations, and folder-level processing—all essential components for building our middleware solution efficiently.
Through SageMaker's Data Wrangler, we can easily create data flows to ingest, preprocess, and transform data from diverse sources, including Amazon S3. The visual interface simplifies the creation of data transformation steps, enabling us to apply data type conversions, fill missing values, and perform advanced analytics effortlessly.
Furthermore, SageMaker offers flexibility in incorporating custom PySpark transformations, as demonstrated in our code repository. This allows us to address specific business requirements and implement complex data processing logic tailored to our use case.
In summary, SageMaker provides a powerful yet user-friendly platform for building end-to-end data pipelines, facilitating seamless data transformation and integration with Adobe Experience Platform (AEP) or other analytics platforms.

<br />

### Step-by-step guide to creating a data flow in Data Wrangler in Amazon SageMaker to achieve your requirements
1. Navigate to Data Wrangler: Go to the Amazon SageMaker console and select "Data Wrangler" from the left-hand menu.
2. Create a New Flow: Click on "Create Flow" to start a new data flow.
3. Select Data Source: Choose "Amazon S3" as the data source. Select the bucket "data-accelerator" and the folder "inbound" where your CSV files are located.
4. Import Data: Import the CSV file from the selected folder.
5. Add Transformation Steps:
   - Data Type Transformation: Click on the "+" icon to add a transformation step. Choose "Data Type" transformation, and apply it to the necessary columns to ensure correct data types.
   - Fill Missing Values: Add another transformation step by clicking the "+" icon. Choose "Fill Missing Values" and configure it to fill empty email values with null.
   - Custom Transformation: For the custom transformations, you need to define a Pyspark script: Click on the "+" icon to add a transformation step.  Choose "Custom Transformation". Write Pyspark code to define the custom function to extract the primary identifier and apply it to create a new column "primaryIdentifier". Also, write code to identify timestamp-type columns, convert timestamp format, filter rows with status "Active", get all column names except the grouping column ("email"), and group data by orderID while aggregating product information into a list.
  

  
