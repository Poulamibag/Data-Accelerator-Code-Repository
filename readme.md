
![Logo](https://1000logos.net/wp-content/uploads/2021/04/Adobe-logo.png)




# Data Accelerator Code Repository

Our objective is to develop a middleware solution capable of seamlessly transforming diverse customer data formats into standardized formats compatible with Adobe Experience Platform (AEP), specifically XDM JSON, XDM Parquet, or delimited files. This solution aims to streamline the process of data ingestion and preprocessing, providing essential ETL (Extract, Transform, Load) functionalities, advanced data profiling, and customizable transformation capabilities to address specific business requirements.

This solution is designed for organizations that receive data from customers in varying formats and need to unify and standardize it for further analysis or integration with AEP. It's suitable for scenarios where data needs to be cleansed, standardized, and enriched with additional insights before being ingested into AEP.


<br /><br />


> [!NOTE]
> The sample input comprises raw data files representing various customer-related information, product details, order records, and other relevant data. These files may come in formats like CSV, JSON, or Parquet, reflecting the diverse sources from which data is collected.



<br /><br />


## Code Transformation
Our code repository showcases PySpark transformations tailored for data preprocessing and standardization. It covers essential steps such as data type conversion, handling missing values, applying custom transformations (e.g., extracting primary identifiers), managing timestamp columns, filtering records based on specified conditions, and aggregating data for meaningful insights. Additionally, it demonstrates grouping data by key identifiers, aggregating on specific column or list of columns, and structuring outputs for seamless integration with downstream systems.


<br /><br />


> ### Why SageMaker?
> We chose Amazon SageMaker for its comprehensive suite of ETL (Extract, Transform, Load) features, which perfectly align with our requirements. SageMaker provides capabilities for data collection, file lookup, incremental loads, out-of-the-box (OOTB) custom code transformations, and folder-level processing—all essential components for building our middleware solution efficiently.
Through SageMaker's Data Wrangler, we can easily create data flows to ingest, preprocess, and transform data from diverse sources, including Amazon S3. The visual interface simplifies the creation of data transformation steps, enabling us to apply data type conversions, fill missing values, and perform advanced analytics effortlessly.
Furthermore, SageMaker offers flexibility in incorporating custom PySpark transformations, as demonstrated in our code repository. This allows us to address specific business requirements and implement complex data processing logic tailored to our use case.
In summary, SageMaker provides a powerful yet user-friendly platform for building end-to-end data pipelines, facilitating seamless data transformation and integration with Adobe Experience Platform (AEP) or other analytics platforms.
