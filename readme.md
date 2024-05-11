
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

### Step-by-step guide to creating a data flow in Data Wrangler in Amazon SageMaker to achieve your requirements:
1. Navigate to Data Wrangler: Go to the Amazon SageMaker console and select "Data Wrangler" from the left-hand menu.
2. Create a New Flow: Click on "Create Flow" to start a new data flow.
3. Select Data Source: Choose "Amazon S3" as the data source. Select the bucket "data-accelerator" and the folder "inbound" where your CSV files are located.
4. Import Data: Import the CSV file from the selected folder.
5. Add Transformation Steps:
   - Data Type Transformation: Click on the "+" icon to add a transformation step. Choose "Data Type" transformation, and apply it to the necessary columns to ensure correct data types.
   - Fill Missing Values: Add another transformation step by clicking the "+" icon. Choose "Fill Missing Values" and configure it to fill empty email values with null.
   - Custom Transformation: For the custom transformations, you need to define a Pyspark script: Click on the "+" icon to add a transformation step.  Choose "Custom Transformation". Write Pyspark code to define the custom function to extract the primary identifier and apply it to create a new column "primaryIdentifier". Also, write code to identify timestamp-type columns, convert timestamp format, filter rows with status "Active", get all column names except the grouping column ("email"), and group data by orderID while aggregating product information into a list.

> [!NOTE]
> Dataflow Name :  `CustomerData.flow`

<br />

## Code Repository
### Transformation 1 :
To group data by orderID and lookup data from lookup table 

<br />

**Code:** <br />
`from pyspark.sql.functions import concat_ws, col, when, to_timestamp, date_format, collect_list, struct, sum, firstdf = df.groupBy("orderID").agg(collect_list("Sub-Category").alias("Agg_Sub_Category"))`
` `
` `

<br />

**Sample Input:**
orderID | productPrice | productSKU_ID | productName | orderDate | email | phoneNumber | status
:---: | :---: | :---: | :---: | :---: | :---: | :---: | :---:
af4c6bc4-1e01-4e07-9464-d8acab64888d | 4278.73 | 4G09V46DQ64 | Squash - Acorn | 7/27/2023 | jscottini0@washington.edu | null | Active
5e5cf0d0-6e6f-4979-84d2-fe40f1608af0 | 78.7 | 6PE8MF9NP67 | Lettuce - Sea / Sea Asparagus | 6/4/2023 |  | 899-252-7372 | Active
1271f168-89f6-4961-a4d8-18461a06fcfd | 8229.59 | 4DF3DR5HK16 | Ham - Cooked Italian | 12/3/2024 | ehuffy2@army.mil | 564-990-2338 | InActive
fe9d2294-4848-4127-8836-07965cfb1cf9 | 8715.01 | 2V87DY4YK10 | Muffin - Mix - Strawberry Rhubarb | 11/13/2023 | kspikings3@slashdot.org | 668-104-0212 | InActive
9a447f3e-deb9-4923-9bb9-42bf0a70394b | 6105.38 | 7H99QY2QU62 | Plastic Arrow Stir Stick | 8/16/2023 | celen4@weebly.com | 880-774-9237 | InActive
b9934c8b-003b-4ccb-932f-992637fad535 | 954.75 | 8CT7K73UF98 | Wine - Magnotta - Cab Sauv | 3/8/2023 | ajagger5@jiathis.com | 205-373-2281 | Active
ccec6b04-94fa-4985-8bc1-b34b66f2d7e3 | 837.22 | 5YD4HW7RF80 | Cake - Box Window 10x10x2.5 | 9/1/2024 | fserrier6@psu.edu | 491-298-6973 | InActive
8376e403-2260-4807-bb22-2051f1c0dfd2 | 3542.11 | 2QD4AJ2YH30 | Salmon - Sockeye Raw | 11/18/2023 | rforrest7@rakuten.co.jp | 397-377-0169 | InActive
c0aaa7dd-ee6c-476e-b5f6-9a25116e1733 | 410.9 | 5NK7W82HR46 | Pants Custom Dry Clean | 5/28/2023 | amatz8@bluehost.com | 753-974-7959 | InActive
52b86a79-a793-4279-9e83-c0d769e8b420 | 7786.83 | 9FY1RM8HW13 | Fish - Bones | 6/23/2023 | iberthot9@e-recht24.de | 184-732-9659 | InActive
af4c6bc4-1e01-4e07-9464-d8acab64888d | 78.7 | 6PE8MF9NP67 | Lettuce - Sea / Sea Asparagus | 7/27/2023 | jscottini0@washington.edu | null | Active
af4c6bc4-1e01-4e07-9464-d8acab64888d | 837.22 | 5YD4HW7RF80 | Cake - Box Window 10x10x2.5 | 7/27/2023 | jscottini0@washington.edu | null | Active
5e5cf0d0-6e6f-4979-84d2-fe40f1608af0 | 954.75 | 8CT7K73UF98 | Wine - Magnotta - Cab Sauv | 6/4/2023 | null | 899-252-7372 | Active

<br />

**Lookup Table:**
Product ID | Sub-Category
:---: | :---:
4G09V46DQ64 | fruit
6PE8MF9NP67 | vegetable
6PE8MF9NP67 | vegetable
5YD4HW7RF80 | cake

<br />

**Sample Ouput:**
orderID | productLineItem | totalOrderValue | subCategory
:---: | :---: | :---: | :---:
af4c6bc4-1e01-4e07-9464-d8acab64888d|"[{4G09V46DQ64, ""Squash - Acorn"",  4278.73}, {6PE8MF9NP67, ""Lettuce - Sea / Sea Asparagus"", 78.8 }, {5YD4HW7RF80, ""Cake - Box Window 10x10x2.5"", 827.22 }]"|$5194.65|"[""fruit"", “vegetable”, “cake”]"
5e5cf0d0-6e6f-4979-84d2-fe40f1608af0|"[{6PE8MF9NP67, ""Lettuce - Sea / Sea Asparagus"", 78.7},{8CT7K73UF98, ""Wine - Magnotta - Cab Sauv"", 954.75}]"|$1033.45|[“vegetable”]
b9934c8b-003b-4ccb-932f-992637fad535|"[{8CT7K73UF98,""Wine - Magnotta - Cab Sauv"", 954.75}]"|$954.75|[]

<br /><br />

### Transformation 2 :
 To create array object from different columns into one column 

<br />

**Code:** <br />
`df = df.groupBy("orderID").agg(collect_list(struct("productSKU_ID", "productName", "productPrice")).alias("productLineItem"))`

<br />

**Sample Input:**
orderID | productPrice | productSKU_ID | productName | orderDate | email | phoneNumber | status
:---: | :---: | :---: | :---: | :---: | :---: | :---: | :---:
af4c6bc4-1e01-4e07-9464-d8acab64888d | 4278.73 | 4G09V46DQ64 | Squash - Acorn | 7/27/2023 | jscottini0@washington.edu | null | Active
5e5cf0d0-6e6f-4979-84d2-fe40f1608af0 | 78.7 | 6PE8MF9NP67 | Lettuce - Sea / Sea Asparagus | 6/4/2023 |  | 899-252-7372 | Active
1271f168-89f6-4961-a4d8-18461a06fcfd | 8229.59 | 4DF3DR5HK16 | Ham - Cooked Italian | 12/3/2024 | ehuffy2@army.mil | 564-990-2338 | InActive
fe9d2294-4848-4127-8836-07965cfb1cf9 | 8715.01 | 2V87DY4YK10 | Muffin - Mix - Strawberry Rhubarb | 11/13/2023 | kspikings3@slashdot.org | 668-104-0212 | InActive
9a447f3e-deb9-4923-9bb9-42bf0a70394b | 6105.38 | 7H99QY2QU62 | Plastic Arrow Stir Stick | 8/16/2023 | celen4@weebly.com | 880-774-9237 | InActive
b9934c8b-003b-4ccb-932f-992637fad535 | 954.75 | 8CT7K73UF98 | Wine - Magnotta - Cab Sauv | 3/8/2023 | ajagger5@jiathis.com | 205-373-2281 | Active
ccec6b04-94fa-4985-8bc1-b34b66f2d7e3 | 837.22 | 5YD4HW7RF80 | Cake - Box Window 10x10x2.5 | 9/1/2024 | fserrier6@psu.edu | 491-298-6973 | InActive
8376e403-2260-4807-bb22-2051f1c0dfd2 | 3542.11 | 2QD4AJ2YH30 | Salmon - Sockeye Raw | 11/18/2023 | rforrest7@rakuten.co.jp | 397-377-0169 | InActive
c0aaa7dd-ee6c-476e-b5f6-9a25116e1733 | 410.9 | 5NK7W82HR46 | Pants Custom Dry Clean | 5/28/2023 | amatz8@bluehost.com | 753-974-7959 | InActive
52b86a79-a793-4279-9e83-c0d769e8b420 | 7786.83 | 9FY1RM8HW13 | Fish - Bones | 6/23/2023 | iberthot9@e-recht24.de | 184-732-9659 | InActive
af4c6bc4-1e01-4e07-9464-d8acab64888d | 78.7 | 6PE8MF9NP67 | Lettuce - Sea / Sea Asparagus | 7/27/2023 | jscottini0@washington.edu | null | Active
af4c6bc4-1e01-4e07-9464-d8acab64888d | 837.22 | 5YD4HW7RF80 | Cake - Box Window 10x10x2.5 | 7/27/2023 | jscottini0@washington.edu | null | Active
5e5cf0d0-6e6f-4979-84d2-fe40f1608af0 | 954.75 | 8CT7K73UF98 | Wine - Magnotta - Cab Sauv | 6/4/2023 | null | 899-252-7372 | Active

<br />



**Sample Ouput:**
orderID | productLineItem | totalOrderValue | subCategory
:---: | :---: | :---: | :---:
af4c6bc4-1e01-4e07-9464-d8acab64888d|"[{4G09V46DQ64, ""Squash - Acorn"",  4278.73}, {6PE8MF9NP67, ""Lettuce - Sea / Sea Asparagus"", 78.8 }, {5YD4HW7RF80, ""Cake - Box Window 10x10x2.5"", 827.22 }]"|$5194.65|"[""fruit"", “vegetable”, “cake”]"
5e5cf0d0-6e6f-4979-84d2-fe40f1608af0|"[{6PE8MF9NP67, ""Lettuce - Sea / Sea Asparagus"", 78.7},{8CT7K73UF98, ""Wine - Magnotta - Cab Sauv"", 954.75}]"|$1033.45|[“vegetable”]
b9934c8b-003b-4ccb-932f-992637fad535|"[{8CT7K73UF98,""Wine - Magnotta - Cab Sauv"", 954.75}]"|$954.75|[]


<br /><br />

### Transformation 2 :
 To create array object from different columns into one column 

<br />

**Code:** <br />
`from pyspark.sql.functions import concat_ws, col, when, to_timestamp, date_format, collect_list, struct, sum, first`

`# Define a custom function to extract primary identifier
def extract_primary_identifier(email, phoneNumber):
    return when(col("email").isNotNull() & col("phoneNumber").isNotNull(), 
                concat_ws(", ", col("email"), col("phoneNumber"))) \
           .when(col("email").isNotNull(), col("email")) \
           .otherwise(col("phoneNumber"))`

`# Apply the custom function to create a new column "primaryIdentifier"
df = df.withColumn("primaryIdentifier", extract_primary_identifier(col("email"), col("phoneNumber")))`

`# Identify timestamp-type columns
timestamp_columns = [col_name for col_name, dtype in df.dtypes if dtype == "timestamp"]`

`# Convert timestamp format for each identified column
for column in timestamp_columns:
    df = df.withColumn(column, 
                       date_format(to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd'T'HH:mm:ss.SS'Z'"))`

`# Filter rows with status "Active"
df = df.filter(col("status") == "Active")`

`# Get all column names except the grouping column ("email")
other_columns = [c for c in df.columns if c != "email"]`

`# Group data by orderID and aggregate product information into a list
df = df.groupBy("orderID").agg(
    collect_list(struct("productSKU_ID", "productName", "productPrice")).alias("productLineItem"),
    sum("productPrice").alias("totalOrderValue"),
    first("orderDate").alias("orderDate"),
    first("email").alias("email"),
    first("phoneNumber").alias("phoneNumber"),
    first("primaryIdentifier").alias("primaryIdentifier")
)`
