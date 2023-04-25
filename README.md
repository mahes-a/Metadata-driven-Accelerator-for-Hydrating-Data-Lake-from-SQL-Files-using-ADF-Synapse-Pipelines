# Metadata-driven-Accelerator-for-Hydrating-Data-Lake-from-SQL-Files-using-ADF-Synapse-Pipelines
Metadata driven Accelerator for Hydrating Data Lake from SQL &amp; Files using ADF/Synapse Pipelines

Metadata driven Accelerator for Hydrating Data Lake from SQL &amp; Files using ADF/Synapse Pipelines

This Accelerator demonstrates copying a number of tables from multiple SQL servers and multiple sql databases and files to Azure Data Lake Storage. You can apply the same pattern in other copy scenarios as well. For example, copying tables from Oracle to Azure Blob. When establishing Analytics workloads in Azure the first step is to establish pipelines that can pull data for initial loads and ongoing delta loads to Azure data lake storage. Furthermore there are multiple ways to capture delta changes like Change Capture, CDC, Watermark in SQL, The template provides samples to handle all the scenarios in a generic meta-driven manner

The accelerator contains.

•	SQL Scripts to create the metadata table that holds all SQL object details and their copy mechanisms (CDC, watermark etc.)
•	Generic ADF Pipelines which handles SQL Delta Copy mechanisms of CDC, Change Tracking, Watermark, Files, Full loads.
    There are Driver and Executor pipelines.
    The Driver Pipeline reads the data from metadata tables and executes the respective delta mechanism executor pipelines.


Step 1: -
To enable import, Source Control needs to be enabled.
https://learn.microsoft.com/en-us/azure/data-factory/source-control#connect-to-a-git-repository

Step 2: -
In ADF Home Page chose “Pipeline Templates”
 ![image](https://user-images.githubusercontent.com/120069348/234419321-907c204d-9d88-455a-b1a2-f900d00171d9.png)

Step 3: -
Import the Template Zip file “PL_Generic_Ingest_Executor_Template.zip” 
 ![image](https://user-images.githubusercontent.com/120069348/234419390-9c9a8afc-7083-417a-98d1-522fdb778140.png)


 
Step 4: -
Click OK to import the template.
![image](https://user-images.githubusercontent.com/120069348/234426349-fd515ee1-50aa-44ad-aba4-af6051a085cd.png)



Step 5: -
Create or select the Configuration SQL DB linked service where Control table and tracking version table is present, select the Source SQL linked service and the ADLS Gen2 linked service and select use the template.

![image](https://user-images.githubusercontent.com/120069348/234419505-79ba4044-bf12-45d0-9683-0eecfbeac0a1.png)

 
The Source SQL Link Service should be parametrized.
![image](https://user-images.githubusercontent.com/120069348/234419523-9339c8cf-a611-4f56-8366-72873f62ad8f.png)

  
Step 5: -
From the Author Tab Select the PL_Generic_Ingest_Executor_Template
 
![image](https://user-images.githubusercontent.com/120069348/234420387-f02a29cf-911c-42cc-a555-2034b17c3928.png)


Step 7: -
From the Author Tab under pipeline section, we find the “PL_Generic_Ingest_Executor_V1” created from the template. The Other Executor pipelines are listed as below.
PL_Generic_File_Load
PL_Generic_Full_Load
PL_Generic_SQL_CHANGETRACKING
PL_GENERIC_INGEST_WATERMARK
PL_GENERIC_SQL_CDC_INGEST
 

Step 8:
Execute the SQL Scripts in the Configuration SQL server and insert values into ControlTableIntegrated table. Configure the table name and values in corresponding tables. 
For Example: - Add the table name and watermark value in [table_store_watermark_value]
Refer to below to learn more on the methodology and tables used in watermark mode.
https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-multiple-tables-portal
For Change tracking refer to
[Incrementally copy data by using change tracking in the Azure portal - Azure Data Factory | Microsoft Learn](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-change-tracking-feature-portal)

For CDC refer to
https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-change-data-capture-feature-portal

Configure the entries in ControlTableIntegrated and execute the pipelines.
 


