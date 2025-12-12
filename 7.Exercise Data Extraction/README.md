

# Data Extraction - JSON, S3, SQL, API

What does this document defines and describes?
### Section 1: Extracting Data from JSON Files
Problem description
1.	Extract and Parse Customers JSON 
2.	Extract Nested Orders JSON

### Section 2: Extracting Data from AWS S3
3.	Extract CSV from S3 (Boto3)
4.	Extract Parquet from S3 (Boto3)

### Section 3: Extracting Data from SQL Databases & APIs
5.	Extract Sales Data from SQL Database
6.	Fetch Data from an API
  
        Correct URL:
        The URL provided explicitly targets Sofia's weather page, which is appropriate for your use case.
        
        User-Agent Header:
        
        Including headers to mimic a browser is good practice and helps avoid being blocked.
        
        Structured Output:
        
        Returns a clear DataFrame with standardized columns (city, temperature, description).
    	
# 8.	Validate All Data Before Loading
      Sales Data from DB
      Orders Data from local File
      Customer Data from local File
      Merged Data from Orders and Customers
      Weather Data from API
      
# 9.	Make needed transformations

# 10.	LOAD Locally in the Project folder OUTPUT
    Save in JSON Format sales data from db
    Save in JSON Format orders
    Save in JSON Format customers
    Save in CSV Format orders cleaned data
    Save in CSV Format weather data
    
# 11.	LOAD the Data in AWS S3 as CSV and JSON



-> run etl_pipeline and got 
<img width="1879" height="993" alt="image" src="https://github.com/user-attachments/assets/c5a7a911-2869-426e-9848-ec27adbf859a" />

we can see the files in S3

<img width="1919" height="487" alt="image" src="https://github.com/user-attachments/assets/d2a7784f-3d0f-4993-aeeb-2d7cb01f309a" />

