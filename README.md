# Global Electronics Data Pipeline

## Overview
This project automates the extraction, transformation, and loading (ETL) of sales data from CSV files into a MySQL database,It sets up an organized data storage system for business insights and number crunching. 

## Key Features
- Data cleaning and preprocessing
- Automated SQL table creation
- Dimension and fact table modeling
- Foreign key and index management

## Data Sources
- Sales.csv
- Customers.csv
- Stores.csv
- Products.csv
- Exchange_Rates.csv

## Technical Components
- Python libraries: pandas, sqlalchemy, numpy
- MySQL database
- ETL process with:
  * Missing value handling
  * Data type conversion
  * Column sanitization

## Setup Instructions
1. Install required Python libraries:
   ```
   pip install pandas sqlalchemy pymysql numpy
   ```
2. Configure database connection in script
3. Ensure CSV files are in `data_sets/` directory
4. Run the Python script to populate MySQL database

## Power BI Visualization
![Dashboard Screenshot 1](screenshots/dashboard_overview.png)
![Dashboard Screenshot 2](screenshots/sales_analysis.png)

### Dashboard Insights
- Sales performance tracking
- Customer segmentation
- Product analysis
- Geographic sales distribution

## Database Schema
- `fact_sales`: Central sales transactions
- `dim_customers`: Customer details
- `dim_stores`: Store information
- `dim_products`: Product catalog
- `dim_exchange_rates`: Currency exchange data

