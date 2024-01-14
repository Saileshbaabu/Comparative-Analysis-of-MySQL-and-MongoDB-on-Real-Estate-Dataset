import mysql.connector
import time

# MySQL database credentials
db_config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'realestate',
    'raise_on_warnings': True
}

# Queries
queries = [
    # First query
    """
SELECT 
    Town, 
    AVG(AssessedValue) AS AverageValue, 
    SUM(SaleAmount) AS TotalSales 
FROM 
    real_estate_table 
WHERE 
    ListYear > 2018 AND 
    (AssessedValue BETWEEN 100000 AND 500000) 
GROUP BY 
    Town 
HAVING 
    AVG(AssessedValue) > 150000 
ORDER BY 
    AverageValue DESC;
""",
"""
SELECT 
    Address, 
    Town, 
    SaleAmount, 
    AssessedValue, 
    (SaleAmount - AssessedValue) AS ProfitLoss, 
    CONCAT(ROUND(((SaleAmount - AssessedValue) / AssessedValue) * 100, 2), '%') AS ProfitLossPercentage 
FROM 
    real_estate_table 
WHERE 
    AssessedValue > 0 
ORDER BY 
    ProfitLoss DESC;

""",
"""
SELECT 
    YEAR(DateRecorded) AS YearOfSale, 
    Town, 
    COUNT(*) AS NumberOfSales, 
    AVG(SaleAmount) AS AverageSale 
FROM 
    real_estate_table 
WHERE 
    DateRecorded IS NOT NULL 
GROUP BY 
    YEAR(DateRecorded), 
    Town 
HAVING 
    COUNT(*) > 10 AND 
    AVG(SaleAmount) > 200000 
ORDER BY 
    YearOfSale DESC, 
    AverageSale DESC;
""",
# """
# #SELECT 
#     #Town,
#     #AVG(SaleAmount) AS AverageSale,
#     #(SELECT MAX(SaleAmount) FROM real_estate_table WHERE Town = r.Town AND ListYear = r.ListYear) AS MaxSaleInTownYear
# #FROM 
#     #real_estate_table r
# #WHERE 
#     #ListYear > 2019 AND SaleAmount > (SELECT AVG(SaleAmount) FROM real_estate_table)
# #GROUP BY 
#     #Town, ListYear
# #ORDER BY 
#     #AverageSale DESC;
# """,
"""
SELECT 
    YEAR(DateRecorded) AS SaleYear, 
    Town, 
    COUNT(*) AS TotalSales, 
    AVG(SaleAmount) AS AverageSaleAmount, 
    ((AVG(SaleAmount) - LAG(AVG(SaleAmount)) OVER (PARTITION BY Town ORDER BY YEAR(DateRecorded))) / LAG(AVG(SaleAmount)) OVER (PARTITION BY Town ORDER BY YEAR(DateRecorded))) * 100 AS YearOverYearGrowth 
FROM 
    real_estate_table 
WHERE 
    YEAR(DateRecorded) > 2015 
GROUP BY 
    YEAR(DateRecorded), 
    Town 
HAVING 
    COUNT(*) > 5 
ORDER BY 
    Town, 
    SaleYear;
""",
"""
SELECT 
    Town,
    YEAR(DateRecorded) AS Year,
    QUARTER(DateRecorded) AS Quarter,
    SUM(SaleAmount) AS TotalQuarterlySales,
    COUNT(*) AS NumberOfSales,
    AVG(SaleAmount) AS AverageSale
FROM 
    real_estate_table
GROUP BY 
    Town, YEAR(DateRecorded), QUARTER(DateRecorded)
HAVING 
    SUM(SaleAmount) > 5000000
ORDER BY 
    Year DESC, Quarter;
""",
# """
# SELECT 
#     Town,
#     ListYear,
#     SUM(SaleAmount) AS TotalSales,
#     (SELECT AVG(SaleAmount) FROM real_estate_table WHERE Town = r.Town AND ListYear = r.ListYear) AS AverageTownSale,
#     (SUM(SaleAmount) - (SELECT AVG(SaleAmount) FROM real_estate_table WHERE Town = r.Town AND ListYear = r.ListYear - 1) * COUNT(*)) AS YearlyDifference
# FROM 
#     real_estate_table r
# WHERE 
#     ListYear > 2019
# GROUP BY 
#     Town, ListYear
# HAVING 
#     YearlyDifference > 0
# ORDER BY 
#     YearlyDifference DESC;	
# """,
# """
# SELECT 
#     Town,
#     ListYear,
#     SUM(SaleAmount) AS TotalSales,
#     (SELECT AVG(SaleAmount) FROM real_estate_table WHERE Town = r.Town AND ListYear = r.ListYear) AS AverageTownSale,
#     (SUM(SaleAmount) - (SELECT AVG(SaleAmount) FROM real_estate_table WHERE Town = r.Town AND ListYear = r.ListYear - 1) * COUNT(*)) AS YearlyDifference
# FROM 
#     real_estate_table r
# WHERE 
#     ListYear > 2019
# GROUP BY 
#     Town, ListYear
# HAVING 
#     YearlyDifference > 0
# ORDER BY 
#     YearlyDifference DESC;
# """,
"""
SELECT 
    Town,
    YEAR(DateRecorded) AS SaleYear,
    SUM(SaleAmount) AS TotalSales,
    (SUM(SaleAmount) / LAG(SUM(SaleAmount)) OVER (PARTITION BY Town ORDER BY YEAR(DateRecorded)) - 1) * 100 AS YearOverYearGrowthPercent
FROM 
    real_estate_table
WHERE 
    SaleAmount IS NOT NULL
GROUP BY 
    Town, SaleYear
HAVING 
    TotalSales > 100000
ORDER BY 
    Town, YearOverYearGrowthPercent DESC;
"""
    # Add additional queries here as needed
]

# Connect to the MySQL database
db_connection = mysql.connector.connect(**db_config)
cursor = db_connection.cursor()

# Execute each query and measure the execution time
for i, query in enumerate(queries):
    start_time = time.time()
    cursor.execute(query)
    query_results = cursor.fetchall()
    elapsed_time = time.time() - start_time

    print(f"MySQL Query {i+1} executed in {elapsed_time} seconds.")
    # for row in query_results:
    #     print(row)

# Close the cursor and connection
cursor.close()
db_connection.close()