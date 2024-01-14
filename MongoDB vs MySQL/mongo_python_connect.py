from pymongo import MongoClient
import time


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['realestate']  # Replace with your database name
collection = db['project']

# Define your queries
queries = [
    # Query 1
    [
        "db.project.find().limit(1000);"

        "db.project.find().limit(10000);"

        "db.project.find().limit(100000);"

       "db.project.find().limit(500000);"

        db.project.find();
    ]
    [
        {"$match": {"List Year": {"$gt": 2018}, "Assessed Value": {"$gte": 100000, "$lte": 500000}}},
        {"$group": {"_id": "$Town", "AverageValue": {"$avg": "$Assessed Value"}, "TotalSales": {"$sum": "$Sale Amount"}}},
        {"$match": {"AverageValue": {"$gt": 150000}}},
        {"$sort": {"AverageValue": -1}}
    ],
    # Query 2
    [
        {"$match": {"Assessed Value": {"$gt": 0}}},
        {"$project": {"_id": 0, "Address": 1, "Town": 1, "Sale Amount": 1, "Assessed Value": 1, 
                      "ProfitLoss": {"$subtract": ["$Sale Amount", "$Assessed Value"]},
                      "ProfitLossPercentage": {"$concat": [{"$toString": {"$round": [{"$multiply": [{"$divide": [{"$subtract": ["$Sale Amount", "$Assessed Value"]}, "$Assessed Value"]}, 100]}, 2]}}, "%"]}}},
        {"$sort": {"ProfitLoss": -1}}
    ],
    # Query 3
    [
        {"$match": {"Date Recorded": {"$exists": True, "$ne": None}}},
        {"$addFields": {"SaleDate": {"$toDate": "$Date Recorded"}}},
        {"$group": {"_id": {"YearOfSale": {"$year": "$SaleDate"}, "Town": "$Town"}, 
                    "NumberOfSales": {"$sum": 1}, "AverageSale": {"$avg": "$Sale Amount"}}},
        {"$match": {"NumberOfSales": {"$gt": 10}, "AverageSale": {"$gt": 200000}}},
        {"$sort": {"_id.YearOfSale": -1, "AverageSale": -1}},
        {"$project": {"_id": 0, "YearOfSale": "$_id.YearOfSale", "Town": "$_id.Town", "NumberOfSales": 1, "AverageSale": 1}}
    ],
    # Query 4
    [
        # Note: This query involves a variable (avgSaleAmount) that is obtained from a separate aggregation.
        # It may need to be executed separately or adjusted for direct use in Python.
        {"$match": {"List Year": {"$gt": 2019}, "Sale Amount": {"$gt": "avgSaleAmount"}}},
        {"$group": {"_id": {"Town": "$Town", "ListYear": "$List Year"}, 
                    "AverageSale": {"$avg": "$Sale Amount"}, 
                    "MaxSaleInTownYear": {"$max": {"$cond": {"if": {"$eq": ["$Town", "$Town"]}, "then": "$Sale Amount", "else": None}}}}},
        {"$sort": {"AverageSale": -1}},
        {"$project": {"_id": 0, "Town": "$_id.Town", "ListYear": "$_id.ListYear", "AverageSale": 1, "MaxSaleInTownYear": 1}}
    ],
    [
        {"$match": {"Date Recorded": {"$exists": True, "$ne": None}, 
                    "$expr": {"$gt": [{"$year": {"$toDate": "$Date Recorded"}}, 2015]}}},
        {"$group": {"_id": {"SaleYear": {"$year": {"$toDate": "$Date Recorded"}}, "Town": "$Town"}, 
                    "TotalSales": {"$sum": 1}, "AverageSaleAmount": {"$avg": "$Sale Amount"}}},
        {"$match": {"TotalSales": {"$gt": 5}}},
        {"$sort": {"_id.Town": 1, "_id.SaleYear": 1}},
        {"$group": {"_id": "$_id.Town", "data": {"$push": {"SaleYear": "$_id.SaleYear", 
                                                           "TotalSales": "$TotalSales", 
                                                           "AverageSaleAmount": "$AverageSaleAmount"}}}},
        {"$project": {"_id": 0, "Town": "$_id", "YearlyData": "$data"}},
        {"$unwind": "$YearlyData"},
        {"$sort": {"Town": 1, "YearlyData.SaleYear": 1}},
        {"$group": {"_id": "$Town", "data": {"$push": "$YearlyData"}}},
        {"$project": {"_id": 0, "Town": "$_id", "YearlyData": 1, 
                      "YearOverYearGrowth": {"$map": {"input": "$YearlyData", "as": "data", 
                                                      "in": {"SaleYear": "$$data.SaleYear", 
                                                            "TotalSales": "$$data.TotalSales", 
                                                            "AverageSaleAmount": "$$data.AverageSaleAmount", 
                                                            "YearOverYearGrowth": {"$multiply": [{"$divide": [{"$subtract": ["$$data.AverageSaleAmount", {"$avg": "$$data.AverageSaleAmount"}]}, {"$avg": "$$data.AverageSaleAmount"}]}, 100]}}}}}}
    ],
    # Query 6
    [
        {"$group": {"_id": {"Town": "$Town", "Year": {"$year": {"$toDate": "$Date Recorded"}}, 
                           "Quarter": {"$subtract": [{"$month": {"$toDate": "$Date Recorded"}}, 
                                                     {"$mod": [{"$month": {"$toDate": "$Date Recorded"}}, 3]}]}}, 
                    "TotalQuarterlySales": {"$sum": "$Sale Amount"}, "NumberOfSales": {"$sum": 1}, "AverageSale": {"$avg": "$Sale Amount"}}},
        {"$match": {"TotalQuarterlySales": {"$gt": 5000000}}},
        {"$sort": {"_id.Year": -1, "_id.Quarter": 1}},
        {"$project": {"_id": 0, "Town": "$_id.Town", "Year": "$_id.Year", "Quarter": "$_id.Quarter", 
                      "TotalQuarterlySales": 1, "NumberOfSales": 1, "AverageSale": 1}}
    ],
    # Query 7
    [
        {"$group": {"_id": "$Town", 
                    "SingleFamilyHomes": {"$sum": {"$cond": [{"$eq": ["$ResidentialType", "Single Family"]}, 1, 0]}}, 
                    "MultiFamilyOrCommercial": {"$sum": {"$cond": [{"$or": [{"$eq": ["$ResidentialType", "Multi Family"]}, 
                                                                           {"$eq": ["$PropertyType", "Commercial"]}]} , 1, 0]}}, 
                    "AvgSaleSingleFamily": {"$avg": {"$cond": [{"$eq": ["$ResidentialType", "Single Family"]}, "$SaleAmount", None]}}, 
                    "AvgSaleMultiFamilyOrCommercial": {"$avg": {"$cond": [{"$or": [{"$eq": ["$ResidentialType", "Multi Family"]}, 
                                                                                 {"$eq": ["$PropertyType", "Commercial"]}]} , "$SaleAmount", None]}}}},
        {"$sort": {"_id": -1}}
    ],
    # Query 8
    [
        {"$match": {"Sale Amount": {"$ne": None}}},
        {"$group": {"_id": {"Town": "$Town", "SaleYear": {"$year": {"$toDate": "$Date Recorded"}}}, 
                    "TotalSales": {"$sum": "$Sale Amount"}, "Sales": {"$push": "$Sale Amount"}}},
        {"$project": {"_id": 0, "Town": "$_id.Town", "SaleYear": "$_id.SaleYear", "TotalSales": 1, "Sales": 1}},
        {"$unwind": {"path": "$Sales", "includeArrayIndex": "index"}},
        {"$sort": {"SaleYear": 1, "index": 1}},
        {"$group": {"_id": "$Town", "SaleYear": {"$first": "$SaleYear"}, "TotalSales": {"$first": "$TotalSales"}, 
                    "Sales": {"$push": "$Sales"}}},
        {"$project": {"_id": 0, "Town": "$_id", "SaleYear": 1, "TotalSales": 1, 
                      "YearOverYearGrowthPercent": {"$multiply": [{"$divide": [{"$subtract": ["$TotalSales", {"$arrayElemAt": ["$Sales", -2]}]}, {"$arrayElemAt": ["$Sales", -2]}]}, 100]}}},
        {"$match": {"TotalSales": {"$gt": 100000}}},
        {"$sort": {"Town": 1, "YearOverYearGrowthPercent": -1}}
    ],
    # Query 9
    [
        {"$group": {"_id": {"Town": "$Town", "PropertyType": "$PropertyType"}, 
                    "TotalProperties": {"$sum": 1}, 
                    "AverageSaleAmount": {"$avg": "$SaleAmount"}, 
                    "Sales_100k_to_200k": {"$sum": {"$cond": [{"$and": [{"$gte": ["$SaleAmount", 100000]}, {"$lte": ["$SaleAmount", 200000]}]}, 1, 0]}}, 
                    "Sales_200k_to_300k": {"$sum": {"$cond": [{"$and": [{"$gt": ["$SaleAmount", 200000]}, {"$lte": ["$SaleAmount", 300000]}]}, 1, 0]}}, 
                    "Sales_Above_300k": {"$sum": {"$cond": [{"$gt": ["$SaleAmount", 300000]}, 1, 0]}}}},
        {"$match": {"TotalProperties": {"$gt": 10}}},
        {"$sort": {"_id.Town": 1, "AverageSaleAmount": -1}}
    ]
]

# Execute and time each query
for i, query in enumerate(queries):
    start_time = time.time()
    result = list(collection.aggregate(query))
    result_cursor = collection.aggregate(query)
  # Convert the cursor to a list to force execution

    elapsed_time = time.time() - start_time
    print(f"MongoDB Query {i+1} executed in {elapsed_time} seconds.")

    # Print results of each query
    # print(f"Results for Query {i+1}:")
    # for doc in result_cursor:
    #     print(doc)
    # print("\n")