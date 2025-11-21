***Comprehensive Project Summary: Microsoft Fabric Medallion Architecture + Data Pipeline + Power BI Analytics***

A retail company wanted to create a unified, high-quality view of its operational data by combining information from its Orders, Inventory, and Returns systems. These datasets were arriving daily from multiple sources into Azure Data Lake Storage (ADLS). Due to inconsistent data types, missing values, and no centralized reporting structure, the company lacked visibility into product performance, profitability, and return trends.

To solve this, I designed and implemented a complete end-to-end Modern Data Engineering solution using Microsoft Fabric, Power BI, and the Medallion Architecture.

<img width="598" height="176" alt="final data pipeline" src="https://github.com/user-attachments/assets/fe44ac28-8b0f-42e7-80bc-53a0048ea763" />

***🔹 1. Data Ingestion Pipeline (Microsoft Fabric)***

I created a reliable and automated Fabric Data Pipeline using two core activities:

***1️.Get Metadata***

•	Retrieves the list of new files from the ADLS folder.

•	Validates if daily data exists.

•	Enables dynamic file handling without hardcoding file names.

•	Makes the pipeline flexible as new files arrive daily.

***2️.Copy Data***

•	Automatically copies all input files from ADLS into the Bronze layer of the Fabric Lakehouse.

•	Preserves raw schema for auditing and historical traceability.

•	Supports multiple formats such as CSV, JSON, and Parquet.

***Pipeline Flow:***

Get Metadata → Copy Data → Bronze Layer

This provides clean automation, eliminating manual data uploads and ensuring fresh data enters the system daily.

***🔹 2. Medallion Architecture Implementation***

***Bronze Layer – Raw Ingestion***

•	Stores data exactly as received from ADLS.

•	Maintains historic and unaltered records.

***Silver Layer – Cleaning & Standardization***

Using Fabric Notebooks (PySpark), I performed:

•	Data type corrections

•	Null value and missing field handling

•	Standardization of product names and numeric columns

•	Joining Orders, Inventory, and Returns into a unified dataset

The Silver layer ensures consistency, correctness, and reliability.

***Gold Layer – Business Aggregations***

I created a curated Gold table by aggregating data at the Product level, including:

•	Total Orders

•	Unique Customers

•	Total Returns & Return Rate (%)

•	Total Revenue & Avg Order Value

•	Stock Levels

•	Avg Cost

•	Net Profit (Revenue – Stock × Avg Cost)

This Gold layer acts as the single source of truth for reporting and analytics.

***🔹 3. Power BI Reporting & Modeling***

I connected Power BI directly to the Gold layer and built an interactive dashboard that visualizes key business insights:
 Key Visuals Created

•	Revenue vs Cost – Combo chart

•	Profitability Heatmap – Highlights products with losses (red) and profits (green)

•	Return Rate Analyses – Donut chart & bar chart

•	Stock vs Demand – Bar charts

•	KPI Cards – Revenue, Orders, Returns, Avg Cost

<img width="886" height="498" alt="page 1" src="https://github.com/user-attachments/assets/bea5a5f4-cead-4b1f-97b8-2717bfb518da" />

<img width="893" height="496" alt="page 2" src="https://github.com/user-attachments/assets/5a0f9a29-d2df-4bea-9a81-f34901cebe2b" />

This notebook handles ingestion from CSV, JSON, Excel, applies Silver-level cleaning rules, and produces a Gold aggregated fact table.
________________________________________
🥉 BRONZE LAYER — RAW INGESTION
### CSV → DataFrame (Orders)
df_orders = spark.read.csv(
    "abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/3f0d51ed-691b-49c1-983f-b4042e721365/Files/BRONZE/orders_data.csv"
)
________________________________________
### JSON → DataFrame (Inventory)
df_inventory = (
    spark.read
        .option("multiline", "true")
        .json(
            "abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/3f0d51ed-691b-49c1-983f-b4042e721365/Files/BRONZE/inventory_data.json"
        )
)
________________________________________
### Excel → DataFrame (Returns, converted to Parquet)
df_return = spark.read.parquet(
    "abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/3f0d51ed-691b-49c1-983f-b4042e721365/Files/BRONZE/returns_data.xlsx.parquet"
)
________________________________________
### Preview Raw Data
display(df_return.limit(5))

display(df_inventory.limit(5))

display(df_orders.limit(5))
________________________________________
### Handling First Row as Header
Extract headers:

orders_header = [str(c).strip() for c in df_orders.first()]

returns_header = [str(c).strip() for c in df_return.first()]

Apply headers:

Orders
df_orders = (
    df_orders.rdd.zipWithIndex()
    .filter(lambda x: x[1] > 0)
    .map(lambda x: x[0])
    .toDF(orders_header)
)

Returns
df_returns = (
    df_return.rdd.zipWithIndex()
    .filter(lambda x: x[1] > 0)
    .map(lambda x: x[0])
    .toDF(returns_header)
)
________________________________________
### Save Bronze Tables

df_orders.write.format("delta").mode("overwrite").saveAsTable("order_bronze")

df_return.write.format("delta").mode("overwrite").saveAsTable("return_bronze")

df_inventory.write.format("delta").mode("overwrite").saveAsTable("inventory_bronze")
________________________________________
🥈 SILVER LAYER — CLEANING & STANDARDIZATION
________________________________________
### SILVER ORDERS

orders = spark.table("order_bronze")

orders_df = (
    orders
    
    .withColumnRenamed("Order_ID", "OrderID")
    
    .withColumnRenamed("cust_id", "CustomerID")
    
    .withColumnRenamed("Product_Name", "ProductName")
    
    .withColumnRenamed("Qty", "Quantity")
    
    .withColumnRenamed("Order_Date", "OrderDate")
    
    .withColumnRenamed("Order_Amount$", "OrderAmount")
    
    .withColumnRenamed("Delivery_Status", "DeliveryStatus")
    
    .withColumnRenamed("Payment_Mode", "PaymentMode")
    
    .withColumnRenamed("Ship_Address", "ShipAddress")
    
    .withColumnRenamed("Promo_Code", "PromoCode")
    
    .withColumnRenamed("Feedback_Score", "FeedbackScore")
    
    # Quantity cleanup
    .withColumn(
        "Quantity",
        when(lower(col("Quantity")) == "one", 1)
        .when(lower(col("Quantity")) == "two", 2)
        .when(lower(col("Quantity")) == "three", 3)
        .otherwise(col("Quantity").cast("int"))
    )

    # Date parsing
    .withColumn(
        "OrderDate",
        coalesce(
            to_date(col("OrderDate"), "yyyy/MM/dd"),
            to_date(col("OrderDate"), "dd-MM-yyyy"),
            to_date(col("OrderDate"), "MM-dd-yyyy"),
            to_date(col("OrderDate"), "yyyy.MM.dd"),
            to_date(col("OrderDate"), "dd/MM/yyyy"),
            to_date(col("OrderDate"), "dd.MM.yyyy")
        )
    )

    # Order amount cleanup
    .withColumn(
        "OrderAmount",
        regexp_replace(col("OrderAmount"), r"(Rs\.|USD|INR|₹|,|\$)", "")
        .cast("double")
    )

    # Payment Mode
    .withColumn(
        "PaymentMode",
        regexp_replace(lower(col("PaymentMode")), r"[^A-Za-z]", "")
    )

    # Delivery Status
    .withColumn(
        "DeliveryStatus",
        regexp_replace(lower(col("DeliveryStatus")), r"[^A-Za-z]", "")
    )

    # Address cleanup
    .withColumn("ShipAddress", regexp_replace(col("ShipAddress"), r"[#@!$]", ""))

    # Email validation
    .withColumn(
        "Email",
        when(
            col("Email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}$"),
            col("Email")
        )
    )

    .withColumn("FeedbackScore", col("FeedbackScore").cast("double"))

    .fillna({
        "Quantity": 0,
        "OrderAmount": 0.0,
        "DeliveryStatus": "unknown",
        "PaymentMode": "unknown"
    })

    .dropna(subset=["CustomerID", "ProductName"])
    .dropDuplicates(["OrderID"])
)

orders_df.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
________________________________________

### SILVER INVENTORY

inventory = spark.table("inventory_bronze")

inventory_new_df = (
    inventory
    
    .withColumnRenamed("productName","ProductName")
    
    .withColumnRenamed("last_stocked","lastStocked")
    
    .withColumnRenamed("cost_price","CostPrice")

    # Stock cleanup
    .withColumn(
        "Stock",
        when(col("stock").rlike("^[0-9]+$"), col("stock").cast("int"))
        .otherwise(None)
    )

    # lastStocked cleanup
    .withColumn(
        "lastStocked",
        to_date(regexp_replace("lastStocked", r"[./]", "-"), "yyyy-MM-dd")
    )

    # cost price numeric extraction
    .withColumn(
        "CostPrice",
        regexp_extract(col("CostPrice"), r"(\d+\.?\d*)", 1).cast("double")
    )

    # Warehouse
    .withColumn(
        "Warehouse",
        initcap(trim(regexp_replace(col("warehouse"), r"[^a-zA-Z0-9 ]", "")))
    )

    # Available cleanup
    .withColumn(
        "Available",
        when(lower(col("available")).isin("yes", "y", "true"), True)
        .when(lower(col("available")).isin("no", "n", "false"), False)
    )
)

inventory_new_df.write.format("delta").mode("overwrite").saveAsTable("silver_inventory")
________________________________________

### SILVER RETURNS

return_df = spark.table("return_bronze")

returns_header = [str(c).strip() for c in return_df.first()]

returns_df_new = (
    return_df.rdd.zipWithIndex()
    .filter(lambda x: x[1] > 0)
    .map(lambda x: x[0])
    .toDF(returns_header)
)

returns_df_new = (
    returns_df_new
        .withColumnRenamed("Return_ID", "ReturnID")
        .withColumnRenamed("Order_ID", "OrderID")
        .withColumnRenamed("Customer_ID", "CustomerID")
        .withColumnRenamed("Return_Reason", "ReturnReason")
        .withColumnRenamed("Return_Date", "ReturnDate")
        .withColumnRenamed("Refund_Status", "RefundStatus")
        .withColumnRenamed("Pickup_Address", "PickupAddress")
        .withColumnRenamed("Return_Amount", "ReturnAmount")

        # Date cleanup
        .withColumn(
            "ReturnDate",
            to_date(regexp_replace(col("ReturnDate"), r"[./]", "-"), "dd-MM-yyyy")
        )

        # Refund status
        .withColumn(
            "RefundStatus",
            lower(regexp_replace(col("RefundStatus"), r"[^a-zA-Z]", ""))
        )

        # Return amount
        .withColumn(
            "ReturnAmount",
            regexp_extract(col("ReturnAmount"), r"(\d+\.?\d*)", 1).cast("double")
        )

        # Address & Product cleanup
        .withColumn("PickupAddress", initcap(trim(regexp_replace(col("PickupAddress"), r"[^a-zA-Z0-9\s]", ""))))
        .withColumn("Product", initcap(trim(regexp_replace(col("Product"), r"[^a-zA-Z0-9\s]", ""))))

        # Standardize CustomerID
        .withColumn("CustomerID", trim(upper(col("CustomerID"))))
)

returns_df_new.write.format("delta").mode("overwrite").saveAsTable("silver_return")
________________________________________
🥇 GOLD LAYER — BUSINESS AGGREGATIONS
________________________________________
Join all silver tables

order = spark.table("silver_orders").alias("o")

returns = spark.table("silver_return").alias("r")

inventory = spark.table("silver_inventory").alias("i")

df_pregold = 

(
    order
          .join(returns, col("o.OrderID") == col("r.OrderID"), "left")
    
         .join(inventory, col("o.ProductName") == col("i.ProductName"), "left")
         
         .select(
             col("o.ProductName"),
             col("o.OrderID"),
             col("o.CustomerID"),
             col("o.OrderAmount"),
             col("i.Stock"),
             col("i.CostPrice"),
             col("r.ReturnID")
         )
)
________________________________________
Gold aggregations
df_gold = (

    df_pregold.groupBy("ProductName")
    
    .agg(
    
        count("OrderID").alias("Total_Orders"),
        
        countDistinct("CustomerID").alias("Unique_Customers"),
        
        count("ReturnID").alias("Total_Returns"),
        
        round((count("ReturnID") / count("OrderID")) * 100, 2).alias("Return_Rate%"),
        
        round(sum("OrderAmount"), 2).alias("Total_Revenue"),
        
        round(avg("OrderAmount"), 2).alias("Avg_Order_Value"),
        
        sum("Stock").alias("Total_Stock"),
        
        round(avg("CostPrice"), 2).alias("Avg_Cost"),
        
        round(sum("OrderAmount") - (sum("Stock") * avg("CostPrice")), 2).alias("Net_Profit")
    )
)
________________________________________
Save Gold Table

df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_retail")
________________________________________








