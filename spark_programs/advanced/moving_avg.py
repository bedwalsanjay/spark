'''
Consider a scenario where you have a DataFrame containing stock market data for different stocks, and you want to calculate 
the 30-day moving average for each stock based on their closing prices. 
Additionally, you want to identify stocks that have experienced a significant increase or decrease in price compared to their 
moving average.
'''

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum, col, when

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    ('AAPL', '2022-01-01', 150.50),
    ('AAPL', '2022-01-02', 148.25),
    ('AAPL', '2022-01-03', 152.80),
    ('AAPL', '2022-01-04', 156.50),
    ('AAPL', '2022-01-05', 162.40),
    ('GOOGL', '2022-01-01', 1980.00),
    ('GOOGL', '2022-01-02', 1965.50),
    ('GOOGL', '2022-01-03', 1995.75),
    ('GOOGL', '2022-01-04', 2020.50),
    ('GOOGL', '2022-01-05', 1985.25),
    ('MSFT', '2022-01-01', 250.20),
    ('MSFT', '2022-01-02', 248.50),
    ('MSFT', '2022-01-03', 255.75),
    ('MSFT', '2022-01-04', 260.80),
    ('MSFT', '2022-01-05', 258.40),
    ('NFLX', '2022-01-01', 500.00),
    ('NFLX', '2022-01-02', 498.50),
    ('NFLX', '2022-01-03', 495.75),
    ('NFLX', '2022-01-04', 493.20),
    ('NFLX', '2022-01-05', 498.90)
]

# Create DataFrame
df = spark.createDataFrame(data, ['stock_symbol', 'date', 'closing_price'])

# Define a window specification for the moving average and moving sum calculation
windowSpec = Window.partitionBy('stock_symbol').orderBy('date').rowsBetween(-2, 0)

# Calculate the 3-day moving average and moving sum for each stock
df_with_moving_avg_sum = df.withColumn('moving_sum', sum('closing_price').over(windowSpec)) \
    .withColumn('moving_avg', avg('closing_price').over(windowSpec))

# Identify stocks with significant price increase or decrease compared to the moving average
df_with_change = df_with_moving_avg_sum.withColumn('change',
    when(col('closing_price') - col('moving_avg') >= 5, 'Significant Increase')
    .when(col('closing_price') - col('moving_avg') <= -5, 'Significant Decrease')
    .otherwise('No Significant Change')
)

# Reorder the columns
df_with_change = df_with_change.select('stock_symbol', 'date', 'moving_sum', 'moving_avg', 'closing_price', 'change')

# Show the DataFrame
df_with_change.show(100, truncate=False)

#output
'''
+------------+----------+------------------+------------------+-------------+---------------------+
|stock_symbol|date      |moving_sum        |moving_avg        |closing_price|change               |
+------------+----------+------------------+------------------+-------------+---------------------+
|AAPL        |2022-01-01|150.5             |150.5             |150.5        |No Significant Change|
|AAPL        |2022-01-02|298.75            |149.375           |148.25       |No Significant Change|
|AAPL        |2022-01-03|451.55            |150.51666666666668|152.8        |No Significant Change|
|AAPL        |2022-01-04|457.55            |152.51666666666668|156.5        |No Significant Change|
|AAPL        |2022-01-05|471.70000000000005|157.23333333333335|162.4        |Significant Increase |
|GOOGL       |2022-01-01|1980.0            |1980.0            |1980.0       |No Significant Change|
|GOOGL       |2022-01-02|3945.5            |1972.75           |1965.5       |Significant Decrease |
|GOOGL       |2022-01-03|5941.25           |1980.4166666666667|1995.75      |Significant Increase |
|GOOGL       |2022-01-04|5981.75           |1993.9166666666667|2020.5       |Significant Increase |
|GOOGL       |2022-01-05|6001.5            |2000.5            |1985.25      |Significant Decrease |
|MSFT        |2022-01-01|250.2             |250.2             |250.2        |No Significant Change|
|MSFT        |2022-01-02|498.7             |249.35            |248.5        |No Significant Change|
|MSFT        |2022-01-03|754.45            |251.48333333333335|255.75       |No Significant Change|
|MSFT        |2022-01-04|765.05            |255.01666666666665|260.8        |Significant Increase |
|MSFT        |2022-01-05|774.9499999999999 |258.31666666666666|258.4        |No Significant Change|
|NFLX        |2022-01-01|500.0             |500.0             |500.0        |No Significant Change|
|NFLX        |2022-01-02|998.5             |499.25            |498.5        |No Significant Change|
|NFLX        |2022-01-03|1494.25           |498.0833333333333 |495.75       |No Significant Change|
|NFLX        |2022-01-04|1487.45           |495.81666666666666|493.2        |No Significant Change|
|NFLX        |2022-01-05|1487.85           |495.95            |498.9        |No Significant Change|
+------------+----------+------------------+------------------+-------------+---------------------+

'''