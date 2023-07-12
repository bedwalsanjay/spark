/*

Consider a scenario where you have a DataFrame containing stock market data for different stocks, and you want to calculate 
the 3-days moving average for each stock based on their closing prices. 
Additionally, you want to identify stocks that have experienced a significant increase or decrease in price compared to their 
moving average.Conisder a change of +5 as a significant increase and -5 as a significant decrease in price

========
INPUT
========

+------------+----------+--------------+
|stock_symbol|      date|closing_price|
+------------+----------+--------------+
|        AAPL|2022-01-01|        150.50|
|        AAPL|2022-01-02|        148.25|
|        AAPL|2022-01-03|        152.80|
|        AAPL|2022-01-04|        156.50|
|        AAPL|2022-01-05|        162.40|
|       GOOGL|2022-01-01|       1980.00|
|       GOOGL|2022-01-02|       1965.50|
|       GOOGL|2022-01-03|       1995.75|
|       GOOGL|2022-01-04|       2020.50|
|       GOOGL|2022-01-05|       1985.25|
|        MSFT|2022-01-01|        250.20|
|        MSFT|2022-01-02|        248.50|
|        MSFT|2022-01-03|        255.75|
|        MSFT|2022-01-04|        260.80|
|        MSFT|2022-01-05|        258.40|
|        NFLX|2022-01-01|        500.00|
|        NFLX|2022-01-02|        498.50|
|        NFLX|2022-01-03|        495.75|
|        NFLX|2022-01-04|        493.20|
|        NFLX|2022-01-05|        498.90|
+------------+----------+--------------+

========
OUTPUT
========

+------------+----------+------------------+------------------+--------------+--------------------+
|stock_symbol|      date|       moving_sum  |    moving_avg    |closing_price|      change        |
+------------+----------+------------------+------------------+--------------+--------------------+
|        AAPL|2022-01-01|          150.50  |      150.50      |        150.50|No Significant Change|
|        AAPL|2022-01-02|          298.75  |      149.37      |        148.25|No Significant Change|
|        AAPL|2022-01-03|          451.55  |      150.52      |        152.80|No Significant Change|
|        AAPL|2022-01-04|          457.55  |      152.52      |        156.50|No Significant Change|
|        AAPL|2022-01-05|          471.70  |      157.23      |        162.40|Significant Increase |
|       GOOGL|2022-01-01|          1980.00 |     1980.00      |       1980.00|No Significant Change|
|       GOOGL|2022-01-02|          3945.50 |     1972.75      |       1965.50|No Significant Change|
|       GOOGL|2022-01-03|          5936.25 |     1978.75      |       1995.75|No Significant Change|
|       GOOGL|2022-01-04|          5991.75 |     1997.25      |       2020.50|Significant Increase |
|       GOOGL|2022-01-05|          6006.75 |     2002.25      |       1985.25|Significant Decrease |
|        MSFT|2022-01-01|          250.20  |      250.20      |        250.20|No Significant Change|
|        MSFT|2022-01-02|          498.70  |      249.35      |        248.50|No Significant Change|
|        MSFT|2022-01-03|          754.45  |      251.48      |        255.75|No Significant Change|
|        MSFT|2022-01-04|          764.25  |      254.75      |        260.80|Significant Increase |
|        MSFT|2022-01-05|          764.65  |      254.88      |        258.40|No Significant Change|
|        NFLX|2022-01-01|          500.00  |      500.00      |        500.00|No Significant Change|
|        NFLX|2022-01-02|          998.50  |      499.25      |        498.50|No Significant Change|
|        NFLX|2022-01-03|          1494.25 |      498.08      |        495.75|No Significant Change|
|        NFLX|2022-01-04|          1492.45 |      497.48      |        493.20|No Significant Change|
|        NFLX|2022-01-05|          1487.85 |      495.95      |        498.90|No Significant Change|
+------------+----------+------------------+------------------+--------------+--------------------+

*/

WITH window_data AS (
  SELECT 
    stock_symbol, 
    date, 
    closing_price,
    SUM(closing_price) OVER (
      PARTITION BY stock_symbol 
      ORDER BY date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_sum,
    AVG(closing_price) OVER (
      PARTITION BY stock_symbol 
      ORDER BY date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg
  FROM your_table_name
)
SELECT 
  stock_symbol,
  date,
  moving_sum,
  moving_avg,
  closing_price,
  CASE 
    WHEN closing_price - moving_avg >= 5 THEN 'Significant Increase'
    WHEN closing_price - moving_avg <= -5 THEN 'Significant Decrease'
    ELSE 'No Significant Change'
  END AS change
FROM window_data
ORDER BY stock_symbol, date
