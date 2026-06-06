-- Purpose: daily DBU consumption with 7-day moving average, broken down by product
-- Tables:  system.billing.usage
-- Output:  date, product, daily_dbu, 7d_moving_avg

WITH daily AS (
    SELECT
        usage_date,
        billing_origin_product                          AS product,
        SUM(usage_quantity)                             AS daily_dbu
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 90 DAYS
      AND usage_unit = 'DBU'
    GROUP BY usage_date, billing_origin_product
)
SELECT
    usage_date,
    product,
    ROUND(daily_dbu, 2)                                 AS daily_dbu,
    ROUND(
        AVG(daily_dbu) OVER (
            PARTITION BY product
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    )                                                   AS moving_avg_7d,
    ROUND(
        SUM(daily_dbu) OVER (
            PARTITION BY product
            ORDER BY usage_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        2
    )                                                   AS cumulative_dbu_ytd
FROM daily
ORDER BY usage_date DESC, daily_dbu DESC;
