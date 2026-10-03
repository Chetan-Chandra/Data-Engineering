{{ config(
    materialized='view',
    schema='MARKETPLACE'
) }}

SELECT

    f.SALES_ORDER_NUMBER,
    f.SALES_ORDER_LINE_NUMBER,

    f.ORDER_DATE,

    d.YEAR,
    d.QUARTER,
    d.MONTH,
    d.MONTH_NAME,
    d.DAY_NAME,
    d.IS_WEEKEND,

    c.CUSTOMER_KEY,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.GENDER,
    c.YEARLY_INCOME,

    p.PRODUCT_KEY,
    p.ENGLISH_PRODUCT_NAME,
    p.COLOR,
    p.SIZE,

    f.ORDER_QUANTITY,
    f.UNIT_PRICE,
    f.EXTENDED_AMOUNT,
    f.DISCOUNT_AMOUNT,
    f.TOTAL_PRODUCT_COST,
    f.SALES_AMOUNT,
    f.TAX_AMT,
    f.FREIGHT

FROM {{ ref('fact_internet_sales') }} f

LEFT JOIN {{ ref('dim_customer') }} c
    ON f.CUSTOMER_KEY = c.CUSTOMER_KEY

LEFT JOIN {{ ref('dim_product') }} p
    ON f.PRODUCT_KEY = p.PRODUCT_KEY

LEFT JOIN {{ ref('dim_date') }} d
    ON f.ORDER_DATE_KEY = d.DATE_KEY