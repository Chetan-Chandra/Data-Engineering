_______________________________________________________________________________________________________________________________________________________________________________
Question:
How would you design a dbt dimensional model in Snowflake for sales analytics, including fact tables, dimensions, surrogate keys, and incremental loading?

Answer:
Direct answer: For sales analytics, I’d build a transaction-level fact table with measures such as quantity, gross sales, discount, and net sales, linked to customer, product, date, and sales-channel dimensions. I’d use a star schema so analysts can consistently slice sales by those business attributes.

Reasoning: I’d assign surrogate keys to dimensions and retain source business keys for matching and traceability. For changing attributes, I’d use the appropriate history strategy, such as a Type 2 customer dimension, and make the fact grain explicit—for example, one row per sales order line.

Example: A product sale on an order line would carry the product, customer, date, and channel keys alongside its quantity and net sales. That lets the business reconcile net sales to the source and compare results across products, periods, and channels.

Fit for role: In dbt, I’d incrementally load new or changed records using a reliable change timestamp and the order-line business key, while handling dimension updates before the fact load. I’d add tests for key uniqueness, relationships, and required values, then document lineage and validate totals against the source.
_______________________________________________________________________________________________________________________________________________________________________________
Question:
A Snowflake query powering a dbt model has become slow and expensive. What steps would you take to diagnose and optimize its performance?

Answer:
Direct answer: I’d treat a sudden slowdown as an operational issue: first confirm the affected dbt model and downstream impact, then compare recent runs in Snowflake query history and inspect the query profile. I’d look for where time and credits are going, such as scans, spills, joins, or warehouse queuing.

Reasoning: I’d check whether filters are applied early, joins use the intended keys, and the model reads only needed rows and columns. Then I’d review pruning and clustering, and consider warehouse sizing only after addressing query inefficiencies.

Example: For example, if a dbt model became slow after a source grew, I’d use the profile to find an oversized scan or join, refine the model, and rerun it. I’d compare runtime and credits with the baseline to confirm the change helped without affecting results.

Fit for role: I’d validate the dbt model’s tests and downstream outputs, then document the change and monitor subsequent runs. That balances Snowflake performance and cost with reliable, traceable transformations.
_______________________________________________________________________________________________________________________________________________________________________________

Question:
How would you implement data quality, testing, and documentation in dbt for a critical customer dimension?

Answer:
Direct answer: I’d treat the customer dimension as a trusted, governed source for reporting and downstream customer analytics. In dbt, I’d combine schema tests, business-rule tests, freshness checks, and clear documentation.

Reasoning: For a customer dimension, duplicate or missing keys can inflate counts, while invalid attributes can undermine segmentation and reporting. I’d test the customer key for uniqueness and non-null values, validate required fields and accepted values, and check relationships to relevant source models.

Example: For example, in a customer dimension built from CRM and billing data, I’d define how records are matched and which source wins when attributes conflict. I’d add a singular test for that rule, then verify the outcome by checking that every customer key is unique and required fields pass before the model is released.

Fit for role: I’d document the model, columns, business definitions, and lineage in dbt YAML, and use dbt tests in CI so failures block promotion. I’d monitor scheduled runs in Snowflake and investigate failures with the engineering and business teams.