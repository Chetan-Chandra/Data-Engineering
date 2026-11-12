dbt CI/CD Pipeline — Stage-wise Execution Sequence
1. DBT CI Stage (Pull Request)
Purpose: Validate dbt code and Snowflake connectivity before merging PR. No deployment.
Order	Step	Important command / action
1	Checkout source code	checkout: self
2	Set Python version	UsePythonVersion@0 (Python 3.10)
3	Create virtual environment	python3.10 -m venv dbt-env
4	Download Snowflake private key	DownloadSecureFile@1 (dev_dbt.p8)
5	Set environment variables	Configure Snowflake credentials and private key path
6	Install dbt	pip install dbt-core dbt-snowflake
7	Install dependencies	pip install -r requirements.txt
8	Install dbt packages	dbt deps
9	Test Snowflake connection	dbt debug --target test
10	Identify selected models	Extract dbt_select / dbt_selector from PR title
11	Compile selected models	dbt compile --target test --select/--selector ...
12	List selected models	dbt ls --target test --select/--selector ...

No dbt run or deployment in CI

2. Test DBT Deploy Stage (Main branch)
Purpose: Deploy and test dbt models in TEST Snowflake environment.
Order	Step	Important command / action
1	Checkout source code	checkout: self
2	Setup Python and virtual environment	UsePythonVersion@0, python3.10 -m venv dbt-env
3	Download Snowflake private key	DownloadSecureFile@1 (dev_dbt.p8)
4	Set environment variables	Configure TEST Snowflake credentials
5	Install dbt and Python dependencies	pip install dbt-core dbt-snowflake, pip install -r requirements.txt
6	Install dbt packages	dbt deps
7	Test Snowflake connection	dbt debug
8	Identify selected models	Extract selection from build commit message
9	Load seed data	dbt seed --target test ...
10	List selected models	dbt ls --target test ...
11	Execute dbt models	dbt run --target test ...
12	Deploy custom operations	dbt run-operation --target test run_all_custom_operations
13	Test dbt models	dbt test --target test ...
14	Download GCP service account key	DownloadSecureFile@1
15	Authenticate with GCP	gcloud auth activate-service-account --key-file=...
16	Generate dbt documentation	dbt docs generate --target test
17	Build Docker image	docker build -t ... -f DockerFile .
18	Push Docker image	docker push ...:latest and docker push ...:$(Build.BuildId)
19	Deploy documentation application	gcloud run deploy dbt ...


3. PreProd DBT Deploy Stage
Purpose: Deploy dbt models into PREPROD Snowflake environment after TEST deployment.
Order	Step	Important command / action
1	Checkout source code	checkout: self
2	Setup Python and virtual environment	UsePythonVersion@0, python3.10 -m venv dbt-env
3	Download Snowflake private key	DownloadSecureFile@1 (preprod_dbt.p8)
4	Set environment variables	Configure PREPROD Snowflake credentials
5	Install dbt and dependencies	pip install dbt-core dbt-snowflake, pip install -r requirements.txt
6	Install dbt packages	dbt deps
7	Test Snowflake connection	dbt debug --target preprod
8	Identify selected models	Extract selection from build commit message
9	Load seed data	dbt seed --target preprod ...
10	List selected models	dbt ls --target preprod ...
11	Execute dbt models	dbt run --target preprod ...

Note: dbt test and dbt run-operation are not executed in this stage.

4. PROD DBT Deploy Stage
Purpose: Deploy dbt models into PROD Snowflake environment.
Order	Step	Important command / action
1	Checkout source code	checkout: self
2	Setup Python and virtual environment	UsePythonVersion@0, python3.10 -m venv dbt-env
3	Download Snowflake private key	DownloadSecureFile@1 (prod_dbt.p8)
4	Set environment variables	Configure PROD Snowflake credentials
5	Install dbt and dependencies	pip install dbt-core dbt-snowflake, pip install -r requirements.txt
6	Install dbt packages	dbt deps
7	Test Snowflake connection	dbt debug --target prod
8	Identify selected models	Extract selection from build commit message
9	Load seed data	dbt seed --target prod ...
10	List selected models	dbt ls --target prod ...
11	Execute dbt models	dbt run --target prod ...
12	Deploy custom operations	dbt run-operation --target prod run_all_custom_operations
13	Test dbt models	dbt test --target prod ...
14	Download GCP service account key	DownloadSecureFile@1
15	Authenticate with GCP	gcloud auth activate-service-account --key-file=...
16	Generate dbt documentation	dbt docs generate --target prod
17	Build Docker image	docker build -t ... -f DockerFile .
18	Push Docker image	docker push ...:latest and docker push ...:$(Build.BuildId)
19	Deploy documentation application	gcloud run deploy dbt ...

Overall dbt CI/CD command flow

CI (Pull Request)
    |
    |-- dbt deps
    |-- dbt debug
    |-- dbt compile
    |-- dbt ls
    |
    v
TEST (Main branch)
    |
    |-- dbt deps
    |-- dbt debug
    |-- dbt seed
    |-- dbt ls
    |-- dbt run
    |-- dbt run-operation
    |-- dbt test
    |-- dbt docs generate
    |-- Docker build & push
    |-- Cloud Run deployment
    |
    v
PREPROD
    |
    |-- dbt deps
    |-- dbt debug
    |-- dbt seed
    |-- dbt ls
    |-- dbt run
    |
    v
PROD
    |
    |-- dbt deps
    |-- dbt debug
    |-- dbt seed
    |-- dbt ls
    |-- dbt run
    |-- dbt run-operation
    |-- dbt test
    |-- dbt docs generate
    |-- Docker build & push
    |-- Cloud Run deployment

CI validates dbt code using compile and connection checks. CD progressively deploys dbt models across TEST → PREPROD → PROD using environment-specific Snowflake credentials, private keys and dbt targets.