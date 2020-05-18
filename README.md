# PySpark Example Project

This document is designed to be read in parallel with the code in the `pyspark-template-project` repository. Together, these constitute what we consider to be a 'best practices' approach to writing ETL jobs using Apache Spark and its Python ('PySpark') APIs using Databricks. This project addresses the following topics:

- how to structure ETL code in such a way that it can be easily tested and debugged;
- how to pass configuration parameters to a PySpark job;
- how to handle dependencies on other modules and packages; and,
- what constitutes a 'meaningful' test for an ETL job.

## ETL Project Structure

The basic project structure is as follows:

```bash
adb_etl/
 |-- configs/
 |   |-- etl_config.py
 |-- dependencies/
 |   |-- spark_utility.py
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- employees/
 |   |-- | -- employees_report/
 |   |-- run.py
 |   |-- test_activate_token.py
 |   |-- test_etl_job.py
 requirements.txt
 setup.py
```

The main Python module containing the ETL job , is `jobs/etl_job.py`. Any external configuration parameters required by `etl_job.py` are stored in Class file in `configs/etl_config.py`. Additional modules that support this job can be kept in the `dependencies` folder (more on this later). Unit test modules are kept in the `tests` folder and small chunks of representative input and output data, to be used with the tests, are kept in `tests/test_data` folder.

## Passing Configuration Parameters to the ETL Job

There are different ways to pass configuration variables in etl_job.py. JSON, YAML, ini and Class file.
In this example we have used `configs/etl_config.py` 

```python
class Config():
    COMMON_DQS_SCOPE = '<databricks scope name>'

    #Replace the Key name from Key Vault for azure active directory tenant id, client id and secret
    TENANT_ID = '<aad tenant_id>' 
    CLIENT_ID = '<aad_client_id>'
    SECRET = '<aad_client secret>'

    STEPS_PER_FLOOR = 21
    DATA_LAKE = '<data lake url>'
```