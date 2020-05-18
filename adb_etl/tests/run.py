from adb_etl.jobs.etl_job import *
from adb_etl.configs.etl_config import *

# trigger main function

config = Config()
config.SCOPE_NAME = '<databricks_scope_name>'
config.TENANT_ID = '<aad tenant_id>'
config.CLIENT_ID = '<aad_client_id>'
config.SECRET = '<aad_client_secret>'
config.STEPS_PER_FLOOR = 21
config.DATA_LAKE = '<data_lake_url>'

main(config=config)
