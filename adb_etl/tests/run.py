from adb_etl.jobs.etl_job import *
from adb_etl.configs.etl_config import *

# trigger main function

config = Config()
config.COMMON_DQS_SCOPE = ''
config.TENANT_ID = ''
config.CLIENT_ID = ''
config.SECRET = ''
config.STEPS_PER_FLOOR = 21
config.DATA_LAKE = ''

main(config=config)
