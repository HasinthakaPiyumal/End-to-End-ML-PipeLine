from MLOps.constants import *
from MLOps.utils.common import read_yaml, create_directories
from MLOps.entity.config_entity import DataIngestionConfig

class ConfigurationManager:
    def __init__(self, config_file_path=CONFIG_FILE_PATH, params_file_path=PARAMS_FILE_PATH, schema_file_path=SCHEMA_FILE_PATH):
        
        self.config = read_yaml(config_file_path)
        self.params = read_yaml(params_file_path)
        self.schema = read_yaml(schema_file_path)
        create_directories(list_of_directories=[self.config.artifacts_root])
    
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        data_ingestion_config = self.config.data_ingestion
        create_directories([data_ingestion_config.root_dir])
        return DataIngestionConfig(
            root_dir=data_ingestion_config.root_dir,
            source_URL=data_ingestion_config.source_URL,
            local_data_file=data_ingestion_config.local_data_file,
            unzip_dir=data_ingestion_config.unzip_dir
        )