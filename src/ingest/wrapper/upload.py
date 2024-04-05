

#from service import main
#from utility import util
from ingest.service import main
from ingest.utility import util
import argparse
import os
import yaml


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--param', help='Section name from config.txt', required=True)
    parser.add_argument('-d', '--param1', help='Config file from config.txt', required=True)
    parser.add_argument('-e', '--param2', help='enviroment', required=True)
    parser.add_argument('-f', '--param3', help='xenon_user', required=True)
    args = parser.parse_args()
    section_name = args.param
    config_file = args.param1
    env = args.param2
    xenon_user = args.param3
    file_name = os.path.join(os.path.abspath(os.path.dirname(__file__)), config_file)
    with open(file_name, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
        if env == 'prod':
            xenon_url = os.environ['xenon_url']
            #xenon_user = os.environ['xenon_user']
            #xenon_password = os.environ['xenon_password']
            hvac_password = os.environ['hvac_password']
            retrieved_secrets = util.hvac_pwv_read('svmdfpwv', hvac_password, 'https://prod.vault.target.com','secret/mdf_file_ingester/nuid', list(xenon_user.split(" ")))
            xenon_password = retrieved_secrets[xenon_user]
        else:
            #running local
            xenon_url = cfg[section_name]['xenon_url']
            xenon_user = cfg[section_name]['xenon_user']
            xenon_password = cfg[section_name]['xenon_password']
        put_params = cfg[section_name]['put_params']
        hdfs_dest_datefile_path = cfg[section_name]['hdfs_dest_datefile_path']
        hdfs_dest_ctrlfile_path = cfg[section_name]['hdfs_dest_ctrlfile_path']
        xenon_method = cfg[section_name]['xenon_method']
        partition_date_method = cfg[section_name]['partition_date_method']
        edge_node = cfg[section_name]['edge_node']
        platform = cfg[section_name]['platform']

        # Vmaas related params

        local_file_path = cfg[section_name]['local_file_path']
        local_datafile_regex_pattern = cfg[section_name]['local_datafile_regex_pattern']
        local_ctrlfile_regex_pattern = cfg[section_name]['local_ctrlfile_regex_pattern']

        # remote server related params
        sftp_method = cfg[section_name]['sftp_method']

        # batch exec related params
        btch_exec_stat_ingestion_job_n = cfg[section_name]['btch_exec_stat_ingestion_job_n']
        btch_exec_stat_ingestion_filepath = cfg[section_name]['btch_exec_stat_ingestion_filepath']
        btch_exec_stat_ingestion_hdfs_dest_file_path = cfg[section_name]['btch_exec_stat_ingestion_hdfs_dest_file_path']
        btch_exec_stat_ingestion_put_params = cfg[section_name]['btch_exec_stat_ingestion_put_params']
        btch_exec_stat_ingestion_partition_date_method = cfg[section_name]['btch_exec_stat_ingestion_partition_date_method']

        # log path related params
        log_filename = cfg[section_name]['log_filename']
        log_filepath = cfg[section_name]['log_filepath']

    main.upload_main(xenon_url, xenon_user, xenon_password, put_params, hdfs_dest_datefile_path, hdfs_dest_ctrlfile_path, local_file_path, local_datafile_regex_pattern, \
                 local_ctrlfile_regex_pattern, \
                 btch_exec_stat_ingestion_job_n, btch_exec_stat_ingestion_filepath, btch_exec_stat_ingestion_hdfs_dest_file_path, btch_exec_stat_ingestion_put_params, log_filename, log_filepath, sftp_method, xenon_method, partition_date_method, btch_exec_stat_ingestion_partition_date_method,
                 edge_node
                 )


