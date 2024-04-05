#pick all files from remote and move to dest ,no local cntrl pattern/remote data or control file pattern specified
#make sure no files in local file path


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
    #parser.add_argument('-f', '--param3', help='xenon_user', required=True)
    args = parser.parse_args()
    section_name = args.param
    config_file = args.param1
    env = args.param2
    #xenon_user = args.param3
    file_name = os.path.join(os.path.abspath(os.path.dirname(__file__)), config_file)
    with open(file_name, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
        remote_username = cfg[section_name]['remote_username']
        remote_passwordauth = cfg[section_name]['remote_passwordauth']
        if env == 'prod':
            #xenon_url = os.environ['xenon_url']
            #xenon_user = os.environ['xenon_user']
            #xenon_password = os.environ['xenon_password']
            xenon_url = cfg[section_name]['xenon_url']
            xenon_user = cfg[section_name]['xenon_user']
            local_private_key = os.environ['local_private_key']
            hvac_password = os.environ['hvac_password']
            retrieved_secrets = util.hvac_pwv_read('svmdfpwv', hvac_password, 'https://prod.vault.target.com','secret/mdf_file_ingester/nuid', list(xenon_user.split(" ")))
            xenon_password = retrieved_secrets[xenon_user]
            if remote_passwordauth != '':
                remote_user = cfg[section_name]['remote_user']
                remoteuser_retrieved_secrets = util.hvac_pwv_read('svmdfpwv', hvac_password, 'https://prod.vault.target.com',
                                                       'secret/mdf_file_ingester/nuid', list(remote_user.split(" ")))
                remoteuser_password = remoteuser_retrieved_secrets[remote_user]
            else:
                remoteuser_password=''

        else:
        #running local
            xenon_url = cfg[section_name]['xenon_url']
            xenon_user = cfg[section_name]['xenon_user']
            xenon_password = cfg[section_name]['xenon_password']
            local_private_key = cfg[section_name]['local_private_key']
            remoteuser_password = cfg[section_name]['remoteuser_password']
        put_params = cfg[section_name]['put_params']
        partition_date_method = cfg[section_name]['partition_date_method']
        hdfs_dest_datefile_path = cfg[section_name]['hdfs_dest_datefile_path']
        hdfs_dest_ctrlfile_path = cfg[section_name]['hdfs_dest_ctrlfile_path']
        xenon_method = cfg[section_name]['xenon_method']
        edge_node = cfg[section_name]['edge_node']
        platform = cfg[section_name]['platform']
        hdfs_sftp_filename = cfg[section_name]['hdfs_sftp_filename']
        hdfs_sftp_filepath = cfg[section_name]['hdfs_sftp_filepath']

        # Vmaas related params

        local_file_path = cfg[section_name]['local_file_path']
        local_datafile_regex_pattern = cfg[section_name]['local_datafile_regex_pattern']
        local_ctrlfile_regex_pattern = cfg[section_name]['local_ctrlfile_regex_pattern']
        local_hdfs_sftp_file_path = cfg[section_name]['local_hdfs_sftp_file_path']
        local_hdfs_sftp_temp_file_path = cfg[section_name]['local_hdfs_sftp_temp_file_path']

        # remote server related params

        remote_host = cfg[section_name]['remote_host']
        remote_file_path = cfg[section_name]['remote_file_path']
        remote_file_regex_pattern = cfg[section_name]['remote_file_regex_pattern']
        remote_ctrlfile_regex_pattern = cfg[section_name]['remote_ctrlfile_regex_pattern']
        sftp_method = cfg[section_name]['sftp_method']
        method = cfg[section_name]['method']
        sub_directory = cfg[section_name]['sub_directory']


        # batch exec related params

        btch_exec_stat_ingestion_job_n = cfg[section_name]['btch_exec_stat_ingestion_job_n']
        btch_exec_stat_ingestion_filepath = cfg[section_name]['btch_exec_stat_ingestion_filepath']
        #derive the partition date in the script
        btch_exec_stat_ingestion_hdfs_dest_file_path = cfg[section_name]['btch_exec_stat_ingestion_hdfs_dest_file_path']
        btch_exec_stat_ingestion_put_params = cfg[section_name]['btch_exec_stat_ingestion_put_params']
        btch_exec_stat_ingestion_partition_date_method = cfg[section_name]['btch_exec_stat_ingestion_partition_date_method']\


        # log path related params
        log_filename = cfg[section_name]['log_filename']
        log_filepath = cfg[section_name]['log_filepath']

    main.sftp_upload_main(xenon_url, xenon_user, xenon_password, put_params, hdfs_dest_datefile_path, hdfs_dest_ctrlfile_path, local_file_path, local_datafile_regex_pattern, \
                          local_ctrlfile_regex_pattern, remote_host, remote_username, local_private_key, remote_file_path, remote_file_regex_pattern, remote_ctrlfile_regex_pattern, \
                          btch_exec_stat_ingestion_job_n, btch_exec_stat_ingestion_filepath, btch_exec_stat_ingestion_hdfs_dest_file_path, btch_exec_stat_ingestion_put_params, log_filename, log_filepath, sftp_method, xenon_method, method, partition_date_method, btch_exec_stat_ingestion_partition_date_method,edge_node,
                          remote_passwordauth,remoteuser_password,platform,hdfs_sftp_filepath, hdfs_sftp_filename,local_hdfs_sftp_file_path,local_hdfs_sftp_temp_file_path,sub_directory
                          )
