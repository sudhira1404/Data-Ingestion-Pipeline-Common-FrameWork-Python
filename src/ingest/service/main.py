
#from service.sftp_xenon import Sftp_Xenon
#from utility.btch_exec_stat_ingestion import Btch_Exec_Stat_Ingestion
#from utility import util

from ingest.service.sftp_xenon import Sftp_Xenon
from ingest.utility.btch_exec_stat_ingestion import Btch_Exec_Stat_Ingestion
from ingest.utility import util

from datetime import datetime
import logging
import os


def sftp_upload_main(xenon_url, xenon_user, xenon_password, put_params,hdfs_dest_datefile_path,hdfs_dest_ctrlfile_path,
         local_file_path, local_datafile_regex_pattern,local_ctrlfile_regex_pattern, remote_host, remote_username, local_private_key,
         remote_file_path,remote_file_regex_pattern, remote_ctrlfile_regex_pattern,btch_exec_stat_ingestion_job_n, btch_exec_stat_ingestion_filepath,
         btch_exec_stat_ingestion_hdfs_dest_file_path, btch_exec_stat_ingestion_put_params,log_filename,log_filepath,sftp_method='Y',
         xenon_method='Y',method='',partition_date_method='',btch_exec_stat_ingestion_partition_date_method='',edge_node='',
         remote_passwordauth='',remoteuser_password='',platform='',hdfs_sftp_filepath='', hdfs_sftp_filename='',local_hdfs_sftp_file_path='',
         local_hdfs_sftp_temp_file_path='',sub_directory=''):

    #util.is_dir_exists(log_filepath)

    if not os.path.exists(local_file_path):
        os.makedirs(local_file_path)

    if hdfs_sftp_filename.strip() != '':
        if not os.path.exists(local_hdfs_sftp_file_path):
            os.makedirs(local_hdfs_sftp_file_path)
        if not os.path.exists(local_hdfs_sftp_temp_file_path):
            os.makedirs(local_hdfs_sftp_temp_file_path)

    if not os.path.exists(log_filepath):
        os.makedirs(log_filepath)

    if not os.path.exists(btch_exec_stat_ingestion_filepath):
        os.makedirs(btch_exec_stat_ingestion_filepath)

    now = datetime.now()
    job_start = now.strftime('%Y-%m-%dT%H_%M_%S')
    filename = log_filename + job_start + ".log"
    logfilename = log_filepath + "/" + filename
    util.logs(logfilename)
    sftp = Sftp_Xenon(xenon_url,xenon_user,xenon_password,xenon_method,edge_node,remote_host,remote_username,local_private_key,sftp_method,method,remote_passwordauth,remoteuser_password,platform)
    restart = 'Y'
    restart_status = 'N'
    restart_status_btch = 'N'
    restart_status_hdfs_sftp = 'N'
    logging.info( "#########################################################  Executing upload_file to move any leftover files in local file path to hdfs  ###########################################################")
    json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon, restart_status = \
        sftp.upload_file(local_file_path, hdfs_dest_datefile_path, restart, put_params,
                     hdfs_dest_ctrlfile_path,local_datafile_regex_pattern, local_ctrlfile_regex_pattern,partition_date_method)
    if hdfs_sftp_filename.strip() != '':
        json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon, restart_status_hdfs_sftp = \
            sftp.upload_file(local_hdfs_sftp_file_path, hdfs_sftp_filepath, restart, params={'overwrite': 'true'})

    if restart_status == 'Y' or restart_status_hdfs_sftp == 'Y':
        restart_status == 'Y'
    job_end = now.strftime('%Y-%m-%dT%H_%M_%S')
    if restart_status == 'Y':
        logging.info("#########################################################  Batch execution metrics for upload_file restart process if any  ###########################################################")
        logging.info("Valid files moved to hdfs by restart = " + str(json_string_list_valid_xenon))
        logging.info("Rejected files not moved to hdfs by restart = " + str(json_string_list_rejected_xenon))
        logging.info("Error message for rejected files not moved to hdfs by restart = " + str(errormsg_xenon))
        logging.info("restart status = " + restart_status)
        btch_exec_stat_ingestion_process_name = 'xenon_put_restart'
        btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
            job_start)
        btch=Btch_Exec_Stat_Ingestion(xenon_url,xenon_user,xenon_password,edge_node,platform)
        json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon=btch.btch_exec_stat_ingestion(btch_exec_stat_ingestion_job_n,
                                                          btch_exec_stat_ingestion_process_name, job_start,
                                                          job_end, sftp.json_string_list_valid_xenon,
                                                          sftp.json_string_list_rejected_xenon, sftp.errormsg_xenon,
                                                          btch_exec_stat_ingestion_source_file,
                                                          btch_exec_stat_ingestion_filepath,
                                                          btch_exec_stat_ingestion_hdfs_dest_file_path,
                                                          btch_exec_stat_ingestion_put_params,btch_exec_stat_ingestion_partition_date_method)
        logging.info("Valid btch file for xenon moved to hdfs by restart= " + str(json_string_valid_xenon))
        logging.info("Rejected btch file for xenon not moved to hdfs by restart = " + str(json_string_rejected_xenon))
        logging.info("Error message for btch file for xenon not moved to hdfs by restart = " + str(errormsg_xenon))

    logging.info(
        "#############################  Starting to upload any Batch execution files left over in btch_exec_stat_ingestion_filepath in prevruns if any #############################")
    btch_exec_stat_ingestion_hdfs_dest_ctrlfile_path = ''
    btch_exec_stat_ingestion_ctrlfile_regex_pattern = ''
    btch_exec_stat_ingestion_datafile_regex_pattern = 'btch'
    restart = 'Y'
    json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon, restart_status_btch = \
        sftp.upload_file(btch_exec_stat_ingestion_filepath, btch_exec_stat_ingestion_hdfs_dest_file_path, restart,
                         btch_exec_stat_ingestion_put_params,
                         btch_exec_stat_ingestion_hdfs_dest_ctrlfile_path,
                         btch_exec_stat_ingestion_datafile_regex_pattern,
                         btch_exec_stat_ingestion_ctrlfile_regex_pattern,
                         btch_exec_stat_ingestion_partition_date_method)
    job_end = now.strftime('%Y-%m-%dT%H_%M_%S')
    if restart_status_btch == 'Y':
        logging.info(
            "#########################################################  Batch execution metrics for batch execution files left over in btch_exec_stat_ingestion_filepath in prevruns ###########################################################")
        logging.info(
            "Valid btch file for sftp/xenon moved to hdfs by restart = " + str(json_string_list_valid_xenon))
        logging.info("Rejected btch file for sftp/xenon  not moved to hdfs by restart = " + str(
            json_string_list_rejected_xenon))
        logging.info(
            "Error message for btch file for sftp/xenon not moved to hdfs by restart = " + str(errormsg_xenon))
        logging.info("restart status = " + restart_status_btch)
        btch_exec_stat_ingestion_process_name = 'btch_restart'
        btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
            job_start)
        btch = Btch_Exec_Stat_Ingestion(xenon_url, xenon_user, xenon_password, edge_node,platform)
        json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon = btch.btch_exec_stat_ingestion(
            btch_exec_stat_ingestion_job_n,
            btch_exec_stat_ingestion_process_name, job_start,
            job_end, sftp.json_string_list_valid_xenon,
            sftp.json_string_list_rejected_xenon, sftp.errormsg_xenon,
            btch_exec_stat_ingestion_source_file,
            btch_exec_stat_ingestion_filepath,
            btch_exec_stat_ingestion_hdfs_dest_file_path,
            btch_exec_stat_ingestion_put_params, btch_exec_stat_ingestion_partition_date_method)
    #if restart_status == 'N':
    if restart_status == 'Y' or restart_status_btch == 'Y':
        restart_status == 'Y'
    xenon_put_method = 'Y'
    #sub_directory_list = sub_directory.split(",")
    #cntr_sub_directory_list = len(sub_directory_list)
    #i = 0
    #remote_file_path1 = remote_file_path
    #while i < cntr_sub_directory_list:
        #remote_sub_directory = sub_directory_list[i]
        #if remote_sub_directory.strip() != '':
            #remote_file_path = remote_file_path1 + "/" + remote_sub_directory
        #else:
            #remote_file_path = remote_file_path1

    sftp.sftp_upload_file(remote_file_path, local_file_path, hdfs_dest_datefile_path, put_params,
                          remote_file_regex_pattern,
                          remote_ctrlfile_regex_pattern, hdfs_dest_ctrlfile_path, local_ctrlfile_regex_pattern,xenon_put_method,partition_date_method,restart_status,
                          hdfs_sftp_filepath, hdfs_sftp_filename,local_hdfs_sftp_file_path,local_hdfs_sftp_temp_file_path,platform)
        #i = i + 1

    logging.info("#########################################################  Batch execution metrics for sftp and upload  ###########################################################")
    logging.info("Valid files sftped = " + str(sftp.json_string_list_valid_sftp))
    logging.info("Rejected files not sfted = " + str(sftp.json_string_list_rejected_sftp))
    logging.info("Error message for rejected files not ftped = " + str(sftp.errormsg_sftp))
    logging.info("Valid files moved to hdfs = " + str(sftp.json_string_list_valid_xenon))
    logging.info("Rejected files not moved to hdfs = " + str(sftp.json_string_list_rejected_xenon))
    logging.info("Error message for rejected files not moved to hdfs = " + str(sftp.errormsg_xenon))

    logging.info( "#############################  Starting to upload Batch execution metrics for sftp #############################")
    btch_exec_stat_ingestion_process_name = 'sftp'
    btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
        job_start)
    btch = Btch_Exec_Stat_Ingestion(xenon_url, xenon_user, xenon_password,edge_node,platform)
    json_string_valid_sftp, json_string_rejected_sftp, errormsg_sftp = btch.btch_exec_stat_ingestion(btch_exec_stat_ingestion_job_n,
                                  btch_exec_stat_ingestion_process_name, job_start,
                                  job_end, sftp.json_string_list_valid_sftp,
                                  sftp.json_string_list_rejected_sftp, sftp.errormsg_sftp,
                                  btch_exec_stat_ingestion_source_file,
                                  btch_exec_stat_ingestion_filepath,
                                  btch_exec_stat_ingestion_hdfs_dest_file_path,
                                  btch_exec_stat_ingestion_put_params,btch_exec_stat_ingestion_partition_date_method)
    logging.info("Valid btch file for sftp moved to hdfs = " + str(json_string_valid_sftp))
    logging.info("Rejected btch file for sftp not moved to hdfs = " + str(json_string_rejected_sftp))
    logging.info("Error message for btch file for sftp not moved to hdfs = " + str(errormsg_sftp))
    logging.info("#############################  Starting to upload Batch execution metrics for xenon #############################")
    btch_exec_stat_ingestion_process_name = 'xenon_put'
    btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
        job_start)
    btch = Btch_Exec_Stat_Ingestion(xenon_url, xenon_user, xenon_password,edge_node,platform)
    json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon = btch.btch_exec_stat_ingestion(btch_exec_stat_ingestion_job_n,
                                  btch_exec_stat_ingestion_process_name, job_start,
                                  job_end, sftp.json_string_list_valid_xenon,
                                  sftp.json_string_list_rejected_xenon, sftp.errormsg_xenon,
                                  btch_exec_stat_ingestion_source_file,
                                  btch_exec_stat_ingestion_filepath,
                                  btch_exec_stat_ingestion_hdfs_dest_file_path,
                                  btch_exec_stat_ingestion_put_params,btch_exec_stat_ingestion_partition_date_method)
    logging.info("Valid btch file for xenon moved to hdfs = " + str(json_string_valid_xenon))
    logging.info("Rejected btch file for xenon not moved to hdfs = " + str(json_string_rejected_xenon))
    logging.info("Error message for btch file for xenon  not moved to hdfs = " + str(errormsg_xenon))
    if json_string_list_rejected_xenon or json_string_rejected_sftp:
        raise Exception
    logging.info("#############################  End of batch execution metrics for sftp and upload  #############################")
    sftp.conn.close()
    """
    logging.info("#############################  Starting to upload any Batch execution files left over in btch_exec_stat_ingestion_filepath in prevruns if any #############################")
    btch_exec_stat_ingestion_hdfs_dest_ctrlfile_path = ''
    btch_exec_stat_ingestion_ctrlfile_regex_pattern = ''
    btch_exec_stat_ingestion_datafile_regex_pattern = 'btch'
    restart = 'Y'
    json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon, restart_status = \
        sftp.upload_file(btch_exec_stat_ingestion_filepath, btch_exec_stat_ingestion_hdfs_dest_file_path, restart,
                         btch_exec_stat_ingestion_put_params,
                         btch_exec_stat_ingestion_hdfs_dest_ctrlfile_path,
                         btch_exec_stat_ingestion_datafile_regex_pattern,
                         btch_exec_stat_ingestion_ctrlfile_regex_pattern,btch_exec_stat_ingestion_partition_date_method)
    job_end = now.strftime('%Y-%m-%dT%H_%M_%S')
    if restart_status == 'Y':
        logging.info(
            "#########################################################  Batch execution metrics for batch execution files left over in btch_exec_stat_ingestion_filepath in prevruns ###########################################################")
        logging.info("Valid btch file for sftp/xenon moved to hdfs by restart = " + str(json_string_list_valid_xenon))
        logging.info("Rejected btch file for sftp/xenon  not moved to hdfs by restart = " + str(json_string_list_rejected_xenon))
        logging.info("Error message for btch file for sftp/xenon not moved to hdfs by restart = " + str(errormsg_xenon))
        logging.info("restart status = " + restart_status)
        btch_exec_stat_ingestion_process_name = 'btch_restart'
        btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
            job_start)
        btch = Btch_Exec_Stat_Ingestion(xenon_url, xenon_user, xenon_password,edge_node)
        json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon = btch.btch_exec_stat_ingestion(
            btch_exec_stat_ingestion_job_n,
            btch_exec_stat_ingestion_process_name, job_start,
            job_end, sftp.json_string_list_valid_xenon,
            sftp.json_string_list_rejected_xenon, sftp.errormsg_xenon,
            btch_exec_stat_ingestion_source_file,
            btch_exec_stat_ingestion_filepath,
            btch_exec_stat_ingestion_hdfs_dest_file_path,
            btch_exec_stat_ingestion_put_params, btch_exec_stat_ingestion_partition_date_method)
        #logging.info("Valid btch file for sftp/xenon moved to hdfs by restart= " + str(json_string_valid_xenon))
        #logging.info(
            #"Rejected btch file for sftp/xenon  not moved to hdfs by restart = " + str(json_string_rejected_xenon))
        #logging.info("Error message for btch file for sftp/xenon  not moved to hdfs by restart = " + str(errormsg_xenon))
    """

    util.logclose()


#@staticmethod
def upload_main(xenon_url, xenon_user, xenon_password,put_params,hdfs_dest_datefile_path,hdfs_dest_ctrlfile_path,
         local_file_path, local_datafile_regex_pattern,
         local_ctrlfile_regex_pattern,
         btch_exec_stat_ingestion_job_n, btch_exec_stat_ingestion_filepath,
         btch_exec_stat_ingestion_hdfs_dest_file_path, btch_exec_stat_ingestion_put_params,log_filename,log_filepath,sftp_method='N',xenon_method='Y',partition_date_method='',btch_exec_stat_ingestion_partition_date_method='',edge_node='',platform=''):

    #util.is_dir_exists(log_filepath)

    if not os.path.exists(local_file_path):
        os.makedirs(local_file_path)

    if not os.path.exists(log_filepath):
        os.makedirs(log_filepath)

    if not os.path.exists(btch_exec_stat_ingestion_filepath):
        os.makedirs(btch_exec_stat_ingestion_filepath)

    now = datetime.now()
    job_start = now.strftime('%Y-%m-%dT%H_%M_%S')
    filename = log_filename + job_start + ".log"
    logfilename = log_filepath + "/" + filename
    util.logs(logfilename)
    sftp = Sftp_Xenon(url=xenon_url,user=xenon_user,password=xenon_password,sftp_method=sftp_method,xenon_method=xenon_method,edge_node=edge_node,platform=platform)
    restart = 'N'
    sftp.upload_file(local_file_path,hdfs_dest_datefile_path,restart,put_params,
                          hdfs_dest_ctrlfile_path,
                          local_datafile_regex_pattern, local_ctrlfile_regex_pattern,partition_date_method)

    job_end = now.strftime('%Y-%m-%dT%H_%M_%S')
    logging.info("#########################################################  Batch execution metrics for upload  ###########################################################")
    logging.info("Valid files moved to hdfs = " + str(sftp.json_string_list_valid_xenon))
    logging.info("Rejected files not moved to hdfs = " + str(sftp.json_string_list_rejected_xenon))
    logging.info("Error message for rejected files not moved to hdfs = " + str(sftp.errormsg_xenon))
    logging.info("#############################  Starting to upload Batch execution metrics for xenon #############################")
    btch_exec_stat_ingestion_process_name = 'xenon_put'
    btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
        job_start)
    btch=Btch_Exec_Stat_Ingestion(xenon_url,xenon_user,xenon_password,edge_node,platform)
    json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon=btch.btch_exec_stat_ingestion(btch_exec_stat_ingestion_job_n,
                                                          btch_exec_stat_ingestion_process_name, job_start,
                                                          job_end, sftp.json_string_list_valid_xenon,
                                                          sftp.json_string_list_rejected_xenon, sftp.errormsg_xenon,
                                                          btch_exec_stat_ingestion_source_file,
                                                          btch_exec_stat_ingestion_filepath,
                                                          btch_exec_stat_ingestion_hdfs_dest_file_path,
                                                          btch_exec_stat_ingestion_put_params,btch_exec_stat_ingestion_partition_date_method)
    logging.info("Valid btch file for xenon moved to hdfs = " + str(json_string_valid_xenon))
    logging.info("Rejected btch file for xenon not moved to hdfs = " + str(json_string_rejected_xenon))
    logging.info("Error message for btch file for xenon  not moved to hdfs = " + str(errormsg_xenon))
    logging.info("#############################  End of batch execution metrics for upload  #############################")
    if json_string_rejected_xenon:
        raise Exception
    logging.info("#############################  Starting to upload any Batch execution files left over in btch_exec_stat_ingestion_filepath in prevruns  #############################")
    btch_exec_stat_ingestion_hdfs_dest_ctrlfile_path = ''
    btch_exec_stat_ingestion_ctrlfile_regex_pattern = ''
    btch_exec_stat_ingestion_datafile_regex_pattern = 'btch'
    restart = 'Y'
    json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon, restart_status = \
        sftp.upload_file(btch_exec_stat_ingestion_filepath, btch_exec_stat_ingestion_hdfs_dest_file_path, restart, btch_exec_stat_ingestion_put_params,
                     btch_exec_stat_ingestion_hdfs_dest_ctrlfile_path,btch_exec_stat_ingestion_datafile_regex_pattern,btch_exec_stat_ingestion_ctrlfile_regex_pattern,btch_exec_stat_ingestion_partition_date_method)
    job_end = now.strftime('%Y-%m-%dT%H_%M_%S')
    logging.info(
        "#########################################################  Batch execution metrics for batch execution files left over in btch_exec_stat_ingestion_filepath in prevruns ###########################################################")
    logging.info("Valid btch files moved to hdfs by restart = " + str(json_string_list_valid_xenon))
    logging.info("Rejected btch files not moved to hdfs by restart = " + str(json_string_list_rejected_xenon))
    logging.info("Error message for rejected btch files not moved to hdfs by restart = " + str(errormsg_xenon))
    logging.info("restart status = " + restart_status)
    if restart_status == 'Y':
        btch_exec_stat_ingestion_process_name = 'btch_restart'
        btch_exec_stat_ingestion_source_file = "btch_exec_stat_ingestion." + btch_exec_stat_ingestion_job_n + '.' + btch_exec_stat_ingestion_process_name + "." + str(
            job_start)
        btch = Btch_Exec_Stat_Ingestion(xenon_url, xenon_user, xenon_password,edge_node,platform)
        json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon = btch.btch_exec_stat_ingestion(
            btch_exec_stat_ingestion_job_n,
            btch_exec_stat_ingestion_process_name, job_start,
            job_end, sftp.json_string_list_valid_xenon,
            sftp.json_string_list_rejected_xenon, sftp.errormsg_xenon,
            btch_exec_stat_ingestion_source_file,
            btch_exec_stat_ingestion_filepath,
            btch_exec_stat_ingestion_hdfs_dest_file_path,
            btch_exec_stat_ingestion_put_params, btch_exec_stat_ingestion_partition_date_method)
        logging.info("Valid btch file for xenon moved to hdfs by restart= " + str(json_string_valid_xenon))
        logging.info(
            "Rejected btch file for xenon not moved to hdfs by restart = " + str(json_string_rejected_xenon))
        logging.info(
            "Error message for btch file for xenon not moved to hdfs by restart = " + str(errormsg_xenon))

    util.logclose()

