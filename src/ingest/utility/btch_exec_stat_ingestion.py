import logging
#from service.xenon import Xenon
#from utility import util

from ingest.service.xenon import Xenon
from ingest.utility import util



class Btch_Exec_Stat_Ingestion(object):

    def __init__(self, url, user, password,edge_node,platform):
        self.url = url
        self.user = user
        self.password = password
        self.edge_node = edge_node
        self.platform = platform

    def btch_exec_stat_ingestion(self,job_n,process_name, job_start, job_end, json_string_list_valid,
                                 json_string_list_rejected, errormsg, btch_exec_stat_ingestion_source_file,
                                 btch_exec_stat_ingestion_filepath, hdfs_dest_file_path, put_params,partition_date_method=''):

        util.is_dir_exists(btch_exec_stat_ingestion_filepath)
        btch_exec_stat_ingestion = job_n + '|' + process_name + '|' + str(job_start) + '|' + str(job_end) + '|' + str(
            json_string_list_valid) + '|' + str(json_string_list_rejected) + '|' + str(errormsg)
        btch_exec_stat_ingestion_filepath_file = btch_exec_stat_ingestion_filepath + "/" + btch_exec_stat_ingestion_source_file
        with open(btch_exec_stat_ingestion_filepath_file, "a") as f:
            f.write(btch_exec_stat_ingestion)

        xenon_call = Xenon(self.url, self.user, self.password,edge_node=self.edge_node,platform=self.platform)
        xenon_call.json_string_list_valid_xenon = []
        xenon_call.json_string_list_rejected_xenon = []

        json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon = xenon_call.put_file(
            btch_exec_stat_ingestion_source_file, btch_exec_stat_ingestion_filepath, hdfs_dest_file_path, put_params,partition_date_method=partition_date_method)

        return json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon

        """
        print_msg_valid = "Valid batch exec files moved to hdfs = " + str(json_string_valid_xenon)
        print_msg_rejected = "Rejected batch exec files not moved to hdfs = " + str(json_string_rejected_xenon)
        print_msg_error = "Error message for batch exec rejected files not moved to hdfs = " + str(errormsg_xenon)

        print(print_msg_valid)
        print(print_msg_rejected)
        print(print_msg_error)
        logging.info(print_msg_valid)
        logging.info(print_msg_rejected)
        logging.info(print_msg_error)
        """
