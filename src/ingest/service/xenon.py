import requests
import urllib3
from requests.auth import HTTPBasicAuth
import json
import re
import logging
#from utility import util
#from partition_date import Partition_date
from ingest.utility import util
from ingest.partition_date import Partition_date
import traceback

class Xenon(object):

    def __init__(self, url, user, password, xenon_method='Y',edge_node='',platform='',json_string_list_valid_xenon=[],
                 json_string_list_rejected_xenon=[],
                 errormsg_xenon=''):
        self.url = url
        self.user = user
        self.password = password
        self.platform = platform
        self.json_string_list_valid_xenon = json_string_list_valid_xenon
        self.json_string_list_rejected_xenon = json_string_list_rejected_xenon
        self.errormsg_xenon = errormsg_xenon
        self.edge_node = edge_node
        if xenon_method.upper() == 'Y':
            logging.info("Getting xenon datanode details")
            operations, servers, datanode, errormsg_xenon = self.get_servers()
            if errormsg_xenon == '':
                logging.info("Got datanode details | %s" % (datanode))
                self.datanode = datanode
                self.operations = operations
                self.servers = servers
            logging.info("Getting xenon namespace details")
            namespacebr3, errormsg_xenon = self.get_namespace()
            if errormsg_xenon == '':
                logging.info("Got namespace details | %s" % (namespacebr3))
                self.namespacebr3 = namespacebr3

    def get_namespace(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        namespacesbr3 = ''
        errormsg = ''
        try:
            response = requests.get(self.url,
                                    auth=HTTPBasicAuth(self.user, self.password),
                                    verify=False)
            values = json.loads(response.text)
            if int(response.status_code / 100) == 2:

                namespaces = values["namespaces"]
                for name in namespaces:
                    if self.platform != 'bigred':
                        if name["displayName"] == 'bigRED3':
                            namespacesbr3 = name["name"]
                    else:
                        if name["displayName"] == 'bigRED (Legacy)':
                            namespacesbr3 = name["name"]
                print_msg = "Getting xenon namespace | Success | + %s + for the platform  | %s | namespace | %s" % (
                    str(response.status_code), self.platform, namespacesbr3) + " | " + response.content.decode('utf-8')
                logging.info(print_msg)

                return namespacesbr3, errormsg
            else:
                self.errormsg_xenon = "Getting xenon namespace | Error | " + str(
                    response.status_code) + " | " + response.content.decode(
                    'utf-8')
                logging.error(self.errormsg_xenon)
                logging.error(traceback.format_exc())
                raise Exception(self.errormsg_xenon)
                # return namespacesbr3, self.errormsg_xenon
                # exit(response.status_code)
        except requests.exceptions.RequestException as e:
            self.errormsg_xenon = "Error: {}".format(e)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_xenon)
            raise Exception(self.errormsg_xenon)
            # return namespacesbr3, self.errormsg_xenon

    def get_servers(self):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        errormsg_xenon = ''
        try:
            response = requests.options(self.url,
                                        auth=HTTPBasicAuth(self.user, self.password),
                                        headers={'Accept': 'application/x-get-service-info+json'},
                                        verify=False)

            if int(response.status_code / 100) == 2:
                # print_msg = "Getting datanode | success | + %s" % (str(response.status_code)) + " | " + response.content.decode('utf-8')
                print_msg = "Getting datanode | success | + %s" % (str(response.status_code))
                logging.info(print_msg)
                values = json.loads(response.text)
                operations = values["operations"]
                servers = values["servers"]
                datanode = values["servers"][0]
                return operations, servers, datanode, errormsg_xenon
            else:
                self.errormsg_xenon = "Getting datanode | Error | " + str(
                    response.status_code) + " | " + response.content.decode(
                    'utf-8')
                logging.error(self.errormsg_xenon)
                logging.error(traceback.format_exc())
                raise Exception(self.errormsg_xenon)
                # return operations, servers, datanode, self.errormsg_xenon
        except requests.exceptions.RequestException as e:
            self.errormsg_xenon = "Error: {}".format(e)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_xenon)
            raise Exception(self.errormsg_xenon)
            # return namespacesbr3, self.errormsg_xenon

    def exception(self, errormsg=''):
        try:
            raise Exception(errormsg)
        except Exception:
            return errormsg

    def get_hdfs_path(self, local_file_path, local_source_file, dest_file_path, partition_date_method, dest_ctrlfile_f):

        logging.info("Inside Xenon.get_hdfs_path")
        partition_cols = ''
        schema_name = ''
        landing_table_name = ''
        if partition_date_method != '':
            partition_date = Partition_date(local_source_file, dest_file_path, dest_ctrlfile_f)
            logging.info("Getting partition_date from partition_date.Partition_date.method_call()")
            hdfs_path,schema_name,landing_table_name = partition_date.method_call(partition_date_method)
            full_url = self.datanode + "xenon/fs/" + self.namespacebr3 + hdfs_path + "/" + local_source_file
            logging.info("hdfs path returned  = %s" % (hdfs_path))
            dest_path = hdfs_path + "/" + local_source_file
            hdfs_path_success = 'Y'
            if hdfs_path == '':
                self.errormsg_xenon = 'Cannot derive partition date for filename | %s | for partition_date_method | %s' % (
                    local_source_file, partition_date_method)
                logging.error(self.errormsg_xenon)
                json_string_rejected_xenon = {
                    "source_path": local_file_path + "/" + local_source_file,
                    "dest_file": '',
                    "errormsg": self.errormsg_xenon
                }
                self.json_string_list_rejected_xenon.append(json_string_rejected_xenon)
                hdfs_path_success = 'N'
            partition_cols = util.get_partition_cols(hdfs_path)
            return full_url, dest_path, self.errormsg_xenon, hdfs_path_success,partition_cols,schema_name,landing_table_name
        else:
            logging.info("No partition_date_method specified so will upload to specified hdfs path in config file")
            logging.info("hdfs path  = %s" % (dest_file_path))
            hdfs_path = dest_file_path
            dest_path = hdfs_path + "/" + local_source_file
            full_url = self.datanode + "xenon/fs/" + self.namespacebr3 + hdfs_path + "/" + local_source_file
            # logging.info("Xenon Url for uploading the file = %s" % (full_url))
            self.errormsg_xenon = ''
            hdfs_path_success = 'Y'

        return full_url, dest_path, self.errormsg_xenon, hdfs_path_success,partition_cols,schema_name,landing_table_name


    def put_file(self, local_source_file, local_file_path, dest_datefile_path, params={},
                 dest_ctrlfile_path='', local_ctrlfile_regex_pattern='', partition_date_method=''):

        logging.info("Executing xenon put process")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        json_string_valid_xenon = ''
        json_string_rejected_xenon = ''
        local_file_path_filename = local_file_path + "/" + local_source_file
        try:
            with open(local_file_path_filename, "rb") as f:
                # filename = local_source_path_file.split("/")[-1]
                data = f.read()
                if dest_ctrlfile_path == '':
                    print_msg = "No Control file path specified for xenon put process |filename | %s|moving file to datafile path " % local_source_file
                    logging.info(print_msg)
                    dest_ctrlfile_f = 'N'
                    full_url, dest_path, errormsg_xenon, hdfs_path_success,partition_cols,schema_name,landing_table_name = self.get_hdfs_path(local_file_path,
                                                                                                local_source_file,
                                                                                                dest_datefile_path,
                                                                                                partition_date_method,
                                                                                                dest_ctrlfile_f)
                    if hdfs_path_success != 'Y':
                        return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon

                    logging.info("Xenon url for uploading datafile | " + full_url)

                else:
                    if re.search(local_ctrlfile_regex_pattern, local_source_file):
                        print_msg = "Control file pattern specified for xenon put process | %s | matching with local source file | %s" % (
                            local_ctrlfile_regex_pattern, local_source_file)
                        logging.info(print_msg)
                        dest_ctrlfile_f = 'Y'

                        
                        full_url, dest_path, errormsg_xenon, hdfs_path_success,partition_cols,schema_name,landing_table_name = self.get_hdfs_path(local_file_path,
                                                                                                    local_source_file,
                                                                                                    dest_ctrlfile_path,
                                                                                                    partition_date_method,
                                                                                                    dest_ctrlfile_f)
                        if hdfs_path_success != 'Y':
                            return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon
                        logging.info("Xenon url for uploading control file | " + full_url)

                    else:
                        print_msg = "Control file pattern specified for xenon put process is | %s | not matching with local source file | %s |\
                        moving file to datafile path" % (local_ctrlfile_regex_pattern, local_source_file)
                        logging.info(print_msg)
                        dest_ctrlfile_f = 'N'
                        full_url, dest_path, errormsg_xenon, hdfs_path_success,partition_cols,schema_name,landing_table_name = self.get_hdfs_path(local_file_path,
                                                                                                    local_source_file,
                                                                                                    dest_datefile_path,
                                                                                                    partition_date_method,
                                                                                                    dest_ctrlfile_f)
                        if hdfs_path_success != 'Y':
                            return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon
                        logging.info("Xenon url for uploading datafile | " + full_url)

        except IOError as e:
            self.errormsg_xenon = "I/O error({0}): {1}".format(e.errno, e.strerror)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_xenon)
            errormsg_xenon = self.exception(self.errormsg_xenon)
            raise e
            # return json_string_valid_xenon, json_string_rejected_xenon, errormsg_xenon
        except ValueError as e:
            self.errormsg_xenon = "Not able to read the file | %s : | %s" % (e, local_file_path_filename)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_xenon)
            self.errormsg_xenon = self.exception(self.errormsg_xenon)
            raise e
            # return json_string_valid_xenon, json_string_rejected_xenon, self.errormsg_xenon
        response = requests.put(full_url,
                                auth=HTTPBasicAuth(self.user, self.password),
                                headers={'Accept': 'application/x-write-file'},
                                data=data,
                                params=params,
                                verify=False)

        if int(response.status_code / 100) == 2:
            print_msg = "Xenon put file success | File moved to  hdfs location | %s | + %s" % (
                full_url, str(response.status_code)) + " | " + response.content.decode('utf-8')
            logging.info(print_msg)
            json_string_valid_xenon = {
                "source_file": local_file_path + "/" + local_source_file,
                "dest_file": dest_path
            }
            self.json_string_list_valid_xenon.append(json_string_valid_xenon)
            # self.remove_file_local(local_file_path, local_source_file)
            util.remove_file_local(local_file_path, local_source_file)
            if partition_date_method != '' and partition_cols != '' and landing_table_name != '':
                util.add_partition(schema_name, landing_table_name, partition_cols, self.edge_node, self.user,
                self.password)

        else:
            self.errormsg_xenon = "Xenon put file | Error | " + full_url + "|" + str(
                response.status_code) + " | " + response.content.decode('utf-8')
            logging.error(self.errormsg_xenon)
            json_string_rejected_xenon = {
                "source_path": local_file_path + "/" + local_source_file,
                "dest_file": dest_path,
                "errormsg": self.errormsg_xenon
            }

            self.json_string_list_rejected_xenon.append(json_string_rejected_xenon)


        return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon


    def get_file(self, get_path):

        errormsg_xenon = ''
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        full_url = self.datanode + "/xenon/fs/" + self.namespacebr3 + get_path
        response = requests.get(full_url,
                                auth=HTTPBasicAuth(self.user, self.password),
                                headers={'Accept': 'application/x-read-file'},
                                # params=params,
                                verify=False)
        if int(response.status_code / 100) == 2:
            # print_msg = "Xenon get file success | %s | + %s" % (
            # full_url, str(response.status_code)) + " | " + response.content.decode('utf-8')
            print_msg = "Xenon get file success | %s | + %s " % (full_url, str(response.status_code))
            logging.info(print_msg)

        else:
            errormsg_xenon = "Xenon get file | Error | " + full_url + "|" + str(
                response.status_code) + " | " + response.content.decode('utf-8')
            logging.error(errormsg_xenon)

        return response.text, errormsg_xenon

    def delete_file(self, path, params):

        errormsg_xenon = ''
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        full_url = self.datanode + "/xenon/fs/" + self.namespacebr3 + path
        response = requests.delete(full_url,
                                   auth=HTTPBasicAuth(self.user, self.password),
                                   headers={'Accept': 'application/x-delete-file'},
                                   params=params,
                                   verify=False)
        if int(response.status_code / 100) == 2:
            # print_msg = "Xenon get file success | %s | + %s" % (
            # full_url, str(response.status_code)) + " | " + response.content.decode('utf-8')
            print_msg = "Xenon delete file success | %s | + %s " % (full_url, str(response.status_code))
            logging.info(print_msg)
        else:
            errormsg_xenon = "Xenon delete file | Error | " + full_url + "|" + str(
                response.status_code) + " | " + response.content.decode('utf-8')
            logging.error(errormsg_xenon)

        return response.text, errormsg_xenon
