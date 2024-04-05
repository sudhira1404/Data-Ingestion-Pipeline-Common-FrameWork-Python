
import paramiko
import time
import sys
import pysftp
import os
import stat
import re
import logging
#from service.xenon import Xenon
#from utility import util

from ingest.service.xenon import Xenon
from ingest.utility import util
import pathlib
import shutil
import traceback

class Sftp_Xenon(Xenon):

    def __init__(self, url='', user='', password='',xenon_method='',edge_node='', host='', username='', private_key='',sftp_method='',method='',remote_passwordauth='',remoteuser_password='',platform='',json_string_list_valid_xenon=[],
                 json_string_list_rejected_xenon=[], errormsg_xenon='', json_string_list_valid_sftp=[],
                 json_string_list_rejected_sftp=[], errormsg_sftp=''):
        super(Sftp_Xenon, self).__init__(url, user, password,xenon_method,edge_node,platform,json_string_list_valid_xenon,
                                         json_string_list_rejected_xenon, errormsg_xenon)
        self.host = host
        self.username = username
        self.private_key = private_key
        self.json_string_list_valid_sftp = json_string_list_valid_sftp
        self.json_string_list_rejected_sftp = json_string_list_rejected_sftp
        self.errormsg_sftp = errormsg_sftp
        if sftp_method.upper() == 'Y':
            try:
                if method == 'paramikosftp':
                    transport = paramiko.Transport((host, 22))
                    if remote_passwordauth.upper() != 'Y':
                        key = paramiko.RSAKey.from_private_key_file(private_key)
                        transport.connect(username=username, pkey=key)
                    else:
                        transport.connect(username=username, password=remoteuser_password)
                    conn = paramiko.SFTPClient.from_transport(transport)
                    # paramiko.SFTPClient.MAX_REQUEST_SIZE = 1024
                    #print('Connected to remote node | %s | using paramikosftp' % host)
                    logging.info('Connected to remote node | %s |using paramikosftp' % host)
                    self.conn = conn
                else:
                    if remote_passwordauth.upper() != 'Y':
                        conn = pysftp.Connection(host=host, username=username, private_key=private_key)
                    else:
                        conn = pysftp.Connection(host=host, username=username, password=remoteuser_password)
                    #print('Connected to remote node | %s |using pysftp' % host)
                    logging.info('Connected to remote node | %s | using pysftp' % host)
                    self.conn = conn
            except Exception as e:
                self.errormsg_sftp = 'Could not connect to remote node %s | Exited with error %s' % (host, e)
                logging.error(self.errormsg_sftp)
                self.errormsg_sftp = errormsg_sftp
                self.errormsg_sftp = self.exception(self.errormsg_sftp)
                logging.error(traceback.format_exc())
                raise e
                #raise ConnectionError(errormsg_sftp)

    def myexec(self, ssh, cmd):
        try:
            start = time.time()
            stdin, stdout, stderr = ssh.exec_command(cmd)
            # while not stdout.channel.exit_status_ready() and not stdout.channel.recv_ready():
            # time.sleep(1)
            err_list = [line for line in stderr.read().decode('utf-8').splitlines()]
            out_list = [line for line in stdout.read().decode('utf-8').splitlines()]
            logging.info("stdout=" + str(out_list))
            logging.info("stderr=" + str(err_list))
            end = time.time()
        except Exception as error:
            print(str(error))
            sys.exit(1)
        exit_status = stdout.channel.recv_exit_status()
        elapsed1 = end - start
        elapsed = str(elapsed1)
        stdout.close()
        stderr.close()
        if exit_status > 0:
            logging.info("Error executing the command " + cmd + " with exit_status=" + str(
                exit_status) + " Elapsed time = " + elapsed)
            ssh.close()
            return out_list, err_list, exit_status
            #exit(exit_status)
        else:
            logging.info("Completed executing the command " + " with exit_status=" + str(
                exit_status) + " Elapsed time = " + elapsed)
        return out_list, err_list,exit_status

    def ssh_connect(self, node, node_user, node_pwd):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=node, username=node_user, password=node_pwd)
        except paramiko.ssh_exception.SSHException:
            raise paramiko.ssh_exception.SSHException("SSH Connection failed")
        return ssh

    def ssh_connect_public_key(self, node, node_user, remote_indentity_file_path):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=node, username=node_user, key_filename=remote_indentity_file_path)
        except paramiko.ssh_exception.SSHException:
            raise paramiko.ssh_exception.SSHException("SSH Connection failed")
        return ssh

    def br_ssh_upload_file(self, ssh, remote_node, remote_user, remote_indentity_file_path, remote_file_path,
                           local_file_path):
        command = "scp -i" + " " + remote_indentity_file_path + " " + remote_user + '@' + remote_node + ":" + remote_file_path + " " + local_file_path
        # command="scp -i ~/.ssh/id_rsa svbigred04@xyzmft.target.com:/Inbox/* /home/corp.target.com/svmdedmp"
        output, error,exit_status = self.myexec(ssh, command)
        return output, error

    def sftp_get_file(self, remote_file_path, local_file_path, remote_source_file_size, cntr):
        get_success = ''
        try:
            self.conn.get(remote_file_path, local_file_path)
            get_success = 'success'
            cntr = cntr + 1
        except Exception as e:
            self.errormsg_sftp = "Error while transferring file | %s : | %s" % (e, remote_file_path)
            logging.error(self.errormsg_sftp)
            json_string_rejected_sftp = {
                "source_file": remote_file_path,
                "source_file_size": remote_source_file_size,
                "dest_file": '',
                "dest_file_size": '',
                "errormsg": self.errormsg_sftp
            }
            self.json_string_list_rejected_sftp.append(json_string_rejected_sftp)

        return cntr, get_success

    def sftp_file_size_check(self, remote_file_path_filename, local_file_path_filename, remote_file_size):

        if (os.path.isfile(local_file_path_filename)):
            local_dest_file_size = os.path.getsize(local_file_path_filename)
            if local_dest_file_size == remote_file_size:
                json_string_valid_sftp = {
                    "source_file": remote_file_path_filename,
                    "source_file_size": remote_file_size,
                    "dest_file": local_file_path_filename,
                    "dest_file_size": local_dest_file_size
                }
                self.json_string_list_valid_sftp.append(json_string_valid_sftp)
            else:
                json_string_rejected_sftp = {
                    "source_file": remote_file_path_filename,
                    "source_file_size": remote_file_size,
                    "dest_file": local_file_path_filename,
                    "dest_file_size": local_dest_file_size,
                    "errormsg": "filesize between source and dest not matching"
                }
                for i in json_string_rejected_sftp:
                    if json_string_rejected_sftp["source_file"][i] == remote_file_path_filename:
                        continue
                self.json_string_list_rejected_sftp.append(json_string_rejected_sftp)
        else:
            logging.info("File does not exist in the localpath | %s" % (local_file_path_filename))

    def sftp_upload_file(self, remote_file_path, local_file_path, dest_datefile_path, params={}, remote_file_regex_pattern ='',remote_ctrlfile_regex_pattern='',
                         dest_ctrlfile_path='', local_ctrlfile_regex_pattern='',xenon_put_method='',partition_date_method='',restart_status='N',
                         hdfs_sftp_filepath='', hdfs_sftp_filename='',local_hdfs_sftp_file_path='',local_hdfs_sftp_temp_file_path='',platform=''):

        logging.info("########################  Inside sftp_upload_file method ########################")
        #util.is_dir_exists(local_file_path)
        #print("local_hdfs_sftp_file_path " + local_hdfs_sftp_file_path)
        if len(dest_ctrlfile_path) > 0 and len(local_ctrlfile_regex_pattern) == 0:
            logging.error("Wrong status for parameter ,local ctrlfile_regex_pattern should not be empty, else make dest_path_ctrlfile empty")
            raise Exception(
                "Wrong status for parameter ,local ctrlfile_regex_pattern should not be empty, else make dest_path_ctrlfile empty")
        if len(dest_ctrlfile_path) == 0 and len(local_ctrlfile_regex_pattern) > 0:
            logging.error("Wrong status for parameter dest_path_ctrlfile should not be empty,else make local ctrlfile_regex_pattern empty")
            raise Exception(
                "Wrong status for parameter dest_path_ctrlfile should not be empty,else make local ctrlfile_regex_pattern empty")
        if len(dest_ctrlfile_path) == 0 and len(remote_ctrlfile_regex_pattern) > 0:
            logging.error("Wrong status for parameter dest_path_ctrlfile should not be empty,else make remote_ctrlfile_regex_pattern empty")
            raise Exception(
                "Wrong status for parameter dest_path_ctrlfile should not be empty,else make remote_ctrlfile_regex_pattern empty")

        cntr_pattern = 1
        cntr_no_pattern = 1
        try:
            filelist1 = self.conn.listdir_attr(remote_file_path)
            filelist_filename1 = self.conn.listdir(remote_file_path)
            filelist,filelist_filename = self.incr_sftp_files(self.url, self.user, self.password, hdfs_sftp_filepath, hdfs_sftp_filename,
                                        filelist1,filelist_filename1,local_hdfs_sftp_file_path,local_hdfs_sftp_temp_file_path,platform)
        except Exception as e:
            self.errormsg_sftp = 'Could not get to remote filepath %s | Exited with error %s' % (remote_file_path, e)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_sftp)
            self.errormsg_sftp = self.exception(self.errormsg_sftp)
            raise Exception(self.errormsg_sftp)

        #filelist_filename = self.conn.listdir(remote_file_path)
        tot_files_remote = len(filelist_filename)
        files = (filelist_det.filename for filelist_det in filelist
                 if not stat.S_ISDIR(filelist_det.st_mode) and (not filelist_det.filename.startswith('.') and not filelist_det.filename.startswith('*')))
        filelist_excluded = list(files)
        if not filelist_excluded and restart_status == 'N':
            self.errormsg_sftp = 'No new file in remote file path | %s | in remote node |  %s| files already sftped is in hdfs location if paramter hdfs_sftp_filename is set | %s ' % (
                remote_file_path,self.host,hdfs_sftp_filepath + '/' + hdfs_sftp_filename)
            if restart_status == 'Y':
                logging.info(self.errormsg_sftp)
                exit(0)
            else:
                logging.error(self.errormsg_sftp)
            #logging.error(traceback.format_exc())
                raise FileNotFoundError(self.errormsg_sftp)
        if not filelist_excluded and restart_status == 'Y':
            self.errormsg_sftp = 'No new file in remote node and may be leftover data or btch_exec files were loaded in this run | %s | remote file path |  %s | files already sftped is in hdfs location if paramter hdfs_sftp_filename is set | %s ' % (
                self.host, remote_file_path,hdfs_sftp_filepath + '/' + hdfs_sftp_filename)
            logging.info(self.errormsg_sftp)
            exit(0)
        if remote_file_regex_pattern == '' and remote_ctrlfile_regex_pattern == '':
            print_msg = "No remote data file and control file pattern specified | fetching all files from remote excluding files which are dir or starting with star or dot | %s | files | %s" % (
                local_file_path, filelist_excluded)
            logging.info(print_msg)
        else:

            if remote_file_regex_pattern != '' or remote_ctrlfile_regex_pattern != '':
                cntr = 0
                print_msg = "############### list of remote files used to match with data file pattern | %s | or control file pattern | %s ###############" % (remote_file_regex_pattern,remote_ctrlfile_regex_pattern)
                logging.info(print_msg)
                logging.info('tot_files_remote' ' =  ' + str(filelist_filename))
                for element in filelist_filename:
                    remote_source_file_pattern = False
                    remote_source_file_pattern1 = False
                    if remote_file_regex_pattern != '' and re.search(remote_file_regex_pattern, element):
                        remote_source_file_pattern = True
                        print_msg = "Filename | %s | matched with remote data file pattern | %s " % (
                            element, remote_file_regex_pattern)
                    if remote_ctrlfile_regex_pattern != '' and not remote_source_file_pattern and re.search(remote_ctrlfile_regex_pattern, element):
                        remote_source_file_pattern = True
                        print_msg = "Filename | %s | matched with remote control file pattern | %s" % (
                            element, remote_ctrlfile_regex_pattern)
                    else:
                        if remote_file_regex_pattern == '':
                            remote_source_file_pattern1 = True
                            print_msg = "Filename | %s | matched with remote data file pattern | %s " % (
                            element, remote_file_regex_pattern)
                            logging.info(print_msg)

                    if remote_source_file_pattern:
                        logging.info(print_msg)
                        cntr = cntr + 1
                    else:
                        if remote_source_file_pattern1 != True:
                            print_msg = "filename | %s | not matching with remote file pattern | %s | or remote control file pattern | %s" % (
                                element,remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                            logging.info(print_msg)
                if cntr == 0:
                    if remote_file_regex_pattern != '' and remote_ctrlfile_regex_pattern != '':
                        self.errormsg_sftp = 'No file in | remote file path |  %s | matching with remote data file pattern | %s | or remote control file pattern | %s ' % (
                            remote_file_path, remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                    elif remote_file_regex_pattern != '':
                        self.errormsg_sftp = 'No file in remote file path |  %s | matching with remote data file pattern | %s ' % (
                            remote_file_path, remote_file_regex_pattern)
                    else:
                        self.errormsg_sftp = 'No new file in remote file path |  %s | matching with remote control file pattern | %s ' % (
                            remote_file_path, remote_ctrlfile_regex_pattern)
                    if restart_status == 'Y':
                        logging.info(self.errormsg_sftp)
                        exit(0)
                    else:
                        logging.error(self.errormsg_sftp)
                        #logging.error(traceback.format_exc())
                        raise Exception(self.errormsg_sftp)
        for filelist_det in filelist:
            remote_file = ''
            get_success = ''
            remote_source_file_pattern =  False
            if stat.S_ISDIR(filelist_det.st_mode) or filelist_det.filename.startswith('.') or filelist_det.filename.startswith('*'):
                continue
            else:
                remote_file = filelist_det.filename
                remote_file_size = filelist_det.st_size
                local_file_path_filename = local_file_path + "/" + remote_file
                remote_file_path_filename = remote_file_path + "/" + remote_file
                if remote_file_regex_pattern == '' and remote_ctrlfile_regex_pattern == '':
                    print_msg = "############################# " + "No remote data and control file pattern specified | prepare fetching from remote_file_path | %s | %s file |filename | %s #############################" % (
                        remote_file_path, str(cntr_no_pattern), filelist_det.filename)
                    logging.info(print_msg)
                    cntr_no_pattern, get_success = self.sftp_get_file(remote_file_path_filename,
                                                                      local_file_path_filename,
                                                                      remote_file_size, cntr_no_pattern)
                    logging.info("Get the filesize of the file that is ftped to local")
                    self.sftp_file_size_check(remote_file_path_filename, local_file_path_filename, remote_file_size)
                else:
                    if remote_file_regex_pattern != '' and re.search(remote_file_regex_pattern, remote_file):
                        remote_source_file_pattern = True
                        #print(re.search(remote_file_regex_pattern, remote_file))
                        print_msg = " #############################  " + str(
                            cntr_pattern ) + " Filename | %s | matching with remote datafile pattern |  %s | prepare fetching from remote #############################" % (
                                        remote_file_path_filename, remote_file_regex_pattern,
                                    )
                    if remote_ctrlfile_regex_pattern != '' and not remote_source_file_pattern and re.search(remote_ctrlfile_regex_pattern, remote_file):
                        remote_source_file_pattern = True
                        print_msg = " #############################  " + str(
                            cntr_pattern ) + " Filename | %s | matching with remote control file pattern | %s| prepare fetching from remote #############################" % (
                                        remote_file_path_filename, remote_ctrlfile_regex_pattern)
                    else:
                        if remote_file_regex_pattern == '':
                            remote_source_file_pattern = True
                            print_msg = " #############################  " + str(
                                cntr_pattern) + " Filename | %s | matching with remote datafile pattern |  %s | prepare fetching from remote #############################" % (
                                            remote_file_path_filename, remote_file_regex_pattern,
                                        )
                    if remote_source_file_pattern:
                        logging.info(print_msg)
                        cntr_pattern = cntr_pattern + 1
                        cntr_pattern, get_success = self.sftp_get_file(remote_file_path_filename,
                                                                       local_file_path_filename,
                                                                       remote_file_size, cntr_pattern)
                        logging.info("Get the filesize of the file that is ftped to local")
                        self.sftp_file_size_check(remote_file_path_filename, local_file_path_filename, remote_file_size)

                    else:
                        print_msg = "Filename | %s | not matching with the remote datafile pattern |  %s | or remote control file pattern | %s" % (
                            remote_file, remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                        logging.info(print_msg)

            if get_success == 'success':
                print_msg = "SFTP process to local file path complete | " + local_file_path + "/" + remote_file
                logging.info(print_msg)
                self.write_incr_sftp_files(local_hdfs_sftp_temp_file_path, hdfs_sftp_filename, remote_file)
                if xenon_put_method == 'Y':
                    print_msg = "Calling | Xenon put process"
                    logging.info(print_msg)
                    json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon = self.put_file(
                            remote_file, local_file_path, dest_datefile_path, params, dest_ctrlfile_path,
                            local_ctrlfile_regex_pattern,partition_date_method)
            #else:
                #print_msg = "Filename | %s | not matching with the remote datafile pattern |  %s | or remote control file pattern | %s" % (
                    #remote_file, remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                #logging.info(print_msg)

        if hdfs_sftp_filename.strip() != '':
            if hdfs_sftp_filename.strip() != '':
                shutil.move(local_hdfs_sftp_temp_file_path + "/" + hdfs_sftp_filename,
                        local_hdfs_sftp_file_path + "/" + hdfs_sftp_filename)
            json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon = self.put_file(
                hdfs_sftp_filename, local_hdfs_sftp_file_path, hdfs_sftp_filepath, params={'overwrite': 'true'})

        if restart_status == 'N':
            if (remote_file_regex_pattern != '' or remote_ctrlfile_regex_pattern != '') and cntr_pattern == 0:
                self.errormsg_sftp = "No new file in remote file path | %s  | in remote node | %s | for file remote datafile pattern | %s | or file remote control file pattern | %s" % (
                    remote_file_path,self.host, remote_file_regex_pattern,remote_ctrlfile_regex_pattern)
                logging.error(self.errormsg_sftp)
                #logging.error(traceback.format_exc())
                raise FileNotFoundError(self.errormsg_sftp)
            #self.conn.close()
        if restart_status == 'Y':
            if (remote_file_regex_pattern != '' or remote_ctrlfile_regex_pattern != '') and cntr_pattern == 0:
                logging.info("No file in remote node | %s  | in the remote path | %s | for file remote datafile pattern | %s | or file remote control file pattern | %s" % (
                        self.host, remote_file_path, remote_file_regex_pattern,remote_ctrlfile_regex_pattern))
                exit(0)

        return self.json_string_list_valid_sftp, self.json_string_list_rejected_sftp, self.errormsg_sftp, self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon

    def upload_file(self, local_file_path, dest_datefile_path,restart,params={},
                    dest_ctrlfile_path='', local_datafile_regex_pattern='', local_ctrlfile_regex_pattern='',partition_date_method=''):

        logging.info("########################  Inside upload_file method ########################")
        #util.is_dir_exists(local_file_path)
        if len(dest_ctrlfile_path) > 0 and len(local_ctrlfile_regex_pattern) == 0:
            logging.error("Wrong status for parameter ,local ctrlfile_regex_pattern should not be empty, else make dest_path_ctrlfile empty")
            #logging.error(traceback.format_exc())
            raise Exception(
                "Wrong status for parameter ,local ctrlfile_regex_pattern should not be empty, else make dest_path_ctrlfile empty")
        if len(dest_ctrlfile_path) == 0 and len(local_ctrlfile_regex_pattern) > 0:
            logging.error("Wrong status for parameter dest_path_ctrlfile should not be empty,else make local ctrlfile_regex_pattern empty")
            #logging.error(traceback.format_exc())
            raise Exception(
                "Wrong status for parameter dest_path_ctrlfile should not be empty,else make local ctrlfile_regex_pattern empty")
        cntr_pattern = 0
        cntr_no_pattern = 0
        try:
            files = (file for file in os.listdir(local_file_path)
                 if os.path.isfile(os.path.join(local_file_path, file)) and not file.startswith('.'))
        except FileNotFoundError as e:
            self.errormsg_xenon = "FileNotFoundError({0}): {1}".format(e.errno, e.strerror)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_xenon)
            errormsg_xenon = self.exception(self.errormsg_xenon)
            raise e
            #Exception(self.errormsg_sftp)
            #return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, errormsg_xenon
        filelist = list(files)
        tot_files_local = len(filelist)
        restart_status = 'N'
        if not filelist:
            errormsg_sftp = 'No file in local node | local file path |  %s  ' % (
                 local_file_path)
            if restart == 'N':
                self.errormsg_sftp = errormsg_sftp
                logging.error(self.errormsg_sftp)
                #logging.error(traceback.format_exc())
                raise FileNotFoundError(self.errormsg_sftp)
            else:
                restart_status = 'N'
                logging.info(errormsg_sftp)
                return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon,restart_status
        print_msg = "Some files exists in the local path | %s  | program will check it based on params to be moved to hdfs| files | %s " % (
            local_file_path, filelist)
        logging.info(print_msg)
        logging.info("tot_files_local = " + str(filelist))

        if local_datafile_regex_pattern == '' and local_ctrlfile_regex_pattern == '':
            print_msg = "No local data file and control file pattern specified | fetching all files from local excluding files which are dir or starting with star or dot | %s | files | %s" % (
                local_file_path, filelist)
            restart_status = 'Y'
            logging.info(print_msg)
        else:
            if local_datafile_regex_pattern != '' or local_ctrlfile_regex_pattern != '':
                cntr = 0
                #print_msg = "############### list of files matching with data file pattern| %s | or control file pattern | %s ###############" % (
                    #local_datafile_regex_pattern, local_ctrlfile_regex_pattern)
                #logging.info(print_msg)
                #logging.info('tot_files_in_local_file_path' ' =  ' + str(filelist))
                for element in filelist:
                    local_file_pattern_match = False
                    local_file_pattern_match1 = False
                    if local_datafile_regex_pattern != '' and re.search(local_datafile_regex_pattern, element):
                        logging.info(re.search(local_datafile_regex_pattern, element))
                        local_file_pattern_match = True
                        print_msg = "Filename | %s | matched with local data file pattern | %s " % (
                            element, local_datafile_regex_pattern)
                    if not local_file_pattern_match and local_ctrlfile_regex_pattern != '' and re.search(local_ctrlfile_regex_pattern, element):
                        local_file_pattern_match = True
                        print_msg = "Filename | %s | matched with local control file pattern | %s" % (
                            element, local_ctrlfile_regex_pattern)
                    else:
                        if local_datafile_regex_pattern == '':
                            local_file_pattern_match1 = True
                            print_msg = "Filename | %s | matched with local data file pattern | %s " % (
                                element, local_datafile_regex_pattern)
                            logging.info(print_msg)
                    if local_file_pattern_match:
                        logging.info(print_msg)
                        restart_status = 'Y'
                        cntr = cntr + 1
                    else:
                        if local_file_pattern_match1 != True:
                            print_msg = "filename | %s | not matching with file pattern | %s | or control file pattern | %s" % (
                                element,local_datafile_regex_pattern, local_ctrlfile_regex_pattern)
                            logging.info(print_msg)
                if cntr == 0:
                    if local_datafile_regex_pattern != '' and  local_ctrlfile_regex_pattern != '':
                        self.errormsg_sftp = 'No file in | local file path |  %s | matching with local data file pattern | %s | or local control file pattern | %s ' % (
                            local_file_path, local_datafile_regex_pattern, local_ctrlfile_regex_pattern)
                    elif local_datafile_regex_pattern != '':
                        self.errormsg_sftp = 'No file in local file path |  %s | matching with local data file pattern | %s ' % (
                            local_file_path, local_datafile_regex_pattern)
                    else:
                        self.errormsg_sftp = 'No file in local file path |  %s | matching with local control file pattern | %s ' % (
                            local_file_path, local_ctrlfile_regex_pattern)
                    if restart == 'N':
                        #logging.error(traceback.format_exc())
                        raise FileNotFoundError(self.errormsg_sftp)
                    else:
                        restart_status = 'N'
                        self.errormsg_sftp = ''
                        return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon, restart_status

        for filelist_det in filelist:
            local_file_pattern_match = False
            local_file = filelist_det
            local_file_path_filename = local_file_path + "/" + local_file
            if local_datafile_regex_pattern == '' and local_ctrlfile_regex_pattern == '':
                print_msg = "############################# " + "No local data and control file pattern specified | prepare fetching from local filepath | %s | %s file | %s #############################" % (
                    local_file_path, str(cntr_no_pattern + 1), local_file)
                logging.info(print_msg)
                restart_status = 'Y'
                json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon = self.put_file(
                    local_file, local_file_path, dest_datefile_path, params, dest_ctrlfile_path,
                    local_ctrlfile_regex_pattern,partition_date_method)
            else:
                if local_datafile_regex_pattern != '' and re.search(local_datafile_regex_pattern, local_file):
                    local_file_pattern_match = True
                    print_msg = " #############################  " + str(
                        cntr_pattern + 1) + " Filename | %s | matching with local datafile pattern |  %s | prepare fetching from local #############################" % (
                                    local_file_path_filename, local_datafile_regex_pattern,
                                    )
                if not local_file_pattern_match and local_ctrlfile_regex_pattern != '' and re.search(
                        local_ctrlfile_regex_pattern, local_file):
                    local_file_pattern_match = True
                    print_msg = " #############################  " + str(
                        cntr_pattern + 1) + " Filename | %s | matching with local control file pattern | %s| prepare fetching from local  #############################" % (
                                    local_file_path_filename,local_ctrlfile_regex_pattern)
                else:
                    if local_datafile_regex_pattern == '':
                        local_file_pattern_match = True
                        print_msg = " #############################  " + str(
                            cntr_pattern + 1) + " Filename | %s | matching with local datafile pattern |  %s | prepare fetching from local  #############################" % (
                                        local_file_path_filename, local_datafile_regex_pattern,
                                    )
                if local_file_pattern_match:
                    logging.info(print_msg)
                    cntr_pattern = cntr_pattern + 1
                    restart_status = 'Y'
                    json_string_list_valid_xenon, json_string_list_rejected_xenon, errormsg_xenon = self.put_file(
                        local_file, local_file_path, dest_datefile_path, params, dest_ctrlfile_path,
                        local_ctrlfile_regex_pattern,partition_date_method)
                else:
                    print_msg = "Filename | %s | not matching with the local datafile pattern |  %s | or local control file pattern | %s" % (
                        local_file_path_filename, local_datafile_regex_pattern, local_ctrlfile_regex_pattern)
                    logging.info(print_msg)
        if (local_datafile_regex_pattern != '' or local_ctrlfile_regex_pattern != '') and cntr_pattern == 0:
            self.errormsg_sftp = "No file | in the local path | %s | matching with  local datafile pattern | %s | or local control file pattern | %s" % (
                local_file_path, local_datafile_regex_pattern, local_ctrlfile_regex_pattern)
            logging.error(self.errormsg_sftp)
            if restart == 'N':
                #logging.error(traceback.format_exc())
                raise FileNotFoundError(self.errormsg_sftp)
            else:
                restart_status = 'N'

        return self.json_string_list_valid_xenon, self.json_string_list_rejected_xenon, self.errormsg_xenon,restart_status

    @staticmethod
    def incr_sftp_files(xenon_url, xenon_user, xenon_password, hdfs_sftp_filepath, hdfs_sftp_filename, sftp_listattr,
                        sftp_list, local_file_path,local_temp_file_path,platform):
        if hdfs_sftp_filename.strip() != '':
            util.remove_file_local(local_file_path, hdfs_sftp_filename)
            hdfs_list = []
            diff_list_attr = []
            hdfs_sftp_filename_filepath = hdfs_sftp_filepath + "/" + hdfs_sftp_filename
            head, tail = os.path.split(hdfs_sftp_filename_filepath)
            local_sftp_filename = tail
            local_sftp_filepath = local_file_path + "/" + local_sftp_filename
            local_sftp_temp_filepath = local_temp_file_path + "/" + local_sftp_filename
            xenon_call = Xenon(xenon_url, xenon_user, xenon_password,platform=platform)
            get_response, errormsg_xenon = xenon_call.get_file(hdfs_sftp_filename_filepath)
            if errormsg_xenon != '':
                logging.error(
                    "No sftp file containing all the filenames that are sftped from the beginning in hdfs path | %s " % hdfs_sftp_filename_filepath)
                #logging.error(traceback.format_exc())
                raise
            file = pathlib.Path(local_sftp_filepath)
            if file.exists():
                logging.info(
                    "file containing all the filenames that are sftped from the beginning exist in the local file path | %s | which was not moved to hdfs | %s | in the prev run " % (
                    local_sftp_filepath, hdfs_sftp_filepath))
                hdfs_list = util.list_csv(local_sftp_filepath)
                diff_list = list(set(sftp_list) - set(hdfs_list))
                for sftp_listattr_det in sftp_listattr:
                    if sftp_listattr_det.filename not in hdfs_list:
                        diff_list_attr.append(sftp_listattr_det)
                logging.info("total list entires of files | %s | that will be checked before sftping the files" % len(
                    diff_list_attr))
            else:
                logging.info(
                    "file containing all the filenames that are sftped from the beginning does not exist in the local file path | %s | so will get it from the hdfs path | %s " % (
                        local_sftp_filepath, hdfs_sftp_filepath))
                with open(local_sftp_temp_filepath, 'w') as fp:
                    for line in get_response.split("\n"):
                        if line.strip() == '':
                            continue
                        hdfs_list.append(line)
                        fp.write(line + "\n")
                    diff_list = list(set(sftp_list) - set(hdfs_list))
                for sftp_listattr_det in sftp_listattr:
                    if sftp_listattr_det.filename not in hdfs_list:
                        diff_list_attr.append(sftp_listattr_det)
                logging.info("total list entires of files | %s | that will be checked before sftping the files" % len(
                    diff_list_attr))
        else:
            diff_list = sftp_list
            diff_list_attr = sftp_listattr
        logging.info("list of files that will be checked for the pattern %s" % diff_list)

        return diff_list_attr, diff_list

    @staticmethod
    def write_incr_sftp_files(local_file_path, hdfs_sftp_filename, new_filename):

        if hdfs_sftp_filename.strip() != '':
            filepath = local_file_path + "/" + hdfs_sftp_filename
            logging.info(
                "adding remote file name to the file path | %s which will be moved to hdfs file path | %s to indicate filenames that are sftped so that in next run these files are not sftped again " % (
                new_filename, hdfs_sftp_filename))
            with open(filepath, 'a') as fp:
                fp.write(new_filename + "\n")
                #fp.write(new_filename)


    def sftp_check(self,remote_file_path, local_file_path, remote_file_regex_pattern ='',remote_ctrlfile_regex_pattern='',
                   hdfs_sftp_filepath='', hdfs_sftp_filename='',local_hdfs_sftp_file_path='',local_hdfs_sftp_temp_file_path='',platform='',local_sftp_temp_file_path='',local_sftp_filename=''):
        logging.info("########################  Inside sftp_check method ########################")

        filelist_excluded_pattern = []
        try:
            filelist1 = self.conn.listdir_attr(remote_file_path)
            filelist_filename1 = self.conn.listdir(remote_file_path)
            filelist, filelist_filename = self.incr_sftp_files_check(self.url, self.user, self.password, hdfs_sftp_filepath,
                                                               hdfs_sftp_filename,
                                                               filelist1, filelist_filename1, local_hdfs_sftp_file_path,
                                                               local_hdfs_sftp_temp_file_path, platform)
        except Exception as e:
            self.errormsg_sftp = 'Could not get to remote filepath %s | Exited with error %s' % (remote_file_path, e)
            logging.error(traceback.format_exc())
            logging.error(self.errormsg_sftp)
            self.errormsg_sftp = self.exception(self.errormsg_sftp)
            raise Exception(self.errormsg_sftp)

        # filelist_filename = self.conn.listdir(remote_file_path)
        tot_files_remote = len(filelist_filename)
        files = (filelist_det.filename for filelist_det in filelist
                 if not stat.S_ISDIR(filelist_det.st_mode) and (
                             not filelist_det.filename.startswith('.') and not filelist_det.filename.startswith('*')))
        filelist_excluded = list(files)
        print(filelist_excluded)
        logging.info(local_sftp_temp_file_path)
        logging.info(local_sftp_filename)
        if filelist_excluded:
            if remote_file_regex_pattern == '' and remote_ctrlfile_regex_pattern == '':
                print_msg = "No remote data file and control file pattern specified | fetching all files from remote excluding files which are dir or starting with star or dot | %s | files | %s" % (
                    local_file_path, filelist_excluded)
                logging.info(print_msg)
                #add remove file
                logging.info("New files are available in the remote | %s| in remote filepath | %s | and new list of files are | %s" % (self.host,remote_file_path,filelist_excluded))
                util.remove_file_local(local_sftp_temp_file_path, local_sftp_filename)
            else:
                if remote_file_regex_pattern != '' or remote_ctrlfile_regex_pattern != '':
                    cntr = 0
                    print_msg = "############### list of remote files used to match with data file pattern | %s | or control file pattern | %s ###############" % (
                    remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                    logging.info(print_msg)
                    logging.info('tot_files_remote' ' =  ' + str(filelist_filename))

                    for element in filelist_filename:
                        remote_source_file_pattern = False
                        remote_source_file_pattern1 = False
                        if remote_file_regex_pattern != '' and re.search(remote_file_regex_pattern, element):
                            remote_source_file_pattern = True
                            print_msg = "Filename | %s | matched with remote data file pattern | %s " % (
                                element, remote_file_regex_pattern)
                            logging.info(print_msg)
                            filelist_excluded_pattern.append(element)
                        if remote_ctrlfile_regex_pattern != '' and not remote_source_file_pattern and re.search(
                                remote_ctrlfile_regex_pattern, element):
                            remote_source_file_pattern = True
                            print_msg = "Filename | %s | matched with remote control file pattern | %s" % (
                                element, remote_ctrlfile_regex_pattern)
                            logging.info(print_msg)
                            filelist_excluded_pattern.append(element)
                        else:
                            if remote_file_regex_pattern == '':
                                remote_source_file_pattern1 = True
                                print_msg = "Filename | %s | matched with remote data file pattern | %s " % (
                                    element, remote_file_regex_pattern)
                                logging.info(print_msg)
                                filelist_excluded_pattern.append(element)

                        if remote_source_file_pattern:
                            logging.info(print_msg)
                            cntr = cntr + 1
                        else:
                            if remote_source_file_pattern1 != True:
                                print_msg = "filename | %s | not matching with remote file pattern | %s | or remote control file pattern | %s" % (
                                    element, remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                                logging.info(print_msg)
                    if cntr == 0:
                        if remote_file_regex_pattern != '' and remote_ctrlfile_regex_pattern != '':
                            self.errormsg_sftp = 'No file in | remote file path |  %s | matching with remote data file pattern | %s | or remote control file pattern | %s ' % (
                                remote_file_path, remote_file_regex_pattern, remote_ctrlfile_regex_pattern)
                            logging.info(self.errormsg_sftp)
                        elif remote_file_regex_pattern != '':
                            self.errormsg_sftp = 'No file in remote file path |  %s | matching with remote data file pattern | %s ' % (
                                remote_file_path, remote_file_regex_pattern)
                            logging.info(self.errormsg_sftp)
                        else:
                            self.errormsg_sftp = 'No new file in remote file path |  %s | matching with remote control file pattern | %s ' % (
                                remote_file_path, remote_ctrlfile_regex_pattern)
                            logging.info(self.errormsg_sftp)
                    else:
                        logging.info(
                            "New files are available in the remote | %s| in remote filepath | %s | and new list of files are | %s" % (
                            self.host, remote_file_path, filelist_excluded_pattern))
                        util.remove_file_local(local_sftp_temp_file_path, local_sftp_filename)
        else:
            print_msg = 'No new file in the remote file path |  %s ' % (
                remote_file_path)
            logging.info(print_msg)


    @staticmethod
    def incr_sftp_files_check(xenon_url, xenon_user, xenon_password, hdfs_sftp_filepath, hdfs_sftp_filename, sftp_listattr,
                        sftp_list, local_file_path,local_temp_file_path,platform):
        if hdfs_sftp_filename.strip() != '':
            util.remove_file_local(local_file_path, hdfs_sftp_filename)
            hdfs_list = []
            diff_list_attr = []
            hdfs_sftp_filename_filepath = hdfs_sftp_filepath + "/" + hdfs_sftp_filename
            head, tail = os.path.split(hdfs_sftp_filename_filepath)
            local_sftp_filename = tail
            local_sftp_filepath = local_file_path + "/" + local_sftp_filename
            local_sftp_temp_filepath = local_temp_file_path + "/" + local_sftp_filename
            xenon_call = Xenon(xenon_url, xenon_user, xenon_password,platform=platform)
            get_response, errormsg_xenon = xenon_call.get_file(hdfs_sftp_filename_filepath)
            if errormsg_xenon != '':
                logging.error(
                    "No sftp file containing all the filenames that are sftped from the beginning in hdfs path | %s " % hdfs_sftp_filename_filepath)
                #logging.error(traceback.format_exc())
                raise
            file = pathlib.Path(local_sftp_filepath)
            if file.exists():
                logging.info(
                    "file containing all the filenames that are sftped from the beginning exist in the local file path | %s | which was not moved to hdfs | %s | in the prev run " % (
                    local_sftp_filepath, hdfs_sftp_filepath))
                hdfs_list = util.list_csv(local_sftp_filepath)
                diff_list = list(set(sftp_list) - set(hdfs_list))
                for sftp_listattr_det in sftp_listattr:
                    if sftp_listattr_det.filename not in hdfs_list:
                        diff_list_attr.append(sftp_listattr_det)
                logging.info("total list entires of files | %s | that will be checked before sftping the files" % len(
                    diff_list_attr))
            else:
                logging.info(
                    "file containing all the filenames that are sftped from the beginning does not exist in the local file path | %s | so will get it from the hdfs path | %s " % (
                        local_sftp_filepath, hdfs_sftp_filepath))

                for line in get_response.split("\n"):
                    if line.strip() == '':
                        continue
                    hdfs_list.append(line)

                diff_list = list(set(sftp_list) - set(hdfs_list))
                for sftp_listattr_det in sftp_listattr:
                    if sftp_listattr_det.filename not in hdfs_list:
                        diff_list_attr.append(sftp_listattr_det)
                logging.info("total list entires of files | %s | that will be checked before sftping the files" % len(
                    diff_list_attr))
        else:
            diff_list = sftp_list
            diff_list_attr = sftp_listattr
        logging.info("list of files that will be checked for the pattern %s" % diff_list)

        return diff_list_attr, diff_list