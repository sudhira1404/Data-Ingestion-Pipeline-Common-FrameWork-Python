import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from contextlib import redirect_stderr
import sys
#from utility import logger
from ingest.utility import logger
import paramiko
import time
import errno
import re
import hvac


#import pathlib


def sendmail(me, you, subject, message_body):
    """
    subject = 'hi'
    message_body = 'Workflow Name - facebook-insight_owned_tgtbrand_acct_agg \n \
                    Oozie ID 0180293-200219143129479-oozie-oozi-W FAILED.\n \
                    Error node : landing_add_partition \n \
                    Error Message - [UncheckedExecutionException: java.lang.RuntimeException: Unable to instantiate org.apache.hive.hcatalog.common.HiveClientCache$CacheableHiveMetaStoreClient]'

    me = "sudhindra.ramakrishna@target.com"
    you = "sudhindra.ramakrishna@target.com,piyush.jindal@target.com"

    sendmail(me,you,subject,message_body)

    """
    msg = MIMEMultipart()
    msg['From'] = me
    msg['To'] = you
    msg['Subject'] = subject
    message_body = message_body
    msg.attach(MIMEText(message_body))
    s = smtplib.SMTP()
    s.connect('localhost')
    # s.connect('https://outlook.office365.com/EWS/Exchange.asmx',443)
    s.sendmail(me, you, msg.as_string())
    s.quit()


def loggings(logfilename):
    with open(logfilename, 'w') as f:
        redirect_stderr(f)
    logging.basicConfig(level=logging.INFO, filename=logfilename, filemode="a",
                        format='%(name)s - %(levelname)s - %(message)s')


def logs(logfilename):
    sys.stderr = logger.Loggers(logfilename)
    # sys.stdout = logger.Loggers(log_filename1, log_filepath)
    log = logger.Loggers(logfilename)
    log.loggings()
    # sys.stderr.loggings()
    # sys.stdout.loggings()


def logclose():
    sys.stderr.logfile.close()
    sys.stderr = sys.stderr.terminal


"""
#Get access token from vault using svmdedmp ,key in its password:
​
client_token=`curl https://prod.vault.target.com/v1/auth/ldap/login/svmdedmp -X POST --data '{"password":""}' | jq -r .auth.client_token`
​
#the token valid for 30 mins​
​
#access the vault using the token
curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"''
or
curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"'' | jq -r .data.password
​
to list:

curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"'' -X LIST
​
#Store UserName and password using the below  command:

curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"'' -H "Content-Type: application/json" -X POST --data '{"password":""}'

#hvac_pwv_read('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svmdedmp_nuid')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svmdfghe_nuid','')
#pwv("svmdedmp","",'mdf_file_ingester/svmdedmp_nuid')

#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svmdfghe_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svmdfghe_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svhkyanp_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svhkyans_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svhkyhds_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svhkyhdp_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svaffdmp_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svaffdmd_nuid','')
#hvac_pwv_write('svmdedmp','','https://prod.vault.target.com','secret/mdf_file_ingester/svsgnhds_nuid','')

"""


def hvac_pwv_read(user_id,password,vault_addr,vault_password_path,secrets_key=[]):
    import os

    import urllib.request
    import tempfile
    bundleFile = os.path.join(tempfile.gettempdir(), "tgt-ca-bundle.crt")
    urllib.request.urlretrieve("http://browserconfig.target.com/tgt-certs/tgt-ca-bundle.crt", bundleFile)
    os.environ['REQUESTS_CA_BUNDLE'] = bundleFile
    uid = user_id
    pwd = password
    VAULT_ADDR=vault_addr
    vault = hvac.Client(url=VAULT_ADDR,verify=False)
    vault.auth.ldap.login(username=uid, password=pwd)
    retrieved_secrets = {}
    try:
        dict_of_secrets_data = vault.read(vault_password_path)
        dict_of_secrets = dict_of_secrets_data["data"]
    except Exception:
        return retrieved_secrets
    #return dict_of_secrets["data"]
    if not secrets_key:
        return dict_of_secrets
    try:
        for secrets_key1 in secrets_key:
            data = {secrets_key1: dict_of_secrets[secrets_key1]}
            retrieved_secrets.update(data)
    except KeyError as error:
        logging.info(str(error))
        logging.error("Exiting with error %s while updating key %s" % (error,data))
    return retrieved_secrets


def hvac_pwv_write(user_id, password,vault_addr,vault_password_path,secrets):
    import os

    import urllib.request
    import tempfile
    bundleFile = os.path.join(tempfile.gettempdir(), "tgt-ca-bundle.crt")
    urllib.request.urlretrieve("http://browserconfig.target.com/tgt-certs/tgt-ca-bundle.crt", bundleFile)
    os.environ['REQUESTS_CA_BUNDLE'] = bundleFile
    uid = user_id
    pwd = password
    VAULT_ADDR = vault_addr
    vault = hvac.Client(url=VAULT_ADDR, verify=False)
    vault.auth.ldap.login(username=uid, password=pwd)
    logging.info("Client authenticated = [%s].  Writing personal secrets in the path %s." % (vault.is_authenticated(),vault_password_path))
    """
    write_password_escaped = write_password.translate(str.maketrans({'"': r'\"',
                                                                     "\\": r"\\",
                                                                     "/": r"\/"}))
    vault.write(vault_password_path, password=write_password_escaped)
    mySecrets = vault.read(vault_password_path)
    return mySecrets["data"]['password']
    """
    derived_secrets = secrets
    for key, value in derived_secrets.items():
        value.translate(str.maketrans({'"': r'\"',
                                         "\\": r"\\",
                                         "/": r"\/"}))
        derived_secrets[key] = value
    tot_secrets = hvac_pwv_read(user_id,password,vault_addr,vault_password_path)
    if tot_secrets:
        tot_secrets.update(derived_secrets)
    else:
        tot_secrets = secrets
    vault.write(vault_password_path, **tot_secrets)
    #list_of_secrets = vault.read(vault_password_path)
    #print(list_of_secrets["data"])

def pwv_curl_read(user_id, password, vault_password_path):
    import requests
    user_id = user_id
    password = password
    vault_password_path = vault_password_path
    token_url = f'https://prod.vault.target.com/v1/auth/ldap/login/{user_id}'
    data = {
        "password": password
    }

    token_response = requests.post(token_url, json=data, verify=False)
    client_token = token_response.json()["auth"]["client_token"]
    secret_url = f'https://prod.vault.target.com/v1/secret/{vault_password_path}'
    header = {
        "X-Vault-Token": client_token,
        "Content-Type": "application/json"
    }
    secret_response = requests.get(secret_url, headers=header, verify=False)
    return secret_response.json()["data"]["password"]


def ssh_connect(edge_node, edge_node_user, edge_node_pwd):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=edge_node, username=edge_node_user, password=edge_node_pwd)
    except paramiko.ssh_exception.SSHException:
        raise paramiko.ssh_exception.SSHException("SSH Connection failed")
    return ssh


def myexec(ssh, cmd):
    try:
        logging.info("***Inside the myexec function***")
        start = time.time()
        stdin, stdout, stderr = ssh.exec_command(cmd)
        err_list = [line for line in stderr.read().decode('utf-8').splitlines()]
        out_list = [line for line in stdout.read().decode('utf-8').splitlines()]
        print("stdout=" + str(out_list))
        print("stderr=" + str(err_list))
        logging.info("stdout=" + str(out_list))
        logging.info("stderr=" + str(err_list))
        end = time.time()
    except Exception as error:
        print(str(error))
        logging.error("Exiting with error " + str(error))
        sys.exit(1)
    exit_status = stdout.channel.recv_exit_status()
    elapsed1 = end - start
    elapsed = str(elapsed1)
    stdout.close()
    stderr.close()
    if exit_status > 0:
        print("Error executing the command " + cmd + " with exit_status=" + str(
            exit_status) + " Elapsed time = " + elapsed)
        logging.error("Error executing the command " + cmd + " with exit_status=" + str(
            exit_status) + " Elapsed time = " + elapsed)
        ssh.close()
        exit(exit_status)
    else:
        # print("Completed executing the command " + cmd + " with exit_status=" + str(exit_status))
        print(
            "Completed executing the command " + " with exit_status=" + str(exit_status) + " Elapsed time = " + elapsed)
        logging.info("Completed executing the command " + cmd + " with exit_status=" + str(
            exit_status) + " Elapsed time = " + elapsed)
        #ssh.close()
    return out_list, err_list


def hive_extract_sql(sql, ssh):
    logging.info("***Inside the hive_extract function***")
    command = 'hive -e  "' + sql + '"'
    output, error = myexec(ssh, command)
    return output, error


def hive_extract_sql_beeline(sql, ssh):
    logging.info("***Inside the hive_extract function***")
    command = 'env HADOOP_CLIENT_OPTS="-Ddisable.quoting.for.sv=true" hive --disableQuotingForSV=true --silent=true --outputformat=tsv -e  "' + sql + '"'

    output, error = myexec(ssh, command)

    return output, error


def hive_extract_sql_file(name, sql, extn, ssh):
    print("***Inside the hive_extract function***")
    logging.info("***Inside the hive_extract function***")
    command = 'env HADOOP_CLIENT_OPTS="-Ddisable.quoting.for.sv=true" hive --disableQuotingForSV=true --silent=true --outputformat=tsv -e   "' + sql + '"' + ">" + name + "." + extn
    output, error = myexec(ssh, command)

    return output, error


def remove_file_ssh(name, extn, ssh):
    print("deleting the local temp files")
    filename = name + "." + extn
    command = "rm -f" + " " + filename
    myexec(ssh, command)

def remove_file_filepath_ssh(filename, filepath, ssh):
    print("deleting the local temp files")
    filename = filepath + "/" + filename
    command = "rm -f" + " " + filename
    myexec(ssh, command)


def remove_file_local(local_file_path, local_source_file):
    logging.info("Entering remove file method")
    local_path_file = local_file_path + "/" + local_source_file
    # print(local_path_file)
    if os.path.exists(local_path_file):
        os.remove(local_path_file)
        logging.info("Removed file " + local_source_file + " from local file path | " + local_path_file)
    else:
        logging.info("No file to remove " + local_source_file + " from local file path | " + local_path_file)


def remove_file_local_csv(name):
    data_filename = name + "." + "csv"
    if os.path.exists(data_filename):
        os.remove(data_filename)


def add_partition(schema, tablename, partition, edge_node, edge_node_user, edge_node_pwd):
    ssh = ssh_connect(edge_node, edge_node_user, edge_node_pwd)
    sql = "alter table %s.%s add IF NOT EXISTS partition(%s)" % (schema, tablename, partition)
    output, error = hive_extract_sql(sql, ssh)
    ssh.close()
    return output, error


def drop_partition(schema, tablename, partition, edge_node, edge_node_user, edge_node_pwd):
    ssh = ssh_connect(edge_node, edge_node_user, edge_node_pwd)
    sql = "alter table %s.%s drop IF EXISTS partition(%s)" % (schema, tablename, partition)
    output, error = hive_extract_sql(sql, ssh)
    ssh.close()
    return output, error


def is_dir_exists(path):
    try:
        os.makedirs(path)
        logging.info(path)
    except OSError as e:
        logging.info(str(e.errno) + path)
        if e.errno != errno.EEXIST:
            raise


def is_file_exists(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise


def remove_file_local_filepath(local_file_path):
    # local_path_file = local_file_path + "/" + local_source_file
    if os.path.exists(local_file_path):
        os.remove(local_file_path)
        logging.info("Removed file from local file path | " + local_file_path)


def add_partition_ssh(ssh, schema, tablename, partition):
    sql = "alter table %s.%s add IF NOT EXISTS partition(%s)" % (schema, tablename, partition)
    output, error = hive_extract_sql(sql, ssh)
    #ssh.close()
    return output, error


def drop_partition_ssh(ssh, schema, tablename, partition):
    sql = "alter table %s.%s drop IF EXISTS partition(%s)" % (schema, tablename, partition)
    output, error = hive_extract_sql(sql, ssh)
    #ssh.close()
    return output, error


def get_partition_cols(partition_path):
    # partition_cols = (",".join(partition_path.replace(path, '').split("/")))[1:]
    # partition_cols_quotes = partition_cols.replace('=', "='")
    # partition = re.sub(r",","',",partition_cols_quotes)+"'"
    partition_cols_list = partition_path.split("/")
    partition_cols_quotes_list = [i.replace('=', "='") for i in partition_cols_list if '=' in i]
    partition_cols_quotes_str = (",".join(partition_cols_quotes_list))
    partition = re.sub(r",", "',", partition_cols_quotes_str) + "'"

    return partition


def sendmail_ssh(name, subj, sql, ssh):
    print("***Inside the sendmail function***")
    logging.info("***Inside the sendmail function***")
    command = "sh " + name + ".sh " + " " + subj + " " + sql
    msg = "Executing command " + command
    output, error = myexec(ssh, command)
    #ssh.close()
    return output, error


def hive_external_local(file_name, edgenode_path, ssh, tablename):
    # if running local then uncomment sftp
    sftp_client = ssh.open_sftp()
    sftp_client.put(file_name, file_name)
    edgenode_filepath = edgenode_path + "/" + file_name
    cmd = "LOAD DATA LOCAL INPATH  " + "'" + edgenode_filepath + "'" + "OVERWRITE INTO TABLE %s;" % (tablename)
    #cmd1 = "drop table %s;CREATE external TABLE %s(filename string) row format delimited fields terminated by '|' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';" + cmd % (
    #tablename, tablename)
    cmd2 = "drop table %s;CREATE external TABLE %s(filename string) row format delimited fields terminated by '|' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';" % (
        tablename, tablename)
    cmd1 = cmd2 + cmd
    output, error = hive_extract_sql(cmd1, ssh)
    remove_file_filepath_ssh(file_name, edgenode_path, ssh)
    #remove_file_local(edgenode_path, file_name)
    #ssh.close()
    return output, error

def hive_external(file_name, edgenode_path, ssh, tablename):

    edgenode_filepath = edgenode_path + "/" + file_name
    cmd = "LOAD DATA LOCAL INPATH  " + "'" + edgenode_filepath + "'" + "OVERWRITE INTO TABLE %s;" % (tablename)
    # cmd1 = "drop table %s;CREATE external TABLE %s(filename string) row format delimited fields terminated by '|' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';" + cmd % (
    # tablename, tablename)
    cmd2 = "drop table %s;CREATE external TABLE %s(filename string) row format delimited fields terminated by '|' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';" % (
        tablename, tablename)
    cmd1 = cmd2 + cmd
    output, error = hive_extract_sql(cmd1, ssh)
    #remove_file_local(edgenode_path, file_name)
    #ssh.close()
    return output, error

def sendmail_html(me,you,message_body_filename,attach_filename,subject,format,cmd='',edge_node='', edge_node_user='', edge_node_pwd=''):

    #you1 = "sudhindra.ramakrishna@target.com,mdf@target.com"
    #you1 = ["sudhindra.ramakrishna@target.com","mdf@target.com"]
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = you
    # Create the body of the message (a plain-text and an HTML version).
    text = "Hi!\nHow are you?\nHere is the link you wanted:\nhttp://www.python.org"
    html = open(message_body_filename).read()
    # Record the MIME types of both parts - text/plain and text/html.
    #part1 = MIMEText(html1, 'html')
    part2 = MIMEText(html, format)
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    #msg.attach(part1)
    msg.attach(part2)
    if format == 'html':
        ssh = ssh_connect(edge_node, edge_node_user, edge_node_pwd)
        myexec(ssh, cmd)
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(open(attach_filename, 'rb').read())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(attach_filename))
        msg.attach(attachment)
    # Send the message via local SMTP server.
    s = smtplib.SMTP()
    s.connect('localhost')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    s.sendmail(me, you, msg.as_string())
    s.quit()


def list_csv(filepath):
    hdfs_list=[]
    with open(filepath) as fp:
        line = fp.readline()
        cnt = 1
        while line:
            print("Line {}: {}".format(cnt, line.strip()))
            hdfs_list.append(line.rstrip('\n'))
            line = fp.readline()
            cnt += 1
    return hdfs_list

def strip_blankline(filepath):
    with open(filepath,"r") as f:
        lines = f.readlines()

    with open(filepath,"w") as f:
        [f.write(line) for line in lines if line.strip()]