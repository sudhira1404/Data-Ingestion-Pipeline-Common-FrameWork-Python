The scripts are executed on vmaas server mdflx2001.

job is triggered like below:

sh -x /home/corp.target.com/svmdedmp/file_ingestion_framework/codebase-0.0.0/src/wrapper/file_ingest_wrapper.sh groupm_bigred_digitalmmo sftp_upload.py config_file_ingestion_bigred file_ingest_wrapper_param.sh prod y

where parameters are
jobname=$1
pythonwrapper=$2
config_file=$3
shellparam_file=$4
env=$5
touch_file_f=$6

shell script `file_ingest_wrapper.sh` reads the `shellparam_file` and  activate the python virtual environment and triggers the `pythonwrapper`.
Once complete,sends out a email with job execution history

Based on `jobname` ,config file entry is searched in `config_file` and all parameters are read and passed to `pythonwrapper`

`pythonwrapper` fetches file from remote and sftp to vmaas and then moves to HDFS location.We could specify partitioning details in `config_file` and
entry(logic how do you want to derive the partitioning ,like reading from filename the date and add that date as partitioned date to hdfs file path or may be adding current date)
should be added in python file partition_date.py for that `jobname(check partition_date.py for examples)

There is logic to handle if sftp is success and xenon push fails then in next run job picks those first from vmaas for previous failed sftp runs and moves to hdfs.

Regex file pattern can be passed as param in `config_file` to search in remote.Based on patterns ,source files could be routed to different destination in HDFS
(check partition_date.py and config_file_ingestion_bigred for examples)

Batch exec stat is created and maintained in hdfs for each run,configured in `config_file` 

If remote is just adding files without deleting the previous,`config file` has parameters like(hdfs_sftp_filepath ,hdfs_sftp_filename,local_hdfs_sftp_file_path and local_hdfs_sftp_temp_file_path)
which keeps track of files already sftp'ed and moved to hdfs and next time it will ignore those files from remote.Check `config_file_ingestion_bigred` for examples

Remote could be configured as key based(default) or with password based on parameter `remote_passwordauth` in `config_file`.
Entry should be made in enterprise secret if it is password based

Destination can be bigred2/3

`touch_file_f` with `y` indicates  if done file is present, then next time when the flow is triggered, process will not execute the script but exits with success.
Once the done file is removed then only entire flow is executed.This to take care of scenario where we get source file like say every month between 6th and 10th.
We could schedule this job daily and FW could be schedule every month starting 6th which removes the touch file and main process executes the entire flow and
creates touch file once done.

sftp_check.py(File watcher):

This script checks if new file is available in remote or not and executed like below

sh -x /home/corp.target.com/svmdedmp/file_ingestion_framework/codebase-0.0.0/src/wrapper/file_ingest_wrapper.sh groupm_bigred_digitalmmo sftp_check.py config_file_ingestion_bigred file_ingest_wrapper_param.sh prod ''

upload.py(File watcher):

This script will just upload file to HDFS with no sftp

sh -x /home/corp.target.com/svmdedmp/file_ingestion_framework/codebase-0.0.0/src/wrapper/file_ingest_wrapper.sh groupm_bigred_digitalmmo upload.py config_file_ingestion_bigred file_ingest_wrapper_param.sh prod ''
