#! /usr/bin/env bash

#for sftp from remote and then upload to hdfs
#sh -x /home/corp.target.com/svmdedmp/file_ingestion/src/ingest/wrapper/file_ingest_wrapper.sh NSA sftp_upload.py config_file_ingestion file_ingest_wrapper_param.sh local y
#for only upload to hdfs
#sh -x /home/corp.target.com/svmdedmp/file_ingestion/src/ingest/wrapper/file_ingest_wrapper.sh NSA_upload upload.py config_file_ingestion file_ingest_wrapper_param.sh local y
#for only td s3 upload to hdfs
#sh -x /home/corp.target.com/svmdedmp/file_ingestion/src/ingest/wrapper/file_ingest_wrapper.sh td_s3_upload td_s3_upload.py config_td_s3 file_ingest_wrapper_param.sh local y

jobname=$1
pythonwrapper=$2
config_file=$3
shellparam_file=$4
env=$5
touch_file_f=$6
shellparam_dir=$(dirname "$0")

#set -eux
if [[ $# -ne 6 ]]; then
    echo "Illegal number of parameters"
    exit 1
fi

. $shellparam_dir"/"$shellparam_file

if [[ $env == 'prod' ]]
then
shellparam_dir=$shellparam_dir
pythonconfig_dir=$pythonconfig_dir
pythonwrapper_scripts_dir=$pythonwrapper_scripts_dir
vir_envi_dir=$vir_envi_dir
log_dir=$log_dir
pythonpath=$pythonpath
mail_group=$mail_group
mail_group=$mail_group
nuid=$nuid
touch_filepath=$touch_filepath
cd $vir_envi_dir
source file_ingestion_venv/bin/activate
set +x
source ~/.bashrc
set -x
else
shellparam_dir=$shellparam_dir_local
pythonconfig_dir=$pythonconfig_dir_local
pythonwrapper_scripts_dir=$pythonwrapper_scripts_dir_local
vir_envi_dir=$vir_envi_dir_local
log_dir=$log_dir_local
pythonpath=$pythonpath_local
mail_group=$mail_group_local
mail_group=$mail_group_local
nuid=$nuid_local
touch_filepath=$touch_filepath_local
cd $vir_envi_dir
source venv/bin/activate
fi

config_filepath=$pythonconfig_dir"/"$config_file
date_log=`date "+%m-%d-%Y_%H:%M:%S"`
logfile=$log_dir/$jobname$date_log.log
attachment=$logfile
mkdir -p $touch_filepath/"$jobname"
touch_filepath=$touch_filepath"/"$jobname"/"$jobname".txt"
touch_file_crte_f=''

mkdir -p $log_dir

if [[ $pythonwrapper != 'sftp_check.py' ]]
then
    if [[  -f "$touch_filepath" ]] && [[ "$touch_file_f" != '' ]]
    then
        echo "touch file $touch_filepath found meaning the prev process is complete and new process is yet to start.The touch file needs to be deleted to kickstart the new load.Skipping the run"
        exit 0
    fi
fi

set +x
#source /home/corp.target.com/${nuid}/.bashrc
#source ~/.bashrc

set -x

#cd $vir_envi_dir
#source file_ingestion_env/bin/activate
#source venv/bin/activate
export PYTHONPATH=$pythonpath
cd $pythonwrapper_scripts_dir

#if running as normal script uncomment below and also make sure to chnage the param file for pythonpath
python $pythonwrapper -c $jobname  -d $config_filepath -e $env > $logfile && exit_status=`echo 0` || exit_status=`echo 1`

#python $pythonwrapper -c $jobname  -d $config_filepath -e $env > $logfile 2>&1 && exit_status=`echo 0` || exit_status=`echo 1`
#python $pythonwrapper -c $jobname  -d $config_filepath -e $env &> $logfile  && exit_status=`echo 0` || exit_status=`echo 1`
#pythonwrapper_noext=$(echo "$pythonwrapper" | cut -f 1 -d '.')
#python -m $pythonwrapper_noext -c $jobname  -d $config_filepath -e $env > $logfile && exit_status=`echo 0` || exit_status=`echo 1`

echo "file ingestion script executed with exit status=${exit_status} and logs are located in ${log_dir}/${jobname}"

send_mail(){

attachment=${1}
jobname=$2
mail_group=${3}
subject="'${4}'"
#echo "hi" | mailx -a $attachment  -s "Vmaas :- $jobname Job Failed " ${mail_group}
mailx -a $attachment  -s "$subject" $mail_group < $log_dir/body_$jobname$date_log.log
#mailx  -s "$subject" $mail_group < $log_dir/body_$jobname$date_log.log
}

file_ingestion(){
cat  $logfile | grep 'restart status = N' || [[ $? == 1 ]]
output=$?
error_message=`grep 'ERROR\|Exception'  $logfile  2>/dev/null | tail -1`
echo $exit_status
if [[ $pythonwrapper = 'sftp_upload.py' ]]
then
sftp_restart=`echo -e '******Status of data files moved to hdfs for any left over files from prev runs ******';sed -n '/hdfs by restart/,/sftp/xenon not moved/p' $logfile 2>/dev/null || true;echo -e`
sftp=`echo -e '******Status of files sfted and moved to hdfs******';sed -n '/Valid files sftped/,/not moved to hdfs/p' $logfile 2>/dev/null || true;echo -e`
xenon=''
btch_sftp=`echo -e '******Status of batch exec file for files sfted ******';sed -n '/Valid btch file for sftp/,/Error message for btch file for sftp/p' $logfile 2>/dev/null || true;echo -e`
btch_xenon=`echo -e '******Status of batch exec file for files moved to hdfs ******';sed -n '/Valid btch file for xenon/,/Error message for btch file for xenon/p' $logfile 2>/dev/null || true;`
btch_xenon_restart=`echo -e '******Status of batch exec files moved to hdfs for any left over files from prev runs ******';sed -n '/Valid btch file for sftp/xenon/,/Error message for btch file for sftp/xenon/p' $logfile 2>/dev/null || true;echo -e`
#btch_xenon_restart=''
if [ $exit_status -eq 1 ]
then
cat <<EOT >> $log_dir/body_$jobname$date_log.log
    Jobname - $jobname
    Vmaas :- $jobname Job Failed
    Error Message - $error_message
    $sftp_restart
    $sftp
    $btch_sftp
    $btch_xenon
    $btch_xenon_restart
    $xenon
EOT
subject="mdflx2001 Vmaas:-$jobname Job Failed"
send_mail $attachment  $jobname $mail_group "'$subject'"
rm -f $log_dir/body_$jobname$date_log.log $logfile
exit 1
else
cat <<EOT >> $log_dir/body_$jobname$date_log.log
    Jobname - $jobname
    Vmaas :- $jobname Job Success
    $sftp_restart
    $sftp
    $btch_sftp
    $btch_xenon
    $btch_xenon_restart
    $xenon

EOT
subject="mdflx2001 Vmaas:-$jobname Job Success"
if [[ $touch_file_f != '' ]]
then
touch "$touch_filepath"
fi
send_mail $attachment  $jobname $mail_group "'$subject'"
rm -f $log_dir/body_$jobname$date_log.log $logfile
fi

else
sftp=''
xenon=`echo -e '******Status of files moved to hdfs ******';sed -n '/ Valid files moved to hdfs/,/Error message for rejected files/p' $logfile 2>/dev/null || true;echo -e`
btch_sftp=''
btch_xenon=`echo -e '******Status of batch exec file for files moved to hdfs ******';sed -n '/Valid btch file for xenon/,/Error message for btch file for xenon/p' $logfile 2>/dev/null || true;`
btch_xenon_restart=`echo -e '******Status of batch exec files moved to hdfs for any left over files from prev runs ******';sed -n '/Valid btch file for sftp/xenon/,/Error message for btch file for sftp/xenon/p' $logfile 2>/dev/null || true;echo -e`
sftp_restart=''
if [ $exit_status -eq 1 ]
then
cat <<EOT >> $log_dir/body_$jobname$date_log.log
    Jobname - $jobname
    Vmaas :- $jobname Job Failed
    Error Message - $error_message
    $xenon
    $btch_xenon
    $btch_xenon_restart
    $btch_sftp
    $sftp_restart
EOT
subject="mdflx2001 Vmaas:-$jobname Job Failed"
send_mail $attachment  $jobname $mail_group "'$subject'"
rm -f $log_dir/body_$jobname$date_log.log $logfile
exit 1
else
cat <<EOT >> $log_dir/body_$jobname$date_log.log
    Jobname - $jobname
    Vmaas :- $jobname Job Success
    $xenon
    $btch_xenon
    $btch_xenon_restart
    $btch_sftp
    $sftp_restart

EOT
subject="mdflx2001 Vmaas:-$jobname Job Success"
send_mail $attachment  $jobname $mail_group "'$subject'"
rm -f $log_dir/body_$jobname$date_log.log $logfile
fi
fi

}

td_s3(){
if [[ $pythonwrapper = 'td_s3_upload.py' || $pythonwrapper = 'td_s3_upload_recovery.py' ]]
then
td_s3_increment=`echo -e '******Status of files moved to hdfs ******';sed -n '/list of prefix fetched/,/list of s3files rejected/p' $logfile 2>/dev/null || true;echo -e`

else
td_s3_increment=''
fi

error_message=`grep 'ERROR\|Exception'  $logfile  2>/dev/null | tail -1`
echo $exit_status

if [ $exit_status -eq 1 ]
then
cat <<EOT >> $log_dir/body_$jobname$date_log.log
    Jobname - $jobname
    Vmaas :- $jobname Job Failed
    Error Message - $error_message
    $td_s3_increment
EOT
subject="mdflx2001 Vmaas:-$jobname Job Failed"
send_mail $attachment  $jobname $mail_group "'$subject'"
exit 1
else
cat <<EOT >> $log_dir/body_$jobname$date_log.log
    Jobname - $jobname
    Vmaas :- $jobname Job Success
    $td_s3_increment
EOT
subject="mdflx2001 Vmaas:-$jobname Job Success"
send_mail $attachment  $jobname $mail_group "'$subject'"
fi

}

if [[ $pythonwrapper = 'sftp_upload.py' || $pythonwrapper = 'upload.py' ]]
then
file_ingestion
fi
if [[ $pythonwrapper = 'td_s3_upload.py' || $pythonwrapper = 'td_s3_upload_recovery.py' ]]
then
td_s3
fi


rm -f $log_dir/body_$jobname$date_log.log $logfile

#exit_status=`python File_Ingestion_testcase1_1.py  2>$1`
#$(python File_Ingestion_testcase1_1.py 2>&1 >/dev/null)
#python File_Ingestion_testcase1_1.py
#stdout=`python File_Ingestion_testcase1_1.py`
#python File_Ingestion_testcase1_1.py > test1.log && echo exit_status = 0 || echo exit_status = 1
#python test1.py > $logfile && echo exit_status = 0 || echo exit_status = 1
##echo "hi" | mail -s "Vmaas :- $jobname Job Failed " sudhindra.ramakrishna@target.com
##echo "something" | mail -s "Vmaas :- $jobname Job Failed " sudhindra.ramakrishna@target.com

