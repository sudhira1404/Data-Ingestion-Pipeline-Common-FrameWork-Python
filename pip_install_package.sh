#! /usr/bin/env bash


code_base_dir=$1
code_base_name='codebase-0.0.0.tar.gz'

#cd /home/corp.target.com/svmdedmp/file_ingestion_framework_test

cd ${code_base_dir}

pip3 install --user ${code_base_dir}/${code_base_name}  --upgrade
chmod -R 700 *
[[ ! -f ${code_base_dir}/codebase_archive/curr_${code_base_name} ]] || mv ${code_base_dir}/codebase_archive/curr_${code_base_name} ${code_base_dir}/codebase_archive/prev_${code_base_name}
[[ ! -f ${code_base_dir}/${code_base_name} ]] || mv ${code_base_dir}/${code_base_name} ${code_base_dir}/codebase_archive/curr_${code_base_name}

