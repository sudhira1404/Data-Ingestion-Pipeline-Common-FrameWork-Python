#! /usr/bin/env bash

#sh -x /home/corp.target.com/svmdedmp/file_ingestion_framework/install_ext_dep.sh "/home/corp.target.com/svmdedmp/file_ingestion_framework_test" "file_ingestion_venv" "/home/corp.target.com/svmdedmp/file_ingestion_framework_test/codebase-0.0.0"


vir_envi_dir=$1
vir_envi=$2
requirement_dir=$3


cd $vir_envi_dir
source ${vir_envi}/bin/activate
pip3 install -r ${requirement_dir}/requirements.txt --upgrade
deactivate
rm -f ${requirement_dir}/src/wrapper/file_ingest_wrapper_no_venv.sh
rm -f ${requirement_dir}/src/wrapper/file_ingest_wrapper_param_no_venv.sh
rm -f ${requirement_dir}/setup.py
rm -f ${requirement_dir}/setup.cfg
rm -f ${requirement_dir}/README.md
rm -f ${requirement_dir}/PKG-INFO
rm -f ${requirement_dir}/requirements.txt
rm -r ${requirement_dir}/src/codebase.egg-info