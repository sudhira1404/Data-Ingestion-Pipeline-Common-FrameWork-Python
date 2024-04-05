#! /usr/bin/env bash

source venv/bin/activate
#pip freeze > requirements.txt
pip freeze | grep -v 'pyspark' > requirements.txt