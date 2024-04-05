from setuptools import setup, find_packages
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

def read_requirements(fname):
    try:
        with open(os.path.join(os.path.dirname(__file__), fname)) as req:
            raw_requirements = req.read().splitlines()
    except:
        raw_requirements = []
    return raw_requirements


setup(
    name="codebase",
    ##version="0.0.2",
    author="sudhindra.ramakrishna",
    author_email="sudhindra.ramakrishna@target.com",
    description="file_ingestion_src_lnd",
    long_description="file_ingestion_src_lnd",
    long_description_content_type="text/markdown",
    url="https://git.target.com/data-engineering-mktg-data-foundation/file_ingestion_src_lnd",
    package_dir={"": "src"},
    #packages=find_packages(where="src",exclude=['ingest.test','ingest1'])),
    #packages = find_packages(where='src', exclude=['ingest1','*.dev-scripts.*', '*.dev-scripts','dev-scripts.*','dev-scripts','ingest.test', 'ingest.misc']),
    #packages = find_packages(where='src',exclude=['*.test.py']),
    packages = find_packages(where='src'),

    #package_data={

       #"": ["wrapper/*.sh"]

    #},

    data_files=[
    ("/home/corp.target.com/svmdedmp/file_ingestion_framework/config",['src/config/config_file_ingestion','src/config/config_file_ingestion_bigred','src/config/config_file_ingestion_stage','src/config/config_file_ingestion','src/config/config_file_ingestion_test']),
    ("/home/corp.target.com/svmdedmp/file_ingestion_framework/wrapper",['src/wrapper/file_ingest_wrapper_no_venv.sh','src/wrapper/file_ingest_wrapper_param_no_venv.sh'])

    ],
    #include_package_data=True,
    #exclude_package_data={"": ["test.py"]},
    license='MIT',
    install_requires=read_requirements('requirements.txt'),

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6'

)

