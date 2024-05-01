# Covid report

## Description

This is a simple project to show the COVID-19 report of some european countries. The data is fetched from an external
API, transformed and loaded into a Power BI report. This project was developed
by [JulianBSL](https://github.com/TehWeifu) for Sistemas Big Data class at CIPFP Mistala.

## Requirements

This project was developed using Python 3.11.5 and needs the libraries listed in the `requirements.txt` file.
It is also needed to set the following environment variables:

- PYSPARK_PYTHON=python
- HADOOP_HOME="path/to/hadoop"

## Installation & Setup

In order to run this project, follow the steps below:

1. Install python 3.11.5
2. (optional) Create a virtual environment
3. Install the required libraries by running `pip install -r requirements.txt` and setting the environment variables
4. Run both setup scripts: `python utils/setup.py` and `python utils/country&CityGenerator.py`

## Running the project

To run the project, run the `main.py` file. This script acts as an orchestrator and will run the following steps:

1. Fetch the data from the API
2. Localize the data, generating data for each city
3. Transform the data
4. Load the data into an historical parquet file

After running the script, if the data must be concatenated in a single file (to be loaded into Power BI), run
the `python utils/ConcatenateData.py` script.
