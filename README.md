# Covid report

## Description

This is a project to show the COVID-19 reports of some european countries. The data is fetched from an externalAPI,
transformed and loaded into a Power BI report. This project was developedby [JulianBSL](https://github.com/TehWeifu) for
Sistemas Big Data class at CIPFP Mistala.

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
4. Run both setup scripts:
    - Generate directory structure: `python utils/setup.py`
    - Generate sample country and city data: `python utils/country&CityGenerator.py`

## Running the project

To run the project, run the `main.py` file. It has two execution modes:

- Single date: pass a single date (YYYYMMDD) as argument. Example: `python main.py 20220101`
- Range of dates: pass a start date and an end date (YYYYMMDD) as arguments. Example: `python main.py 20220101 20220131`

The script acts as an orchestrator and will run the following steps:

1. Fetch the data from the API
2. Localize the data, generating data for each city
3. Transform the data
4. Load the data into an historical parquet file

After running the script, if the data must be concatenated in a single file (to be loaded into Power BI), run
the `python utils/ConcatenateData.py` script.
