# PySpark Expectations

[<img src="https://opensourcelogos.aws.dmtech.cloud/dmTECH_opensource_logo.svg" height="21" width="130">](https://www.dmtech.de/)

PySpark Expectations helps to check the most often data quality failures using pyspark modules. 
The implementation with pyspark makes fast quality testing of big data possible.

PySpark Expectations are inspired by [Great Expectations](https://github.com/great-expectations/great_expectations).

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

1. First of all, you have to install Python and Pip on your computer.
* [Python 3 Installation & Setup Guide](https://realpython.com/installing-python/)
* [Pip Installation Guide](https://pip.pypa.io/en/stable/installing/)
2. The best practice is to run the project in a virtual environment. Next install it using [pipenv](https://pipenv-fork.readthedocs.io/en/latest/) or [virtualenv](https://virtualenv.pypa.io/en/latest/).
3. Finally, install all packages from "requiremens.txt" in the virtual environment. 
<br/> With pipenv: 
```
    $ pipenv install -r requirements.txt
```
<br/> Or in the environment created with virtualenv: 
```
    $ pip install -r requirements.txt
```

### Installing

A step by step series of examples that tell you how to get a development env running

1. Download the pyspark-expectations on your computer from github.
2. Open a terminal at the folder, where you've downloaded the pyspark-expectations and create a Source Distribution.

    ```
    $ python setup.py sdist
    ```

3. Install the created Distribution using pip in python enviroment, where you are going to use pyspark-expectations functions. After this step you can import pyspark-expectations package and use the functions from it.

    ```
    $ pip install dist/*.tar.gz 
    ```

4. You can also uninstall pyspark-expectations using pip.

    ```
    $ pip uninstall pyspark_expectations
    ```

## Usage

### Description

Pyspark-expectations contains functions, that checking the most common failures in big data. 
The given functions are applied on Pyspark Dataframe. Folowing checks are implemented:
* expect_table_row_count_to_be_between
* expect_column_values_to_be_unique
* expect_column_values_to_be_null
* expect_column_values_to_not_be_null
* expect_column_values_to_match_regex
* expect_column_values_not_to_match_regex
* expect_column_values_to_be_in_set
* expect_column_values_to_be_between

The names of the functions are describing the expected behavior. 

### Output

The output of the functions is dictionary with two values:
1. Value "success" can be true or false. The values is true, if condition is fulfilled. 
2. Value "summary" contains additional information about checking result.

### Example

In the folowing example will be checked, how many unique value contains Pyspark-dataframe

```
import pyspark_expectations
from pyspark.sql import SparkSession, SQLContext

spark = (
        SparkSession.builder.master("local")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


l = [["Patrik", 1.92], ["Tanja", 1.62], ["Tobi", None]] * 10
df = spark.createDataFrame(l, ["name", "height"])
result_from_unique_values_analyse = df.expect_column_values_to_be_unique("height")

print("""
    Percent of duplicate values : {duplicates_count_percent},
    Percent of distinct values : {distinct_count_percent},
    Total count : {total_count},
    Number of duplicate values : {duplicates_count},
    Number of distinct values : {distinct_count}
""".format(**result_from_unique_values_analyse["summary"]))

spark.stop()
```

Output after running:

```
Percent of duplicate values : 0.9,
Percent of distinct values : 0.1,
Total count : 30,
Number of duplicate values : 27,
Number of distinct values : 3
```

## Running the tests

The tests are implemented via pytest framework. 
To run the test execute such command at the terminal:
```
    $ pytest 
```
If you have any error issues, check the [$PYTHONPATH](https://bic-berkeley.github.io/psych-214-fall-2016/using_pythonpath.html) variable of the environment.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details