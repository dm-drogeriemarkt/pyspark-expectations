#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numbers
from pyspark_expectations.arguments_format_checker import * 
from pyspark.sql.functions import col

def _return_percent(count, total_count):
    # Check input:
    check_var_type(count, "count", numbers.Number)
    check_var_type(total_count, "total_count", numbers.Number)

    # Return percent of <count> in <total_count>
    if total_count == 0:
        return 0
    else:
        return float(count) / total_count


def _return_dict(success, summary):
    # Check input:
    check_var_type(success, "success", bool)
    check_var_type(summary, "summary", dict)

    # Return dict
    return {"success": success, "summary": summary}


def expect_table_row_count_to_be_between(df, min_value=None, max_value=None):
    """
    Checks if row count of table is between the provided parameters. 


    Parameters
    ---------------------------------------------------------------------------

        - df : Pyspark dataframe
            Pyspark dataframe value, which will be analysed. 

        - min_value : int
            Maximum possible number of rows in referred DataFrame value. 
            Standart value is None. None means infinity in this context.

        - max_value : str
            Minimum possible number of rows in referred DataFrame value. 
            Standart value is None. None means infinity in this context.



    Returns

    ---------------------------------------------------------------------------

        - summary: dictionary
        
            dictionary with summary description , which contains two fields :

            1) Field "success" contains true if the number of rows in dataframe is 
            between minimum and maximum value. Otherwise contains false.

            2) Field "summary" with this fields: 
                2.1) Field "row_count" , which contains number of rows in the Dataframe
                2.2) Field "message" , which describes the occured error or success.   
    """

    # Check input:
    check_min_max(min_value, max_value)

    # Check expecation:
    row_count = df.count()

    success = True
    if min_value is not None:
        if row_count < min_value:
            success = False
    if max_value is not None:
        if row_count > max_value:
            success = False

    return _return_dict(success=success, summary={"row_count": row_count})


def expect_column_values_to_be_unique(df, column_name, unexpected_percent=0):
    """
    Expect each column value to be unique.
    This expectation detects duplicates. All duplicated values are counted as exceptions.

    Parameters
    ---------------------------------------------------------------------------
        - df : PySpark DataFrame
            DataFrame value, that was created from 'table_name' table

        - column_name : str
            Name of the column in table 'table_name'.
            Values in this column must be unique.

        - unexpected_percent: float, optional
            Must be between 0 and 1 (inclusively). 
            Return “success” = True if at least unexpected_percent percent of values match the expectation.

    Returns
    ---------------------------------------------------------------------------

        -summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains , if dataframe doesn't contain duplicates.

        2) Field "summary" with additional information.


    """

    # Check input:
    check_column_name(column_name, df.columns)
    check_unexpected_percent(unexpected_percent)

    # Create values, which will be returned, if the condition is fulfilled. Otherwise the values will be changed later
    success = True
    total_count = df.count()
    df = df.select(column_name)
    distinct_count = df.distinct().count()
    duplicates_count = total_count - distinct_count

    distinct_count_percent = _return_percent(distinct_count, total_count)
    duplicates_count_percent = _return_percent(duplicates_count, total_count)

    # Check success
    if total_count != distinct_count:
        success = check_allowed_unexpected_percent(
            duplicates_count_percent, unexpected_percent
        )

    return _return_dict(
        success,
        {
            "total_count": total_count,
            "duplicates_count": duplicates_count,
            "duplicates_count_percent": duplicates_count_percent,
            "distinct_count": distinct_count,
            "distinct_count_percent": distinct_count_percent,
        },
    )




def expect_column_values_to_be_null(df, column_name, unexpected_percent=0):
    """Checks if a column in a PySpark DataFrame only consists of NULL values.

    Parameters:
    ---------------------------------------------------------------------------
        - df : Pyspark dataframe
            Pyspark dataframe value, which will be analysed. 

        - column_name : str
            Name of the column in the pyspark dataframe that will be checked. 

        - unexpected_percent: float , optional
            Must be between 0 and 1. 
            Return “success” True if at least unexpected_percent percent of values match the expectation. 

    Returns:
    ---------------------------------------------------------------------------
    
        -summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains true in two situations: 
            1. when "expect_not_null = True" and column does not contain null values.
            2. when "expect_not_null = False" and column contains just null values.
        Otherwise contains false.

        2) Field "summary" with additional information.

    """

    return _expect_column_values_to_be_null_or_not_null(
        df=df,
        column_name=column_name,
        unexpected_percent=unexpected_percent,
        expect_not_null=False,
    )


def expect_column_values_to_not_be_null(df, column_name, unexpected_percent=0):
    """ Checks if a column in a PySpark DataFrame does not contain any NULL values.

    Parameters:
    ---------------------------------------------------------------------------
    
        - df : Pyspark dataframe
            Pyspark dataframe value, which will be analysed. 

        - column_name : str
            Name of the column in the pyspark dataframe that will be checked. 
            
        - unexpected_percent: float , optional
            Must be between 0 and 1. 
            Return “success” True if at least unexpected_percent percent of values match the expectation. 

    Returns:
    ---------------------------------------------------------------------------
    
        -summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains true in two situations: 
            1. when "expect_not_null = True" and column does not contain null values.
            2. when "expect_not_null = False" and column contains just null values.
        Otherwise contains false.

        2) Field "summary" with additional information.

    """

    return _expect_column_values_to_be_null_or_not_null(
        df=df,
        column_name=column_name,
        unexpected_percent=unexpected_percent,
        expect_not_null=True,
    )

def _expect_column_values_to_be_null_or_not_null(
    df, column_name, unexpected_percent=0, expect_not_null=True
):

    # Check input:
    check_column_name(column_name, df.columns)
    check_unexpected_percent(unexpected_percent)
    check_var_type(expect_not_null, "expect_not_null", bool)

    # Create values, which will be returned, if the condition is fulfilled. Otherwise the values will be changed later
    total_count = df.count()
    null_count = df.filter(df[column_name].isNull()).count()
    not_null_count = total_count - null_count
    null_count_percent = _return_percent(null_count, total_count)
    not_null_count_percent = _return_percent(not_null_count, total_count)

    # Check success:
    unexpected_list = []
    if expect_not_null:
        success = check_allowed_unexpected_percent(
            null_count_percent, unexpected_percent
        )
        if null_count > 0:
            unexpected_list = [None]
    else:
        success = check_allowed_unexpected_percent(
            not_null_count_percent, unexpected_percent
        )
        if not_null_count > 0:
            df_not_nulls = (
                df.filter(df[column_name].isNotNull())
                .select(df[column_name])
                .distinct()
                .limit(20)
                .collect()
            )
            unexpected_list = [row[column_name] for row in df_not_nulls]

    return _return_dict(
        success=success,
        summary={
            "row_count": total_count,
            "null_count": null_count,
            "not_null_count": not_null_count,
            "null_count_percent": null_count_percent,
            "not_null_count_percent": not_null_count_percent,
            "unexpected_list": unexpected_list,
        },
    )


def expect_column_values_to_match_regex(df, column_name, regex, unexpected_percent=0):

    """
    Expect the column entries to be strings that can be matched to either any of 
    or all of a list of regular expressions.
       
    Parameters
    ---------------------------------------------------------------------------
        - df : PySpark Dataframe
            DataFrame value, that will be checked.

        - column_name : str
            Name of the column in the given dataframe.
            Values in this column must match to given regular expression.

        - regex : str, list[str]
            Regular expression(s) to test for. Values in the column <column_name> must 
            match any regular expresion from string or list.

        - unexpected_percent: float, optional 
            Must be between 0 and 1. 
            Return “success”: True if at least unexpected_percent percent of values 
            match the expectation.
        
    Returns
    ---------------------------------------------------------------------------

        -summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains , if all values in the column match regular expresions.
        Otherwise contains false.

        2) Field "summary" with additional information.

         
    """

    return _expect_column_values_to_match_or_not_to_match_regex(
        df=df,
        column_name=column_name,
        regex=regex,
        unexpected_percent=unexpected_percent,
        expect_to_match=True,
    )


def expect_column_values_not_to_match_regex(
    df, column_name, regex, unexpected_percent=0
):

    """
    Expect the column entries to be strings that can not be matched to either any of or all of a list of regular expressions.
       
    Parameters
    ---------------------------------------------------------------------------
        - df : PySpark Dataframe
            DataFrame value, that will be checked.

        - column_name : str
            Name of the column in the given dataframe.
            Values in this column are expected not match to given regular expression.

        - regex : str, list[str]
            Regular expression(s) to test for. Values in the column <column_name> must not
            match any regular expresion from string or list.

        - unexpected_percent: float, optional 
            Must be between 0 and 1. 
            Return “success”: True if at least unexpected_percent percent of values match the expectation.
        
    Returns
    ---------------------------------------------------------------------------

        -summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains , if all values in the column not match regular expresions.
        Otherwise contains false.

        2) Field "summary" with additional information.

         
    """

    return _expect_column_values_to_match_or_not_to_match_regex(
        df=df,
        column_name=column_name,
        regex=regex,
        unexpected_percent=unexpected_percent,
        expect_to_match=False,
    )
    
def _expect_column_values_to_match_or_not_to_match_regex(
    df, column_name, regex, unexpected_percent=0, expect_to_match=True
):

    # Check input:

    check_column_name(column_name, df.columns)
    check_unexpected_percent(unexpected_percent)
    check_var_type(regex, "regex", (list, tuple, str))
    if isinstance(regex, str):
        regex_list = [regex]
    else:
        regex_list = regex
    if len(regex_list) == 0:
        raise ValueError("Regex can not be empty ")
    for regex in regex_list:
        check_var_type(regex, "regex", str)

    # Create values, which will be returned, if the condition is fulfilled. Otherwise the values will be changed later
    success = True
    df = df.select(column_name)
    total_count = df.count()

    df_not_match_regex = df
    for regex in regex_list:
        # Create df_not_match_regex
        df_not_match_regex = df_not_match_regex.where(~df[column_name].rlike(regex))

        # Create df_match_regex
        if regex == regex_list[0]:
            df_match_regex = df.where(df[column_name].rlike(regex))
        else:
            df_match_regex = df_match_regex.union(
                df.where(df[column_name].rlike(regex))
            )

    values_not_match_regex = [
        row[column_name] for row in df_not_match_regex.limit(20).collect()
    ]
    values_match_regex = [
        row[column_name] for row in df_match_regex.limit(20).collect()
    ]

    not_match_regex_count = df_not_match_regex.count()
    match_regex_count = total_count - not_match_regex_count

    not_match_regex_count_percent = _return_percent(not_match_regex_count, total_count)
    match_regex_count_percent = _return_percent(match_regex_count, total_count)

    # Check success
    if expect_to_match:
        if not_match_regex_count != 0:
            success = check_allowed_unexpected_percent(
                not_match_regex_count_percent, unexpected_percent
            )
    else:
        if match_regex_count != 0:
            success = check_allowed_unexpected_percent(
                match_regex_count_percent, unexpected_percent
            )

    return _return_dict(
        success,
        {
            "total_count": total_count,
            "not_match_regex_count": not_match_regex_count,
            "match_regex_count": match_regex_count,
            "match_regex_count_percent": match_regex_count_percent,
            "not_match_regex_count_percent": not_match_regex_count_percent,
            "values_not_match_regex": values_not_match_regex,
            "values_match_regex": values_match_regex,
        },
    )

def expect_column_values_to_be_in_set(df, column_name, value_set, unexpected_percent = 0):
    """
    Expect each column value to be in a given set.

       Parameters
    ---------------------------------------------------------------------------
        - df : PySpark DataFrame
            DataFrame value, that was created from 'table_name' table
        - column_name : str
            Name of the column in table 'table_name'.
            Values in this column must be in the set 'value_set'.
        - value_set : set, list, tuple
            Set of Values that allowed to be in column 'column_name'
        - unexpected_percent: float 
            Must be between 0 and 1. 
            Return “success” = True if at least unexpected_percent percent of values match the expectation. 
            Otherwise "success" field in result dictionary contains false.

    Returns
    ---------------------------------------------------------------------------
        -summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains , if all values in the column are in set.
        Otherwise contains false..

        2) Field "summary" with additional information.
    """

    # Check input:
    check_column_name (column_name, df.columns)
    check_unexpected_percent (unexpected_percent)
    check_var_type(value_set,"value_set", (list, tuple, set))


    # Create values, which will be returned, if the condition is fulfilled. Otherwise the values will be changed later    
    df = df.select(column_name)
    total_count = df.count()
    
    success = True

    df_filter_not_in_set = df.filter(~df[column_name].isin(value_set))

    not_in_set_count = df_filter_not_in_set.count()
    in_set_count = total_count - not_in_set_count

    in_set_count_percent = _return_percent (in_set_count, total_count)
    not_in_set_count_percent = _return_percent (not_in_set_count, total_count)
    
    # Check success
    values_not_in_set = []
    if not_in_set_count != 0:
        values_not_in_set = [row[column_name] for row in df_filter_not_in_set.distinct().limit(20).collect()]
        success = check_allowed_unexpected_percent(not_in_set_count_percent,unexpected_percent)
        
    
    return _return_dict(success,
                        {
                            "total_count": total_count,
                            "not_in_set_count": not_in_set_count,
                            "not_in_set_count_percent": not_in_set_count_percent,
                            "in_set_count": in_set_count,
                            "in_set_count_percent": in_set_count_percent,
                            "values_not_in_set": values_not_in_set
                        })

def expect_column_values_to_be_between(df,column_name, min_value = None, max_value = None, unexpected_percent = 0, ignore_null = False):
    """
    Expect column entries to be between a minimum value and a maximum value (inclusive).
    Return dictionary with summary of expectation. 
      
    Parameters
    ---------------------------------------------------------------------------
        - df : pyspark dataframe
            DataFrame that will be checked.

        - column_name : str
            Name of the column in table 'table_name'.
            Values in this column must be in the set 'value_set'.

        - min_value : int, optional
            Maximum possible value of rows in referred DataFrame. 
            Standart value is None. None means minus infinity in this context.

        - max_value : str, optional
            Minimum possible value of rows in referred DataFrame value. 
            Standart value is None. None means plus infinity in this context.

        - unexpected_percent: float, optional 
            Must be between 0 and 1. 
            Return “success”: True if at least unexpected_percent percent of values match the expectation.
        
        - ignore_null: bool
            Indicates if null values allowed to be between the checked minimum value and a maximum value.
            True : allowed, False: not allowed.
        
    Returns
    ---------------------------------------------------------------------------

        - summary: dict

        Return dictionary with summary description , which contains two fields :

        1) Field "success" contains true if all( or unexpected_percent all ) values in the column are between min_value and max_value.
        Otherwise contains false.

        2) Field "summary" with additional information.

        """
    # Check input:
    check_column_name (column_name, df.columns)
    check_unexpected_percent (unexpected_percent)
    check_min_max(min_value, max_value)

    # Create values, which will be returned, if the condition is fulfilled. Otherwise the values will be changed later
    success = True
    total_count = df.count()

    df_not_between = df.filter(~df[column_name].between(min_value,max_value)) 
    # By default, null values was filtered from the df before "between" command is executed.
    # It means that in df_not_between there is no null values. 
    # User is expecting null values to be in df_not_between, when ignore_null == False.
    if not ignore_null: 
        df_with_nulls = df.filter(df[column_name].isNull())
        df_not_between = df_not_between.union(df_with_nulls)

    not_between_count = df_not_between.count()
    between_count = total_count - not_between_count
    
    between_count_percent = _return_percent (between_count, total_count)
    not_between_count_percent = _return_percent (not_between_count, total_count)

    # Check success
    not_between_list = []
    if not_between_count != 0 :
        not_between_list = [row[column_name] for row in df_not_between.select(col(column_name)).distinct().limit(20).collect()]
        success = check_allowed_unexpected_percent(not_between_count_percent,unexpected_percent)

    min_in_df = df.agg({column_name: "min"}).collect()[0][0]
    max_in_df = df.agg({column_name: "max"}).collect()[0][0]
    return _return_dict(success,
                        {
                        "total_count": total_count,
                        "not_between_count": not_between_count,
                        "between_count": between_count,
                        "not_between_count_percent": not_between_count_percent,
                        "between_count_percent": between_count_percent,
                        "not_between_list": not_between_list,
                        "min_in_df": min_in_df,
                        "max_in_df" : max_in_df
                        })


