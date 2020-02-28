import numbers

def check_var_type(var, var_name, types_list):
    if isinstance(types_list, type):
        types_list = [types_list]
    if not isinstance(var, tuple(types_list)):
        raise ValueError("<{0}> only accepts {1}".format(var_name, types_list))


def check_min_max(min_value, max_value):
    check_var_type(min_value, "min_value", (numbers.Number, type(None)))
    check_var_type(max_value, "max_value", (numbers.Number, type(None)))
    if min_value is None and max_value is None:
        raise ValueError("max_value and min_value can not be None simultaneously")
    if min_value is not None and max_value is not None and max_value < min_value:
        raise ValueError("max_value must be bigger then min_value")


def check_unexpected_percent(
    unexpected_percent, unexpected_percent_name="unexpected_percent"
):
    check_var_type(unexpected_percent, unexpected_percent_name, numbers.Number)
    if unexpected_percent > 1 or unexpected_percent < 0:
        raise ValueError(
            "<{0}> must be a float between 0 and 1".format(unexpected_percent_name)
        )


def check_column_name(column_name, columns):
    check_var_type(columns, "columns", (list, set, tuple))
    check_var_type(column_name, "column_name", str)
    if column_name not in columns:
        raise ValueError(
            "The given dataframe has not column with the name {column_name}. The columns in the dataframe are {columns}".format(
                columns=columns, column_name=column_name
            )
        )


def check_allowed_unexpected_percent(unexpected_percent, allowed_unexpected_percent):
    check_unexpected_percent(unexpected_percent)
    check_unexpected_percent(allowed_unexpected_percent, "allowed_unexpected_percent")
    if unexpected_percent <= allowed_unexpected_percent:
        return True
    else:
        return False

