import datetime as dt

import pandas as pd
import yaml


def read_configuration(configuration_file_path):
    """
    This function reads the configuration from the given path (yaml file)

    Args:
        configuration_file_path ([str]): path to yaml configuration

    Returns:
        [dict]: yaml config. used in this pipeline script as a dict
    """
    with open(configuration_file_path) as file:
        configuration = yaml.full_load(file)
    return configuration


def remove_fileformats(any_list):
    """function to remove everything after a '.'

    Args:
        any_list ([list]): a list of elements, eg. filenames like xyz.parquet

    Returns:
        [list]: [xyz, xza, yza], elements without fileformat
    """
    output = [element.split(".")[1] for element in any_list]
    return output


def all_days(start_date, end_date, timeformat="%Y-%m-%d"):
    """function to generate a list of dates between start and enddate

    Args:
        start_date ([type]): format %Y-%m-%d, smaller than end_date
        end_date ([type]): format %Y-%m-%d, larger than start date
        timeformat (str, optional): Defaults to "%Y-%m-%d".

    Returns:
        [list]: of days
    """

    startdate = dt.datetime.strptime(start_date, timeformat).date()
    enddate = dt.datetime.strptime(end_date, timeformat).date()

    delta = enddate - startdate

    output = []

    for i in range(delta.days + 1):
        day = startdate + dt.timedelta(days=i)
        output.append(str(day))
    return output


def check_datetimes_in_datelist(list_datestamp, list_timestamps):
    """function that checks if a str(timestamp) with date and time is part
    of a date list

    Args:
        list_datestamp ([type]): list of dates, eg. ["2022-01-15", "2022-01-14"]
        list_timestamps ([type]): list of timestamps, eg. ["2022-01-14 12:01"]

    Returns:
        [type]: list of valid timestamps
    """

    output = []

    for timestamp in list_timestamps:
        if timestamp[:10] in list_datestamp:
            output.append(timestamp)

    return output


def get_missing_elements(list_of_all_elements, list_to_test):
    """function to look for elements in list_of_all_elements and are missing in list_to_test

    Args:
        list_of_all_elements ([type]): a complete list
        list_to_test ([type]): an incomplete list

    Returns:
        [type]: a list of missing values of list_of_all_elements
    """
    output = [
        element for element in list_of_all_elements if element not in list_to_test
    ]
    return output


def time_or_limit(str_time, lower_limit_time, timeformat):
    st_date = dt.datetime.strptime(str_time, timeformat)
    limit_date = dt.datetime.strptime(lower_limit_time)

    if st_date >= limit_date:
        return str(st_date.day())
    else:
        return str(limit_date.day())


def get_str_time_timedifference(str_time, daydifference, timeformat):
    str_date = dt.datetime.strptime(str_time, timeformat)
    day = str_date - dt.timedelta(days=daydifference)
    output = str(day.date())
    return output


def make_timegrid_df(
    start_date=None, end_date=None, timeformat="%Y-%m-%d %H:%M", grid="minutes", step=1
):
    st_date = dt.datetime.strptime(start_date, timeformat)
    e_date = dt.datetime.strptime(end_date, timeformat)
    delta = e_date - st_date

    output = []

    if grid == "minutes":
        for i in range(int((delta.total_seconds() / 60 + int(step)) / int(step))):
            minutes = st_date + dt.timedelta(seconds=i * 60 * step)
            output.append(dt.datetime.strftime(minutes, timeformat))

    if grid == "days":
        for i in range(int((delta.days / int(step) + int(step)) / int(step))):
            days = st_date + dt.timedelta(days=i * step)
            output.append(dt.datetime.strftime(days, timeformat))

    output = output[::-1]
    output = pd.DataFrame(output, columns=["datetime"])

    return output


def get_index_targets_not_na(data, list_of_targets):
    output = []

    if list_of_targets is not None:
        for i in list_of_targets:
            try:
                output = output + [j for j, x in enumerate(data[i].notnull()) if x]
            except KeyError:
                pass
    output = list(set(output))
    output.sort
    return output


all_days(start_date="2022-01-01", end_date="2022-01-15", timeformat="%Y-%m-%d")


make_timegrid_df(
    start_date="2022-01-1 09:55",
    end_date="2022-01-15 08:01",
    timeformat="%Y-%m-%d %H:%M",
    grid="minutes",
    step=1,
)
