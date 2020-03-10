
import pprint
import time as time_t
from datetime import datetime, timedelta, time

class Helpers():
    def round_to_nearest_release_time(data_ts, interval=4):
        """
        Round time to nearest 4/8/12 AM/PM (default)
        Or any other interval

        Args:
            data_ts (datetime)
            interval (Integer)

        Returns datetime
        """
        hour = data_ts.hour

        quotient = int(hour / interval)
        hour_of_release = (quotient + 1) * interval

        if hour_of_release < 24:
            date_time = datetime.combine(
                data_ts.date(), time((quotient + 1) * interval, 0))
        else:
            date_time = datetime.combine(data_ts.date() + timedelta(1), time(0, 0))

        return date_time


    def var_checker(var_name, var, have_spaces=False):
        """
        A function used to check variable value including
        title and indentation and spacing for faster checking
        and debugging.

        Args:
        var_name (String): the variable name or title you want display
        var (variable): variable (any type) to display
        have_spaces (Boolean): keep False is you dont need spacing for each display.
        """
        if have_spaces:
            print()
            print(f"===== {var_name} =====")
            printer = pprint.PrettyPrinter(indent=4)
            printer.pprint(var)
            print()
        else:
            print(f"===== {var_name} =====")
            printer = pprint.PrettyPrinter(indent=4)
            printer.pprint(var)

    def str_to_dt(string_value):
        return datetime.strptime(string_value, "%Y-%m-%d %H:%M:%S")
