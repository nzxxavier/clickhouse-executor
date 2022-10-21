from datetime import datetime, timedelta


def get_time(time_format, unit=None, delta=None):
    if unit is not None and delta is not None:
        if unit == "DAY":
            time_delta = timedelta(days=delta)
        elif unit == "WEEK":
            time_delta = timedelta(weeks=delta)
        elif unit == "HOUR":
            time_delta = timedelta(hours=delta)
        elif unit == "MINUTE":
            time_delta = timedelta(minutes=delta)
        elif unit == "SECOND":
            time_delta = timedelta(seconds=delta)
        else:
            time_delta = timedelta(seconds=0)
        base_time = datetime.now() + time_delta
    else:
        base_time = datetime.now()
    if time_format == "bizdate":
        return base_time.strftime("%Y%m%d")
    else:
        return base_time.strftime(time_format)
