import datetime as dt
import dateutil.tz

IST = dateutil.tz.gettz("Asia/Kolkata")


def yesterday():
    return (dt.datetime.now(IST) - dt.timedelta(days=1)).strftime("%Y-%m-%d")


def current_date():
    return dt.datetime.now(IST).strftime("%Y-%m-%d")


def current_hour():
    return dt.datetime.now(IST).strftime("%H")


def current_min():
    return dt.datetime.now(IST).strftime("%M")


def istnow():
    return dt.datetime.now(IST)


def utcnow():
    return dt.datetime.now()


def format_string(data):
    if isinstance(data, str):
        return data.format(
            yesterday=yesterday(),
            today=current_date(),
            hour=current_hour(),
            min=current_min(),
        )
    else:
        return data
