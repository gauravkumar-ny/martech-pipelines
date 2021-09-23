import json
import logging
from typing import Dict, Optional, Sequence, List, Any
from dateutil.parser import parse
import datetime

log = logging.getLogger(__name__)


def identity_field_check(data: Dict, identity_field="customer_id") -> Any:
    """
    Find the identity field in data dictionary using the identity_field names provided.
    In case of multiple identity fields, the function return first value it finds.
    :param data: Dictionary object containing data
    :param identity_field: identity field/ key name to look for
    """
    if isinstance(identity_field, str):
        if data.get(identity_field):
            return data.get(identity_field)
        else:
            log.debug(f"Identity field not found for : {data}")
            return None
    elif isinstance(identity_field, Sequence):
        for field in identity_field:
            if data.get(field):
                return data.get(field)
        log.debug(f"Identity field not found for : {data}")
    return None


def extract_attributes(data: Dict, attributes: List[str]) -> Dict:
    """
    Extract specific keys from dictionary (if present) based on list of attributes provided
    :param data: Dictionary from which attributes to be extracted
    :param attributes: List of keys which need to be extracted if present in data
    :return: Dict
    """
    result = dict((k, data[k]) for k in attributes if k in data)
    return result


def swap_key_name(data: Dict, swap_map: Dict) -> Dict:
    """
    Swaps the keys name in dictionary according to swap map dictionary
    :param data: data dictionary
    :param swap_map: swap key dict (old_key_name : new_key_name)
    :return: Dict
    """
    for key in swap_map.keys():
        if key in data:
            data[swap_map[key]] = data[key]
            data.pop(key, None)
    return data


def data_type_transformation(data: Dict, type_map: Dict) -> Dict:
    for key in type_map.keys():
        if key in data:
            if type_map[key] == "string":
                data[key] = str(data[key])
            elif type_map[key] == "int":
                try:
                    data[key] = int(data[key]) if data[key] is not None else None
                except ValueError as e:
                    log.warning(f"Conversion failed. Returning same value, {data[key]}")
            elif type_map[key] == "float":
                try:
                    data[key] = float(data[key]) if data[key] is not None else None
                except ValueError as e:
                    log.warning(f"Conversion failed. Returning same value, {data[key]}")
            elif type_map[key] == "date":
                if key == "dob":
                    data[key] = fix_dob(data[key])
                else:
                    try:
                        datetime = parse(data[key])
                        data[key] = datetime.strftime("%Y-%m-%d")
                    except Exception as e:
                        print(e)
                        log.warning(
                            f"Conversion failed. Returning same value, {data[key]}"
                        )
            elif type_map[key] == "mobile_sanity":
                data[key] = mobile_sanity(data[key])
            elif type_map[key] == "modify_reward":
                data[key] = modify_reward(data[key])
            elif type_map[key] == "Unix_epoch":
                if data.get(key, None):
                    value = unix_epoch(data[key])
                    if value is not None:
                        data[key] = value

    return data


def mobile_sanity(mobile):
    try:
        mobile = str(int(mobile)) if mobile is not None else None
    except ValueError:
        return None

    result = None
    try:
        mobile = mobile[-10:]
        if len(mobile) == 10:
            result = mobile
    except Exception as e:
        print("Error in mobile sanity : ", e)

    return result


def modify_reward(reward):
    try:
        reward = int(reward * 100)
    except Exception as e:
        print("Error : ", e)
        return None
    if isinstance(reward, int):
        return reward
    return None


def fix_dob(value):
    if value:
        val = str(value)
        if val.endswith("BC"):
            val = val[:-3]
        dob_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"]

        for dob_format in dob_formats:
            result = None
            try:
                result = datetime.datetime.strptime(val, dob_format)
            except Exception as e:
                result = None
            if result:
                if result.year < 1900:
                    result = result.replace(year=1952)
                return result.strftime("%Y-%m-%d")
    return None


def unix_epoch(value):
    try:
        dob = datetime.datetime.strptime(value, "%Y-%m-%d")
        seconds_since_epoch = int(dob.timestamp())
        dob = "$D_" + str(seconds_since_epoch)
        return dob
    except:
        data = value.pop("dob", None)
        log.warning(f"Invalid DOB, {data}")
        return None


def get_birth_date(dob):
    dob = fix_dob(dob)
    if not dob:
        return None
    date = datetime.datetime.strptime(dob, "%Y-%m-%d")
    birth_date = datetime.datetime.strftime(date, "%b-%d")
    return birth_date


def get_gender_implicit_data(data: Dict, parameters: Dict) -> Dict:
    signal_col_data = data.get(parameters.get("data_col", "v1__json"))
    signal_data = (
        json.loads(signal_col_data).get(parameters.get("signal_field", "last7days"))
        if signal_col_data is not None
        else {}
    )
    profile_data = dict()
    profile_data["gender_men_dp"] = int(signal_data.get("men", 0))
    profile_data["gender_women_dp"] = int(signal_data.get("women", 0))
    profile_data["gender_kids_dp"] = int(signal_data.get("kids", 0))
    profile_data["gender_home_dp"] = int(signal_data.get("home", 0))
    return profile_data
