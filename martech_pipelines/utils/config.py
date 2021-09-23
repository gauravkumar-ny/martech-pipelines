import os


def get_env():
    if os.environ.get("ENV") == "prod":
        return "prod"
    elif os.environ.get("ENV") == "preprod":
        return "preprod"
    else:
        return "dev"


def get_constants():
    if os.environ.get("ENV") == "prod":
        return "prod"
    else:
        return "preprod"


def get_secret_name():
    if get_env() == "prod":
        return "martech"
    else:
        return "martech_dev"


def get_db():
    if get_env() == "prod":
        return "martech"
    else:
        return "martech_dev"