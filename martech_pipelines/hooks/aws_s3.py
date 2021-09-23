import gzip as gz
import re
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Tuple, Optional, Union
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from martech_pipelines.hooks.aws_base import AwsBaseHook
from martech_pipelines.utils.helpers import chunks


class S3Hook(AwsBaseHook):
    """
    Interact with AWS S3, using the boto3 library.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "s3"
        super().__init__(*args, **kwargs)

    @staticmethod
    def parse_s3_url(s3url: str) -> Tuple[str, str]:
        """
        Parses the S3 Url into a bucket name and key.
        :param s3url: The S3 Url to parse.
        :rtype s3url: str
        :return: the parsed bucket name and key
        :rtype: tuple of str
        """
        parsed_url = urlparse(s3url)

        if not parsed_url.netloc:
            raise Exception(f'Please provide a bucket_name instead of "{s3url}"')

        bucket_name = parsed_url.netloc
        key = parsed_url.path.strip("/")

        return bucket_name, key

    def check_for_prefix(
        self, prefix: str, delimiter: str, bucket_name: Optional[str] = None
    ) -> bool:
        """
        Checks that a prefix exists in a bucket
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :return: False if the prefix does not exist in the bucket and True if it does.
        :rtype: bool
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(fr"(\w+[{delimiter}])$", prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return prefix in plist

    def list_prefixes(
        self,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> list:
        """
        Lists prefixes in a bucket under prefix
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        :return: a list of matched prefixes
        :rtype: list
        """
        prefix = prefix or ""
        delimiter = delimiter or ""
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("list_objects_v2")
        response = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter=delimiter,
            PaginationConfig=config,
        )

        prefixes = []
        for page in response:
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    prefixes.append(common_prefix["Prefix"])

        return prefixes

    def list_keys(
        self,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> list:
        """
        Lists keys in a bucket under prefix and not containing delimiter
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        :return: a list of matched keys
        :rtype: list
        """
        prefix = prefix or ""
        delimiter = delimiter or ""
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("list_objects_v2")
        response = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter=delimiter,
            PaginationConfig=config,
        )

        keys = []
        for page in response:
            if "Contents" in page:
                for k in page["Contents"]:
                    keys.append(k["Key"])

        return keys

    def check_for_key(self, key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Checks if a key exists in a bucket
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: True if the key exists and False if not.
        :rtype: bool
        """
        try:
            self.get_conn().head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            else:
                raise e

    def get_key(self, key: str, bucket_name: Optional[str] = None):
        """
        Returns a boto3.s3.Object
        :param key: the path to the key
        :type key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: the key object from the bucket
        :rtype: boto3.s3.Object
        """
        obj = self.get_resource_type("s3").Object(bucket_name, key)
        obj.load()
        return obj

    def read_key(self, key: str, bucket_name: Optional[str] = None) -> str:
        """
        Reads a key from S3
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: the content of the key
        :rtype: str
        """
        obj = self.get_key(key, bucket_name)
        return obj.get()["Body"].read().decode("utf-8")

    def load_file(
        self,
        filename: Union[Path, str],
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = True,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Loads a local file to S3
        :param filename: path to the file to load.
        :type filename: Union[Path, str]
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        :param gzip: If True, the file will be compressed locally
        :type gzip: bool
        :param acl_policy: String specifying the canned ACL policy for the file being
            uploaded to the S3 bucket.
        :type acl_policy: str
        """
        filename = str(filename)
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = {}
        if encrypt:
            extra_args["ServerSideEncryption"] = "AES256"
        if gzip:
            with open(filename, "rb") as f_in:
                filename_gz = f_in.name + ".gz"
                with gz.open(filename_gz, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz
        if acl_policy:
            extra_args["ACL"] = acl_policy

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args)

    def download_file(
        self,
        key: str,
        bucket_name: Optional[str] = None,
        local_path: Optional[str] = None,
    ) -> str:
        """
        Downloads a file from the S3 location to the local file system.
        :param key: The key path in S3.
        :type key: str
        :param bucket_name: The specific bucket to use.
        :type bucket_name: Optional[str]
        :param local_path: The local path to the downloaded file. If no path is provided it will use the
            system's temporary directory.
        :type local_path: Optional[str]
        :return: the file name.
        :rtype: str
        """
        self.log.info(
            "Downloading source S3 file from Bucket %s with path %s", bucket_name, key
        )

        if not self.check_for_key(key, bucket_name):
            raise Exception(
                f"The source file in Bucket {bucket_name} with path {key} does not exist"
            )

        s3_obj = self.get_key(key, bucket_name)

        with NamedTemporaryFile(
            dir=local_path, prefix="airflow_tmp_", delete=False
        ) as local_tmp_file:
            s3_obj.download_fileobj(local_tmp_file)

        return local_tmp_file.name

    def delete_objects(self, bucket: str, keys: Union[str, list]) -> None:
        """
        Delete keys from the bucket.
        :param bucket: Name of the bucket in which you are going to delete object(s)
        :type bucket: str
        :param keys: The key(s) to delete from S3 bucket.
            When ``keys`` is a string, it's supposed to be the key name of
            the single object to delete.
            When ``keys`` is a list, it's supposed to be the list of the
            keys to delete.
        :type keys: str or list
        """
        if isinstance(keys, str):
            keys = [keys]

        s3 = self.get_conn()

        # We can only send a maximum of 1000 keys per request.
        # For details see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        for chunk in chunks(keys, chunk_size=1000):
            response = s3.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]}
            )
            deleted_keys = [x["Key"] for x in response.get("Deleted", [])]
            self.log.info("Deleted: %s", deleted_keys)
            if "Errors" in response:
                errors_keys = [x["Key"] for x in response.get("Errors", [])]
                raise Exception(f"Errors when deleting: {errors_keys}")

    def copy_object(
        self,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: Optional[str] = None,
        dest_bucket_name: Optional[str] = None,
        source_version_id: Optional[str] = None,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Creates a copy of an object that is already stored in S3.
        Note: the S3 connection used here needs to have access to both
        source and destination bucket/key.
        :param source_bucket_key: The key of the source object.
            It can be either full s3:// style url or relative path from root level.
            When it's specified as a full s3:// url, please omit source_bucket_name.
        :type source_bucket_key: str
        :param dest_bucket_key: The key of the object to copy to.
            The convention to specify `dest_bucket_key` is the same
            as `source_bucket_key`.
        :type dest_bucket_key: str
        :param source_bucket_name: Name of the S3 bucket where the source object is in.
            It should be omitted when `source_bucket_key` is provided as a full s3:// url.
        :type source_bucket_name: str
        :param dest_bucket_name: Name of the S3 bucket to where the object is copied.
            It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
        :type dest_bucket_name: str
        :param source_version_id: Version ID of the source object (OPTIONAL)
        :type source_version_id: str
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be copied which is private by default.
        :type acl_policy: str
        """
        acl_policy = acl_policy or "private"

        if dest_bucket_name is None:
            dest_bucket_name, dest_bucket_key = self.parse_s3_url(dest_bucket_key)
        else:
            parsed_url = urlparse(dest_bucket_key)
            if parsed_url.scheme != "" or parsed_url.netloc != "":
                raise Exception(
                    "If dest_bucket_name is provided, "
                    + "dest_bucket_key should be relative path "
                    + "from root level, rather than a full s3:// url"
                )

        if source_bucket_name is None:
            source_bucket_name, source_bucket_key = self.parse_s3_url(source_bucket_key)
        else:
            parsed_url = urlparse(source_bucket_key)
            if parsed_url.scheme != "" or parsed_url.netloc != "":
                raise Exception(
                    "If source_bucket_name is provided, "
                    + "source_bucket_key should be relative path "
                    + "from root level, rather than a full s3:// url"
                )

        copy_source = {
            "Bucket": source_bucket_name,
            "Key": source_bucket_key,
            "VersionId": source_version_id,
        }
        response = self.get_conn().copy_object(
            Bucket=dest_bucket_name,
            Key=dest_bucket_key,
            CopySource=copy_source,
            ACL=acl_policy,
        )
        return response

    def upload_df_to_s3(self, dataframe, bucket_name, key):
        with NamedTemporaryFile(mode="r+", suffix=".csv") as tmp_csv:
            dataframe.to_csv(tmp_csv.name, index=False, header=True, na_rep="")
            self.load_file(tmp_csv.name, key=key, bucket_name=bucket_name)

        if self.check_for_key(key, bucket_name):
            self.log.info(f"File uploaded successfully to S3 : {bucket_name}/{key}")
        else:
            self.log.warning(f"File not uploaded")
            raise FileNotFoundError

    @staticmethod
    def get_s3_uri(bucket_name, key) -> str:
        return "s3://" + bucket_name + "/" + key
