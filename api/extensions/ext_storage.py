import logging
from collections.abc import Callable, Generator
from typing import Literal, Union, overload
from functools import wraps


from flask import Flask
from prometheus_client import Counter, Histogram

from configs import dify_config
from dify_app import DifyApp
from extensions.storage.base_storage import BaseStorage
from extensions.storage.storage_type import StorageType

logger = logging.getLogger(__name__)

storage_request_latency = Histogram(
    name="storage_request_latency",
    documentation="The latency of storage requests",
    unit="seconds",
    labelnames=["method", "provider"],
)

storage_request_total_counter = Counter(
    name="storage_request_total_counter",
    documentation="The total count of storage requests",
    labelnames=["method", "provider"],
)

storage_request_failed_counter = Counter(
    name="storage_request_failed_counter",
    documentation="The failed count of storage requests",
    labelnames=["method", "provider"],
)


def timeit(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        with storage_request_latency.labels(method=func.__name__, provider=dify_config.STORAGE_TYPE).time():
            storage_request_total_counter.labels(method=func.__name__, provider=dify_config.STORAGE_TYPE).inc()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                storage_request_failed_counter.labels(method=func.__name__, provider=dify_config.STORAGE_TYPE).inc()
                raise e

    return decorator


class Storage:
    def init_app(self, app: Flask):
        storage_factory = self.get_storage_factory(dify_config.STORAGE_TYPE)
        with app.app_context():
            self.storage_runner = storage_factory()

    @staticmethod
    def get_storage_factory(storage_type: str) -> Callable[[], BaseStorage]:
        match storage_type:
            case StorageType.S3:
                from extensions.storage.aws_s3_storage import AwsS3Storage

                return AwsS3Storage
            case StorageType.OPENDAL:
                from extensions.storage.opendal_storage import OpenDALStorage

                return lambda: OpenDALStorage(dify_config.OPENDAL_SCHEME)
            case StorageType.LOCAL:
                from extensions.storage.opendal_storage import OpenDALStorage

                return lambda: OpenDALStorage(scheme="fs", root=dify_config.STORAGE_LOCAL_PATH)
            case StorageType.AZURE_BLOB:
                from extensions.storage.azure_blob_storage import AzureBlobStorage

                return AzureBlobStorage
            case StorageType.ALIYUN_OSS:
                from extensions.storage.aliyun_oss_storage import AliyunOssStorage

                return AliyunOssStorage
            case StorageType.GOOGLE_STORAGE:
                from extensions.storage.google_cloud_storage import GoogleCloudStorage

                return GoogleCloudStorage
            case StorageType.TENCENT_COS:
                from extensions.storage.tencent_cos_storage import TencentCosStorage

                return TencentCosStorage
            case StorageType.OCI_STORAGE:
                from extensions.storage.oracle_oci_storage import OracleOCIStorage

                return OracleOCIStorage
            case StorageType.HUAWEI_OBS:
                from extensions.storage.huawei_obs_storage import HuaweiObsStorage

                return HuaweiObsStorage
            case StorageType.BAIDU_OBS:
                from extensions.storage.baidu_obs_storage import BaiduObsStorage

                return BaiduObsStorage
            case StorageType.VOLCENGINE_TOS:
                from extensions.storage.volcengine_tos_storage import VolcengineTosStorage

                return VolcengineTosStorage
            case StorageType.SUPBASE:
                from extensions.storage.supabase_storage import SupabaseStorage

                return SupabaseStorage
            case _:
                raise ValueError(f"unsupported storage type {storage_type}")

    def save(self, filename, data):
        try:
            self.storage_runner.save(filename, data)
        except Exception as e:
            logger.exception(f"Failed to save file {filename}")
            raise e

    @overload
    @timeit
    def load(self, filename: str, /, *, stream: Literal[False] = False) -> bytes: ...

    @overload
    @timeit
    def load(self, filename: str, /, *, stream: Literal[True]) -> Generator: ...

    @timeit
    def load(self, filename: str, /, *, stream: bool = False) -> Union[bytes, Generator]:
        try:
            if stream:
                return self.load_stream(filename)
            else:
                return self.load_once(filename)
        except Exception as e:
            logger.exception(f"Failed to load file {filename}")
            raise e

    @timeit
    def load_once(self, filename: str) -> bytes:
        try:
            return self.storage_runner.load_once(filename)
        except Exception as e:
            logger.exception(f"Failed to load_once file {filename}")
            raise e

    @timeit
    def load_stream(self, filename: str) -> Generator:
        try:
            return self.storage_runner.load_stream(filename)
        except Exception as e:
            logger.exception(f"Failed to load_stream file {filename}")
            raise e

    @timeit
    def download(self, filename, target_filepath):
        try:
            self.storage_runner.download(filename, target_filepath)
        except Exception as e:
            logger.exception(f"Failed to download file {filename}")
            raise e

    @timeit
    def exists(self, filename):
        try:
            return self.storage_runner.exists(filename)
        except Exception as e:
            logger.exception(f"Failed to check file exists {filename}")
            raise e

    @timeit
    def delete(self, filename):
        try:
            return self.storage_runner.delete(filename)
        except Exception as e:
            logger.exception(f"Failed to delete file {filename}")
            raise e


storage = Storage()


def init_app(app: DifyApp):
    storage.init_app(app)
