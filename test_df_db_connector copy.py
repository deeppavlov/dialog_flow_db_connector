# # %%
# import pytest
import socket
import os
from platform import system

# from df_engine.core.context import Context

from df_db_connector.db_connector import DBConnector, DBAbstractConnector
from df_db_connector.ydb_connector import YDBConnector, ydb_available
from df_db_connector import connector_factory


# def ping_localhost(port: int, timeout=60):
#     try:
#         socket.setdefaulttimeout(timeout)
#         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         s.connect(("localhost", port))
#     except OSError as error:
#         return False
#     else:
#         s.close()
#         return True


# YDB_ACTIVE = ping_localhost(2136)


# def generic_test(connector_instance, testing_context, testing_telegram_id):
#     assert isinstance(connector_instance, DBConnector)
#     assert isinstance(connector_instance, DBAbstractConnector)
#     # perform cleanup
#     connector_instance.clear()
#     assert len(connector_instance) == 0
#     # test write operations
#     connector_instance[testing_telegram_id] = {"foo": "bar", "baz": "qux"}
#     assert testing_telegram_id in connector_instance
#     assert len(connector_instance) == 1
#     connector_instance[testing_telegram_id] = testing_context  # overwriting a key
#     assert len(connector_instance) == 1
#     # test read operations
#     new_ctx = connector_instance[testing_telegram_id]
#     assert isinstance(new_ctx, Context)
#     assert new_ctx.dict() == testing_context.dict()
#     # test delete operations
#     del connector_instance[testing_telegram_id]
#     assert testing_telegram_id not in connector_instance
#     # test `get` method
#     assert connector_instance.get(testing_telegram_id) is None


# # @pytest.mark.skipif(YDB_ACTIVE == False, reason="YQL server not running")
# # @pytest.mark.skipif(ydb_available == False, reason="YDB dependencies missing")
# # def test_ydb(testing_context, testing_telegram_id):

# # %%
#  connector_instance = YDBConnector("grpc://localhost:2136/local", "test")
connector_instance = YDBConnector("grpc://localhost:2136/local", "test")
# # generic_test(connector_instance, testing_context, testing_telegram_id)
connector_instance

# %%
# import ydb
# driver = ydb.Driver(
#         endpoint="grpc://localhost:2136",
#         database="/local",
#     )
# driver.wait(fail_fast=True, timeout=5)
# # %%
# import os
# os.environ["YDB_TOKEN"] = "t1.9euelZqZlpuMj8udlZOemJePy5fOju3rnpWalpWJncuSx5ySl5vHz86Nlsnl8_deEwho-e8kDWpf_t3z9x5CBWj57yQNal_-.DCIMeKs4-WatHChnYDYJBZkjOQeIwwmPsSU4zFVnse53PHiaBxf5cHZJ4aCD7gKmKlD2z1dhleXTDUe5Fh2YCQ"
# import ydb
# driver_config = ydb.DriverConfig(
#     'grpcs://ydb.serverless.yandexcloud.net:2135', '/ru-central1/b1gd15knbklm5j624l8r/etn87mouuuqsrim0p8hc',
#     credentials=ydb.construct_credentials_from_environ(),
#     root_certificates=ydb.load_ydb_root_certificate(),
# )
# print(driver_config)
# ydb.construct_credentials_from_environ()
# # with ydb.Driver(driver_config) as driver:
# #     try:
# #         driver.wait(timeout=15)
# #     except TimeoutError:
# #         print("Connect failed to YDB")
# #         print("Last reported errors by discovery:")
# #         print(driver.discovery_debug_details())
# # %%
# driver = ydb.Driver(driver_config)
# # %%
# ydb.construct_credentials_from_environ()
# # %%
# driver.wait(timeout=15)
# # %%
# print(driver.discovery_debug_details())
# %%
