"""
ydb_connector
---------------------------

| Provides the version of the :py:class:`~df_db.connector.db_connector.DBConnector` for YDB.

"""
from .db_connector import DBConnector, threadsafe_method
from df_engine.core.context import Context

import os
import json
from urllib.parse import urlsplit
import ydb


class YDBConnector(DBConnector):
    """
    | Version of the :py:class:`~df_db.connector.db_connector.DBConnector` for YDB.

    Parameters
    -----------

    path: str
        Standard sqlalchemy URI string.
        When using sqlite backend in Windows, keep in mind that you have to use double backslashes '\\'
        instead of forward slashes '/' in the file path.
    table_name: str
        The name of the table to use.
    """

    def __init__(self, path: str, table_name: str = "contexts"):
        super(YDBConnector, self).__init__(path)
        _, self.entrypoint, self.table_path, _, _ = urlsplit(path)
        self.table_name = table_name

        self.driver_config = ydb.DriverConfig(
            self.entrypoint,
            self.table_path,
            root_certificates=ydb.load_ydb_root_certificate(),
        )

        self.driver = ydb.Driver(self.driver_config)
        self.driver.wait(timeout=5, fail_fast=True)

        self.pool = ydb.SessionPool(self.driver)

        if not self._is_table_exists(self.pool, self.table_path, self.table_name):  # create table if it does not exist
            self._create_table(self.pool, self.table_path, self.table_name)

    @threadsafe_method
    def __setitem__(self, key: str, value: Context) -> None:
        if isinstance(value, Context):
            value = value.dict()

        if not isinstance(value, dict):
            raise TypeError(f"The saved value should be a dict or a dict-serializeable item, not {type(value)}")

        def callee(session):
            query = """
                PRAGMA TablePathPrefix("{}");
                DECLARE $queryId AS Uint64;
                DECLARE $queryContext AS Json;

                UPSERT INTO {}
                (
                    id,
                    context
                )
                VALUES
                (
                    $queryId,
                    $queryContext
                );
                """.format(
                self.table_path, self.table_name
            )
            prepared_query = session.prepare(query)

            session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {"$queryId": int(key), "$queryContext": json.dumps(value)},
                commit_tx=True,
            )

        return self.pool.retry_operation_sync(callee)

    @threadsafe_method
    def __getitem__(self, key: str) -> Context:
        def callee(session):
            query = """
                PRAGMA TablePathPrefix("{}");
                DECLARE $queryId AS Uint64;

                SELECT
                    id,
                    context
                FROM {}
                WHERE id = $queryId;
                """.format(
                self.table_path, self.table_name
            )
            prepared_query = session.prepare(query)

            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {
                    "$queryId": int(key),
                },
                commit_tx=True,
            )
            if result_sets[0].rows:
                return Context.cast(result_sets[0].rows[0].context)
            else:
                raise KeyError

        return self.pool.retry_operation_sync(callee)

    @threadsafe_method
    def __delitem__(self, key: str) -> None:
        def callee(session):
            query = """
                PRAGMA TablePathPrefix("{}");
                DECLARE $queryId AS Uint64;

                DELETE
                FROM {}
                WHERE
                    id = $queryId
                ;
                """.format(
                self.table_path, self.table_name
            )
            prepared_query = session.prepare(query)

            session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {"$queryId": int(key)},
                commit_tx=True,
            )

        return self.pool.retry_operation_sync(callee)

    @threadsafe_method
    def __contains__(self, key: str) -> bool:
        def callee(session):
            # new transaction in serializable read write mode
            # if query successfully completed you will get result sets.
            # otherwise exception will be raised
            query = """
                PRAGMA TablePathPrefix("{}");
                DECLARE $queryId AS Uint64;

                SELECT
                    id,
                    context
                FROM {}
                WHERE id = $queryId;
                """.format(
                self.table_path, self.table_name
            )
            prepared_query = session.prepare(query)

            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {
                    "$queryId": int(key),
                },
                commit_tx=True,
            )
            return len(result_sets[0].rows) > 0

        return self.pool.retry_operation_sync(callee)

    @threadsafe_method
    def __len__(self) -> int:
        def callee(session):
            query = """
                PRAGMA TablePathPrefix("{}");

                SELECT
                    COUNT(*) as cnt
                FROM {}
                """.format(
                self.table_path, self.table_name
            )
            prepared_query = session.prepare(query)

            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                commit_tx=True,
            )
            return result_sets[0].rows[0].cnt

        return self.pool.retry_operation_sync(callee)

    @threadsafe_method
    def clear(self) -> None:
        def callee(session):
            query = """
                PRAGMA TablePathPrefix("{}");
                DECLARE $queryId AS Uint64;

                DELETE
                FROM {}
                WHERE
                    id > 0
                ;
                """.format(
                self.table_path, self.table_name
            )
            prepared_query = session.prepare(query)

            session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {},
                commit_tx=True,
            )

        return self.pool.retry_operation_sync(callee)

    def _is_directory_exists(self, driver, path):
        try:
            return driver.scheme_client.describe_path(path).is_directory()
        except ydb.SchemeError:
            return False

    def _ensure_path_exists(self, driver, database, path):
        paths_to_create = list()
        path = path.rstrip("/")
        while path not in ("", database):
            full_path = os.path.join(database, path)
            if self._is_directory_exists(driver, full_path):
                break
            paths_to_create.append(full_path)
            path = os.path.dirname(path).rstrip("/")

        while len(paths_to_create) > 0:
            full_path = paths_to_create.pop(-1)
            driver.scheme_client.make_directory(full_path)

    def _is_table_exists(self, pool, path, table_name):
        try:

            def callee(session):
                session.describe_table(os.path.join(path, table_name))

            pool.retry_operation_sync(callee)
            return True
        except ydb.SchemeError:
            return False

    def _create_table(self, pool, path, table_name):
        def callee(session):
            session.create_table(
                os.path.join(path, table_name),
                ydb.TableDescription()
                .with_column(ydb.Column("id", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_column(ydb.Column("context", ydb.OptionalType(ydb.PrimitiveType.Json)))
                .with_primary_key("id"),
            )

        return pool.retry_operation_sync(callee)
