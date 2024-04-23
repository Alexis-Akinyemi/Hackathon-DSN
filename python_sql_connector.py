import functools
import gc

import psycopg2
import pandas as pd


class SQLConnector:
    def __init__(self):
        pass

    def open_connection(
        self,
        database: str = None,
        user: str = None,
        password: str = None,
        host: str = None,
        port: str = None,
        connection_type: str = "postgres",
    ):
        # if a connexion is already open, close it
        if hasattr(self, "connection"):
            self.close_connection()

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.connection = psycopg2.connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def _check_connection(func):
        """
        Decorator to check if connection is open.
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            assert hasattr(self, "connection"), "there is no open connection."
            return func(self, *args, **kwargs)

        return wrapper

    @_check_connection
    def close_connection(self):
        """
        Close a SQL Server connection.
        """

        self.connection.close()

        del self.connection
        gc.collect()

        print("connection closed")

    @_check_connection
    def execute_query(
        self,
        query: str,
        commit: bool = False,
        fetch: str = "all",
        return_dataframe: bool = False,
        return_records: bool = False,
        return_cursor: bool = False,
    ):
        """
        Execute a SQL query.

        Parameters
        ----------
        query : str
            SQL query.
        commit : bool, optional
            Commit the query, by default False.
        fetch : str, optional
            Retrieve data from the database using methods such as fetchone() or fetchall(), by default "all".
        return_dataframe : bool, optional
            Return data on a pandas.DataFrame object, by default False.
        return_records : bool, optional
            Return data with records format, by default False.
        return_cursor : bool, optional
            Return open cursor else close it, by default False.

        Returns
        -------
        results : list or pd.DataFrame
            Query results.
        """
        assert fetch in ["one", "all"]
        assert (
            return_dataframe != True or return_records != True
        ), "return_dataframe and return_records cannot both be set to true"

        with self.connection as connection:
            cursor = connection.cursor()
            cursor.execute(query)

            if commit:
                try:
                    self.connection.commit()
                except Exception as e:
                    self.connection.rollback()
                    print(e)

                if return_cursor:
                    return cursor

                else:
                    cursor.close()
                    return

            if fetch == "one":
                results = cursor.fetchone()
                results = [] if results is None else [results]
            else:
                results = cursor.fetchall()
                results = [list(row) for row in results]

            columns = [row[0] for row in cursor.description]

            if return_dataframe:
                results = pd.DataFrame(results, columns=columns)

            if return_records:
                results = [
                    {column: value for value, column in zip(row, columns)} for row in results
                ]

            if return_cursor:
                return results, cursor

            cursor.close()

            return results
