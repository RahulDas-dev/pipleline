import logging
from typing import Literal

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session

from ..db.curd import add_a_new_results
from ..db.orm import meta
from ..register import registry
from ..structure import Data
from .base import BaseComponent

logger = logging.getLogger(__name__)

DBType = Literal["POSTGRES", "MYSQL", "ORACLE", "SQLITE"]


@registry.factory.register("db_writter")
class DbWritter(BaseComponent):
    def __init__(
        self,
        kind: DBType,
        db_name: str,
        host: str = None,
        port: str = None,
        user: str = None,
        password: str = None,
    ):
        if kind == "SQLITE":
            self.url = URL.create(drivername="sqlite", database=db_name)
        else:
            if kind == "POSTGRES":
                drivername_ = "postgresql+pg8000"
            elif kind == "MYSQL":
                drivername_ = "mysql+pymysql"
            else:
                drivername_ = "oracle"
            self.url = URL.create(
                drivername=drivername_,
                database=db_name,
                host=host,
                port=port,
                username=user,
                password=password,
            )
        self.table_exists = False
        self.engine = None

    def __repr__(self) -> str:
        return f"DbWritter[url={self.url}]"

    def _create_table(self):
        if self.table_exists:
            return
        self.engine = create_engine(self.url, echo=False)
        with self.engine.begin() as connection:
            return_code = meta.create_all(connection, checkfirst=True)
            logger.info(f"Retuen Code {return_code}")
            self.table_exists = True

    def __call__(self, data: Data) -> Data:
        # Transform The Data here
        logger.info("DbWritter ....")
        if not self.table_exists:
            self._create_table()
        with Session(self.engine) as session:
            session.begin()
            try:
                data_dict = {
                    "identifier": data.identifier,
                    "data_description": data.metadata,
                    "data": str(data.obj),
                    "archived": False,
                }
                data_ = add_a_new_results(session=session, insert_dict=data_dict)
                if data_ is None:
                    session.rollback()
                else:
                    session.commit()
            except Exception as err:
                logger.error(f"Error While {err}")
                session.rollback()
                data_ = None
            finally:
                session.close()
        self.engine.dispose()
        return data_
