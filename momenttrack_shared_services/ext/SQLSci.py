from typing import Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class SQLSci:
    __options: Dict
    __default_uri: Dict

    def __init__(self, db_config):
        self.binds = {}
        self.__options = db_config['SQLALCHEMY_BINDS']
        self.__default_uri = db_config['SQLALCHEMY_DATABASE_URI']

        for key, val in self.__options.items():
            self.binds[key] = create_engine(val)

    @property
    def writer_session(self):
        return sessionmaker('writer')

    @property
    def session(self):
        engine = create_engine(self.__default_uri)
        return sessionmaker(engine)
