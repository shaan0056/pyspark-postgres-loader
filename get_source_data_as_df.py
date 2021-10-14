"""
This is an abstract class with a 'get_source_data_as_df' method signature which needs
to be implemented by subclass, which should provide the source data as
a Spark DataFrame with the column names in lower case and matching with the target
Postgres table column names to load.
"""
from abc import ABC, abstractmethod


class GetSourceDataAsDf(ABC):

    @abstractmethod
    def get_source_data_as_df(self):
        raise NotImplementedError

    def __repr__(self):
        return repr(self.__dict__)
