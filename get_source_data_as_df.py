from abc import ABC, abstractmethod


class GetSourceDataAsDf(ABC):

    @abstractmethod
    def get_source_data_as_df(self):
        raise NotImplementedError

    def __repr__(self):
        return repr(self.__dict__)
