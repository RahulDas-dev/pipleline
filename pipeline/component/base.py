from abc import ABC, abstractmethod

from ..structure import Data


class BaseComponent(ABC):
    @abstractmethod
    def __call__(self, data: Data) -> Data:
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass
