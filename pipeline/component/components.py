import logging

from ..register import registry
from ..structure import Data
from .base import BaseComponent

logger = logging.getLogger(__name__)


@registry.factory.register("component_1")
class Component1(BaseComponent):
    def __init__(self, arg11, arg12):
        self.arg1 = arg11
        self.arg2 = arg12

    def __repr__(self) -> str:
        return f"Component1[arg1={self.arg1} arg2={self.arg1}]"

    def __call__(self, data: Data) -> Data:
        # Transform The Data here
        logger.info("Component1 ....")
        return data


@registry.factory.register("component_2")
class Component2(BaseComponent):
    def __init__(self, arg21, arg22):
        self.arg1 = arg21
        self.arg2 = arg22

    def __repr__(self) -> str:
        return f"Component2[arg1={self.arg1} arg2={self.arg2}]"

    def __call__(self, data: Data) -> Data:
        # Transform The Data here
        logger.info("Component2 ....")
        return data


@registry.factory.register("component_3")
class Component3(BaseComponent):
    def __init__(self, arg31, arg32):
        self.arg1 = arg31
        self.arg2 = arg32

    def __repr__(self) -> str:
        return f"Component3[arg1={self.arg1} arg2={self.arg2}]"

    def __call__(self, data: Data) -> Data:
        # Transform The Data here
        logger.info("Component3 ....")
        return data
