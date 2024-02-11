from typing import Any, Callable, Dict, TypeVar

from pydantic.dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Data:
    identifier: str
    metadata: Dict[str, Any]
    obj: Any

    def replicate(self, **kwargs) -> "Data":
        identifier_ = kwargs.get("identifier", None)
        metadata_ = kwargs.get("metadata", None)
        obj_ = kwargs.get("obj", None)
        # print(identifier_, self.identifier, metadata_, self.metadata, obj_, self.obj)
        return Data(
            identifier=self.identifier if identifier_ is None else identifier_,
            metadata=self.metadata if metadata_ is None else metadata_,
            obj=self.obj if obj_ is None else obj_,
        )


TComponent = TypeVar("TComponent", bound=Callable[[Data], Data])
