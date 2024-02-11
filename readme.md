# Pipeline Implementation 

## Features
1. Configurations 
2. Multiprocessings 
3. Output writter for db

4. Implementing Cache for Transformer[ Needs to be implemented]

## How to use 
1. Create your own data Structre & add them in `pipeline/structre` , example -

```
from typing import Any, Callable, Dict, TypeVar

from pydantic.dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Data:
    identifier: str
    metadata: Dict[str, Any]
    obj: Any
```

2. Add your own Transformer in `pipeline/components` & use registry decorator to register the component. example -

```
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
```

3. Setup config file, Kindly check the [Config file](!config.cfg)

4. Finally Run the Pipeline.

```
config_path = "./config.cfg"
pipeline = PipeLine.load(config_path)

print(pipeline)
data_ = [
    Data(identifier="1", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="2", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="3", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="4", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="5", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="6", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="7", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="8", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="9", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="10", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="11", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    Data(identifier="12", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
]
results = pipeline(data_)
print(results)
```