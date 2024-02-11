import logging
from functools import reduce
from pathlib import Path
from typing import List, Optional, Set, Tuple, Union

from confit import Config
from joblib import Parallel, delayed, parallel_config

from .register import registry
from .structure import Data, TComponent

logger = logging.getLogger(__name__)


class PipeLine:
    _component: List[Tuple[str, TComponent]]
    _disable: List[str]

    def __init__(
        self, components: List[Tuple[str, TComponent]] = None, disable: List[str] = None
    ):
        self._component = [] if components is None else components
        self._disable = set() if disable is None else disable

    def __call__(self, data: Union[Data, List[Data]]) -> Data:
        list_of_callables = [
            component
            for name, component in self._component
            if name not in self._disable
        ]
        final_func = reduce(lambda f, g: lambda x: g(f(x)), list_of_callables)
        if isinstance(data, list):
            with parallel_config(backend="loky", n_jobs=-1, verbose=10):
                results = Parallel()(delayed(final_func)(item) for item in data)
        else:
            results = final_func(data)
        # results = final_func(data)
        return results

    def add_component(self, name: str, component: TComponent) -> TComponent:
        self._component.append((name, component))
        return component

    def __repr__(self):
        component = " -> ".join([str(c) for _, c in self._component])
        return f"PipeLine [ \n\tcomponents = {component}\n]"

    @property
    def component_names(self):
        return [name for name, _ in self._component]

    @classmethod
    def load(cls, config_path: Union[Path, str, Config]) -> "PipeLine":
        if isinstance(config_path, (Path, str)):
            config_path = Path(config_path)
            if config_path.is_file():
                config = Config.from_disk(config_path)
            else:
                raise ValueError(
                    "The Load function excepts, a config or Path to config file"
                )
        elif not isinstance(config_path, Config):
            raise ValueError(
                "The Load function excepts, a config or Path to config file"
            )
        return PipeLine.from_config(config)

    @classmethod
    def from_config(
        cls, config: Config, disable: Optional[Set[str]] = None
    ) -> "PipeLine":
        config_ = Config(config).copy()
        disable_ = disable if disable is not None else set()

        if "pipeline" not in config_.keys() or "components" not in config_.keys():
            raise ValueError(
                "PDF-Perser config must contain a 'pipeline' and a 'components' key"
            )

        config = Config(config).resolve(root=config_, registry=registry)

        component_cfg = config.get("components", {})
        pilpeline_cfg = config.get("pipeline", {})

        if "component_names" not in pilpeline_cfg.keys():
            raise ValueError(
                "PipeLine config must contain a 'component_name' key under 'pipeline'"
            )

        component_name = pilpeline_cfg.get("component_names", None)

        if component_name is None:
            raise ValueError(
                "PipeLine config must contain a valid list under 'component_name' key "
            )

        pipeline = cls()

        for name_ in component_name:
            if name_ not in component_cfg:
                raise ValueError(f"Component {repr(name_)} not found in config")
            pipeline.add_component(name_, component_cfg[name_])

        pipeline._disabled = [
            name for name in pipeline.component_names if name in disable_
        ]
        return pipeline
