from confit import Registry, RegistryCollection


class registry(RegistryCollection):
    factory = Registry(("pipeline", "factory"), entry_points=True)
