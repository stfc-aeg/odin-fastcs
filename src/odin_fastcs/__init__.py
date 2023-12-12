from importlib.metadata import version  # noqa

__version__ = version("odin-fastcs")
del version

__all__ = ["__version__"]
