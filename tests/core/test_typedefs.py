import pytest

from timothy.core._typedefs import Singleton


def test_singleton_can_only_be_instantiated_once():
    class OneAndOnly(Singleton):
        pass

    OneAndOnly()

    with pytest.raises(RuntimeError):
        OneAndOnly()
