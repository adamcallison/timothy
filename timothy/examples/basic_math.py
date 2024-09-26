"""Example pipeline using basic math."""

from timothy import memory_pipeline
from timothy.core import Pipeline

basic_math_pipe = Pipeline("basic_math")


@basic_math_pipe.register(returns=["num5"])
def add_num3_and_num4(num3: int, num4: float) -> float:
    """Add num3 and num 4."""
    num5 = num3 + num4
    print(f"Added num3 ({num3}) and num4 ({num4}) to get num5 ({num5}).")
    return num5


@basic_math_pipe.register(returns=["num4"])
def cube_num2(num2: float) -> float:
    """Cube num2."""
    num4 = num2**3
    print(f"Cubed num2 ({num2}) to get num4 ({num4}).")
    return num4


@basic_math_pipe.register(returns=["num3"])
def square_num1(num1: int) -> int:
    """Square num1."""
    num3 = num1**2
    print(f"Squared num1 ({num1}) to get num3 ({num3}).")
    return num3


if __name__ == "__main__":
    basic_math_pipe = memory_pipeline(basic_math_pipe)
    basic_math_pipe.set_values(num1=5, num2=7.3)
    basic_math_pipe.run()
    values = basic_math_pipe.get_values()
    print(f"Final values are: {values}")
