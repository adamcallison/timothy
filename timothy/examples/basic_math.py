"""Example pipeline using basic math."""

from timothy import DAGPipelineStageRunner, MemoryPipelineIO
from timothy.core import Pipeline

basic_math_pipeline = Pipeline("basic_math", stage_runner=DAGPipelineStageRunner())
basic_math_pipeline.register_object("num1", MemoryPipelineIO[int](initial_value=5))
basic_math_pipeline.register_object("num2", MemoryPipelineIO[float](initial_value=7.3))
basic_math_pipeline.register_object("num3", MemoryPipelineIO[int]())
basic_math_pipeline.register_object("num4", MemoryPipelineIO[float]())
basic_math_pipeline.register_object("num5", MemoryPipelineIO[float]())


@basic_math_pipeline.register(returns=["num5"])
def add_num3_and_num4(num3: int, num4: float) -> float:
    """Add num3 and num 4."""
    num5 = num3 + num4
    print(f"Added num3 ({num3}) and num4 ({num4}) to get num5 ({num5}).")
    return num5


@basic_math_pipeline.register(returns=["num4"])
def cube_num2(num2: float) -> float:
    """Cube num2."""
    num4 = num2**3
    print(f"Cubed num2 ({num2}) to get num4 ({num4}).")
    return num4


@basic_math_pipeline.register(returns=["num3"])
def square_num1(num1: int) -> int:
    """Square num1."""
    num3 = num1**2
    print(f"Squared num1 ({num1}) to get num3 ({num3}).")
    return num3


if __name__ == "__main__":
    basic_math_pipeline.run()
    values = {k: v.load() for k, v in basic_math_pipeline.objects.items()}
    print(f"Final values are: {values}")
