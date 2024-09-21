"""Example pipeline using basic math."""

from operator import itemgetter

from timothy import DAGPipelineStageRunner, MemoryPipeline

basic_math_pipe = MemoryPipeline("basic_math", stage_runner=DAGPipelineStageRunner())


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
    basic_math_pipe.set_initial_values(num1=5, num2=7.3)
    basic_math_pipe.run()
    values = {k: v.load() for k, v in sorted(basic_math_pipe.objects.items(), key=itemgetter(0))}
    print(f"Final values are: {values}")
