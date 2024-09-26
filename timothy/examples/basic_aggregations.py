"""Example pipeline using basic math."""

import argparse
import json
import pprint
from pathlib import Path
from typing import NotRequired, TypedDict

from timothy import json_pipeline
from timothy.core import Pipeline


class DataRow(TypedDict):
    """Row of data to be aggregated."""

    name: str
    type: str
    cost: float


class AggregationRow(TypedDict):
    """Row of fully aggregated data."""

    type: NotRequired[str]
    mean_cost: float
    std_cost: float


DataRows = list[DataRow]
AggregationRows = list[AggregationRow]


basic_agg_pipe = Pipeline("basic_agg")


def _aggregations(data: DataRows) -> AggregationRow:
    """Compute mean and std of cost across all rows. Not a direct pipeline stage."""
    mean = sum(row["cost"] for row in data) / len(data)
    std = (sum(row["cost"] ** 2 for row in data) / len(data) - mean**2) ** 0.5
    return AggregationRow(mean_cost=mean, std_cost=std)


@basic_agg_pipe.register(returns=["initial_data_without_excluded"])
def remove_excluded(initial_data: DataRows, exclude_types: list[str]) -> DataRows:
    """Exclude rows by type from initial data."""
    return [row for row in initial_data if row["type"] not in exclude_types]


@basic_agg_pipe.register(
    name="aggregations_by_type_without_excluded",
    params=["initial_data_without_excluded"],
    returns=["aggregated_by_type_without_excluded"],
)
@basic_agg_pipe.register(returns=["aggregated_by_type"])
def aggregations_by_type(initial_data: DataRows) -> AggregationRows:
    """Aggregate data by type."""
    types = sorted({row["type"] for row in initial_data})
    aggs = []
    for type_ in types:
        data_of_type = [row for row in initial_data if row["type"] == type_]
        agg = AggregationRow(type=type_, **_aggregations(data_of_type))
        agg.update(_aggregations(data_of_type))
        aggs.append(agg)
    return aggs


@basic_agg_pipe.register(
    name="aggregations_total_without_excluded",
    params=["initial_data_without_excluded"],
    returns=["aggregated_total_without_excluded"],
)
@basic_agg_pipe.register(returns=["aggregated_total"])
def aggregations_total(initial_data: DataRows) -> AggregationRow:
    """Aggregate over all rows."""
    return _aggregations(initial_data)


def setup_initial_data(json_dir: Path) -> None:
    """Set up the initial data, to be run outside of pipeline."""
    initial_data = [
        DataRow(name="apple", type="fruit", cost=1.23),
        DataRow(name="banana", type="fruit", cost=3.21),
        DataRow(name="carrot", type="vegetable", cost=0.89),
        DataRow(name="turnip", type="vegetable", cost=1.30),
        DataRow(name="juice", type="drink", cost=1.99),
        DataRow(name="soda", type="drink", cost=2.99),
        DataRow(name="rice", type="grain", cost=0.49),
        DataRow(name="couscous", type="grain", cost=1.49),
    ]
    json_dir.mkdir(parents=True, exist_ok=True)
    with (json_dir / "initial_data.json").open("w") as f:
        json.dump(initial_data, f, indent=4)


def _get_json_path_from_cli() -> Path:
    parser = argparse.ArgumentParser("basic aggregation pipeline example")
    parser.add_argument("json_path", type=Path)
    json_path: Path = parser.parse_args().json_path
    return json_path


if __name__ == "__main__":
    json_path = _get_json_path_from_cli()

    setup_initial_data(json_path)

    basic_agg_pipe = json_pipeline(basic_agg_pipe, json_path)

    basic_agg_pipe.set_values(exclude_types=["fruit", "grain"])

    basic_agg_pipe.run()

    values = basic_agg_pipe.get_values()
    print("Final values are: ")
    pprint.pp(values)
