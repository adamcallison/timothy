"""Example pipeline using basic math."""

import argparse
import json
import pprint
from pathlib import Path
from typing import NotRequired, TypedDict

from timothy import DAGPipelineStageRunner, JSONFilePipelineIO, MemoryPipelineIO
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


def _get_pipeline(json_dir: Path) -> Pipeline:
    basic_agg_pipeline = Pipeline("basic_agg", stage_runner=DAGPipelineStageRunner())
    basic_agg_pipeline.register_object(
        "exclude_types",
        MemoryPipelineIO[list[str]](initial_value=["fruit", "grain"]),
    )
    basic_agg_pipeline.register_object(
        "initial_data",
        JSONFilePipelineIO[DataRows](json_dir / "initial_data.json"),
    )
    basic_agg_pipeline.register_object(
        "initial_data_without_excluded",
        JSONFilePipelineIO[DataRows](json_dir / "initial_data_without_exluded.json"),
    )
    basic_agg_pipeline.register_object(
        "aggregated_by_type",
        JSONFilePipelineIO[AggregationRows](json_dir / "aggregated_by_type.json"),
    )
    basic_agg_pipeline.register_object(
        "aggregated_by_type_without_excluded",
        JSONFilePipelineIO[AggregationRows](json_dir / "aggregated_by_type_without_excluded.json"),
    )
    basic_agg_pipeline.register_object(
        "aggregated_total",
        JSONFilePipelineIO[AggregationRow](json_dir / "aggregated_total.json"),
    )
    basic_agg_pipeline.register_object(
        "aggregated_total_without_excluded",
        JSONFilePipelineIO[AggregationRow](json_dir / "aggregated_total_without_excluded.json"),
    )

    def _aggregations(data: DataRows) -> AggregationRow:
        """Compute mean and std of cost across all rows. Not a direct pipeline stage."""
        mean = sum(row["cost"] for row in data) / len(data)
        std = (sum(row["cost"] ** 2 for row in data) / len(data) - mean**2) ** 0.5
        return AggregationRow(mean_cost=mean, std_cost=std)

    @basic_agg_pipeline.register(returns=["initial_data_without_excluded"])
    def remove_excluded(initial_data: DataRows, exclude_types: list[str]) -> DataRows:
        """Exclude rows by type from initial data."""
        return [row for row in initial_data if row["type"] not in exclude_types]

    @basic_agg_pipeline.register(
        name="aggregations_by_type_without_excluded",
        params=["initial_data_without_excluded"],
        returns=["aggregated_by_type_without_excluded"],
    )
    @basic_agg_pipeline.register(returns=["aggregated_by_type"])
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

    @basic_agg_pipeline.register(
        name="aggregations_total_without_excluded",
        params=["initial_data_without_excluded"],
        returns=["aggregated_total_without_excluded"],
    )
    @basic_agg_pipeline.register(returns=["aggregated_total"])
    def aggregations_total(initial_data: DataRows) -> AggregationRow:
        """Aggregate over all rows."""
        return _aggregations(initial_data)

    return basic_agg_pipeline


def _get_json_dir_from_cli() -> Path:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_dir", type=Path)
    json_dir: Path = parser.parse_args().json_dir
    return json_dir


def _setup_initial_data(json_dir: Path) -> None:
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


if __name__ == "__main__":
    json_dir = _get_json_dir_from_cli()
    _setup_initial_data(json_dir)
    basic_agg_pipeline = _get_pipeline(json_dir)

    basic_agg_pipeline.run()

    values = {k: v.load() for k, v in basic_agg_pipeline.objects.items()}
    print("Final values are: ")
    pprint.pp(values)
