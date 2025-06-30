# src/belibetl/pipelines/ingestion/pipeline.py

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_api_data, clean_data, insert_to_mongo

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=fetch_api_data,
                inputs=None,
                outputs="raw_data",
                name="fetch_api_data_node"
            ),
            node(
                func=clean_data,
                inputs="raw_data",
                outputs="cleaned_data",
                name="clean_data_node"
            ),
            node(
                func=insert_to_mongo,
                inputs="cleaned_data",
                outputs=None,
                name="insert_to_mongo_node"
            )
        ]
    )
