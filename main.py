import json
import tempfile
import os
from pathlib import Path

import mlflow
import wandb
import hydra
from omegaconf import DictConfig

_steps = [
    "download",
    "basic_cleaning",
    "data_check",
    "data_split",
    "train_random_forest",
    # NOTE: We do not include this in the steps so it is not run by mistake.
    # You first need to promote a model export to "prod" before you can run this,
    # then you need to run this step explicitly
#    "test_regression_model"
]


# This automatically reads in the configuration
@hydra.main(config_name='config')
def go(config: DictConfig):

    # Setup the wandb experiment. All runs will be grouped under this name
    os.environ["WANDB_PROJECT"] = config["main"]["project_name"]
    os.environ["WANDB_RUN_GROUP"] = config["main"]["experiment_name"]

    # Steps to execute
    steps_par = config['main']['steps']
    active_steps = steps_par.split(",") if steps_par != "all" else _steps
    
    cwd_path = Path(hydra.utils.get_original_cwd())

    # Move to a temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:

        if "download" in active_steps:
            # Download file and load in W&B
            _ = mlflow.run(
                f"{config['main']['components_repository']}/get_data",
                "main",
                parameters={
                    "sample": config["etl"]["sample"],
                    "artifact_name": "sample.csv",
                    "artifact_type": "raw_data",
                    "artifact_description": "Raw file as downloaded"
                },
            )

        if "basic_cleaning" in active_steps:
            _ = mlflow.run(
                str(cwd_path / "src" / "basic_cleaning"),
                parameters={
                    **config["cleaning"]
                },
            )

        if "data_check" in active_steps:
            ##################
            # Implement here #
            ##################
            _ = mlflow.run(
                str(cwd_path / "src" / "data_check"),
                parameters={
                    **config["data_check"],
                    "min_price": config["cleaning"]["min_price"],
                    "max_price": config["cleaning"]["max_price"],
                },
            )

        if "data_split" in active_steps:
            ##################
            # Implement here #
            ##################
            _ = mlflow.run(
                f"{config['main']['components_repository']}/train_val_test_split",
                parameters={
                    "input" : f'{config["cleaning"]["output_artifact"]}:latest',
                    "test_size" : config["modeling"]["test_size"],
                    "random_seed" : config["modeling"]["random_seed"],
                    "stratify_by" : config["modeling"]["stratify_by"],
                },
            )

        if "train_random_forest" in active_steps:

            # NOTE: we need to serialize the random forest configuration into JSON
            rf_config = os.path.abspath("rf_config.json")
            with open(rf_config, "w+") as fp:
                json.dump(dict(config["modeling"]["random_forest"].items()), fp)  # DO NOT TOUCH

            # NOTE: use the rf_config we just created as the rf_config parameter for the train_random_forest
            # step

            ##################
            # Implement here #
            ##################

            _ = mlflow.run(
                str(cwd_path / "src" / "train_random_forest"),
                parameters={
                    "trainval_artifact" : config["modeling"]["trainval_artifact"],
                    "val_size" : config["modeling"]["val_size"],
                    "random_seed" : config["modeling"]["random_seed"],
                    "stratify_by" : config["modeling"]["stratify_by"],
                    "rf_config" : rf_config,
                    "max_tfidf_features" : config["modeling"]["max_tfidf_features"],
                    "output_artifact" : config["modeling"]["output_artifact"],
                },
            )

        if "test_regression_model" in active_steps:

            ##################
            # Implement here #
            ##################

            _ = mlflow.run(
                str(cwd_path / "components" / "test_regression_model"),
                parameters={
                    **config["testing"]
                },
            )


if __name__ == "__main__":
    go()
