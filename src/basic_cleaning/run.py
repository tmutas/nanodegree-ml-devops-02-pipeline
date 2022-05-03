#!/usr/bin/env python
"""
Download raw data from WandB, apply basic data cleaning and export processed data to WandB
"""
import argparse
import logging
from tempfile import NamedTemporaryFile 

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def filter_column_bounds(df : pd.DataFrame, col : str, min_val, max_val):
    """Filter out rows outside given bounds

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    col : str
        Column name
    min_val 
        minimum_value, has to be type of the provided column
    max_val
        maximum_value, has to be type of the provided column

    Returns
    ---------------
    pd.DataFrame
        Filtered dataframe
    """    
    mask = df.loc[:,col].between(min_val, max_val)

    return df[mask].copy(), len(df) - mask.sum()

def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    df = pd.read_csv(artifact_local_path)
    logger.info("Downloaded input dataframe of shape %s", df.shape)

    df, dropped = filter_column_bounds(df, "price", args.min_price, args.max_price)
    logger.info(
        "Filtered out %s rows outside of price %s and %s", 
        dropped,
        args.min_price,
        args.max_price,
    )

    df, dropped  = filter_column_bounds(df, "longitude", args.min_longitude, args.max_longitude)
    logger.info(
        "Filtered out %s rows outside of longitude %s and %s", 
        dropped,
        args.min_longitude,
        args.max_longitude,
    )

    df, dropped  = filter_column_bounds(df, "latitude", args.min_latitude, args.max_latitude)
    logger.info(
        "Filtered out %s rows outside of latitude %s and %s", 
        dropped,
        args.min_latitude,
        args.max_latitude,
    )
    
    with NamedTemporaryFile() as fil:
        df.to_csv(fil.name, index=False)
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )
        artifact.add_file(fil.name)
        run.log_artifact(artifact)
        # Necessary to ensure artifact is uploaded before tempfile closes
        artifact.wait()
        logger.info(
            "Output artifact %s succesfully logged", 
            args.output_artifact
        )
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price of records to kept in dataset",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Minimum price of records to kept in dataset",
        required=True
    )

    parser.add_argument(
        "--min_longitude", 
        type=float,
        help="Minimum longitude of records to kept in dataset",
        required=True
    )

    parser.add_argument(
        "--max_longitude", 
        type=float,
        help="Minimum longitude of records to kept in dataset",
        required=True
    )

    parser.add_argument(
        "--min_latitude", 
        type=float,
        help="Minimum latitude of records to kept in dataset",
        required=True
    )

    parser.add_argument(
        "--max_latitude", 
        type=float,
        help="Minimum latitude of records to kept in dataset",
        required=True
    )


    args = parser.parse_args()

    go(args)
