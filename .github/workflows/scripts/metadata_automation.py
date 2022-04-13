"""
Automates metadata ingestion from Google Sheets to the models.
"""

import base64
from io import BytesIO
import json
from os import getenv
from pathlib import Path
from typing import List

from google.oauth2 import service_account
import gspread
import pandas as pd
import requests
import ruamel.yaml as ryaml

METADATA_FILE_PATH = "metadata.yaml"


def download_spreadsheet(
    spreadsheet_id: str, gspread_client: gspread.Client = None
) -> BytesIO:
    """
    Downloads a spreadsheet from Google Sheets.
    """
    if not gspread_client:
        gspread_client = get_gspread_client()
    url = f"https://www.googleapis.com/drive/v3/files/{spreadsheet_id}" + "?alt=media"

    res = requests.get(
        url, headers={"Authorization": "Bearer " + gspread_client.auth.token}
    )
    return BytesIO(res.content)


def dump_metadata_into_schema_yaml(
    dataset_id: str, table_id: str, metadata: dict
) -> None:
    """
    Dumps the metadata into the schema.yaml file.
    """
    schema_yaml_path = f"models/{dataset_id}/schema.yml"

    schema = (
        load_metadata_file(schema_yaml_path)
        if Path(schema_yaml_path).exists()
        else {"version": 2, "models": []}
    )
    if schema is None:
        schema = {"version": 2, "models": []}

    if len(schema["models"]) > 0:
        # If we find a model with the same name, we delete it.
        schema["models"] = [m for m in schema["models"] if m["name"] != table_id]
    schema["models"].append(metadata)

    ruamel = load_ruamel()
    ruamel.dump(
        schema,
        open(Path(schema_yaml_path), "w", encoding="utf-8"),
    )


def fetch_metadata_from_google_sheets(
    spreadsheet_id: str, table_id: str, gspread_client: gspread.Client = None
) -> dict:
    """
    Fetches the metadata from Google Sheets.

    Model:
    {
        "name": "table_id",
        "description": "A starter dbt model",
        "columns": [
            {
                "name": "id",
                "description": "The primary key for this table",
                "tests": [
                    "unique",
                    "not_null",
                ],
            },
        ],
    }
    """

    if not gspread_client:
        gspread_client = get_gspread_client()
    spreadsheet: BytesIO = download_spreadsheet(spreadsheet_id, gspread_client)

    df_table_metadata = pd.read_excel(
        spreadsheet, sheet_name="tabela", header=None
    ).fillna(" ")

    df_columns_metadata = pd.read_excel(spreadsheet, sheet_name="colunas")
    df_columns_metadata = df_columns_metadata[
        (df_columns_metadata["Nome da coluna"].notnull())
        & (df_columns_metadata["Tipo da coluna"].notnull())
    ].fillna(" ")

    table_description = format_table_description(df_table_metadata)
    columns_metadata = format_columns_metadata(df_columns_metadata)
    return {
        "name": table_id,
        "description": table_description,
        "columns": columns_metadata,
    }


def format_columns_metadata(dataframe: pd.DataFrame) -> List[dict]:
    """
    Formats the columns metadata.
    """
    columns_metadata = []
    for row in dataframe.iterrows():
        if pd.isna(row[1]["Nome da coluna"]):
            continue
        column_metadata = {
            "name": row[1]["Nome da coluna"],
            "description": row[1]["Descrição da coluna"],
        }
        columns_metadata.append(column_metadata)
    return columns_metadata


def format_table_description(dataframe: pd.DataFrame) -> str:
    """
    Formats the table description.
    """
    return "".join(f"**{row[1][0]}:** {row[1][1]}\n" for row in dataframe.iterrows())


def get_credentials_from_env(scopes: list = None) -> service_account.Credentials:
    """Gets credentials from env vars"""
    env: str = getenv("GKE_SA_KEY")
    info: dict = json.loads(base64.b64decode(env))
    cred = service_account.Credentials.from_service_account_info(info)
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred


def get_gspread_client() -> gspread.Client:
    """Gets gspread client"""
    GSPREAD_SCOPE = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    cred = get_credentials_from_env(scopes=GSPREAD_SCOPE)
    client = gspread.Client(auth=cred)
    client.login()
    return client


def load_ruamel():
    """
    Loads a YAML file.
    """
    ruamel = ryaml.YAML()
    ruamel.default_flow_style = False
    ruamel.top_level_colon_align = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    return ruamel


def load_metadata_file(filepath: str) -> dict:
    """
    Loads the file that contains path to the models' metadata.
    """
    ruamel = load_ruamel()
    return ruamel.load((Path(filepath)).open(encoding="utf-8"))


if __name__ == "__main__":
    # Load the metadata file
    metadata: dict = load_metadata_file(METADATA_FILE_PATH)

    # List all models
    models: dict = metadata["models"]

    # Iterate over datasets
    for dataset_id, dataset in models.items():

        print(f"Ingesting metadata for dataset {dataset_id}")

        # Iterate over tables
        for table_id in dataset:

            # Get the table
            table: dict = dataset[table_id]

            # Check whether there is a spreadsheet ID set for this table
            if "spreadsheet_id" in table and table["spreadsheet_id"]:
                print(f"- Fetching metadata for table {table_id}...", end="\n")

                # Fetch the metadata from Google Sheets
                table_metadata = fetch_metadata_from_google_sheets(
                    table["spreadsheet_id"],
                    table_id,
                )
                # Dump the metadata into the schema.yaml file
                dump_metadata_into_schema_yaml(dataset_id, table_id, table_metadata)
