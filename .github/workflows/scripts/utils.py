from pathlib import Path
from typing import Any, Dict, List, Tuple

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


def get_datasets_tables_from_modified_files(
    modified_files: List[str],
) -> List[Tuple[str, str]]:
    """
    Returns a list of (dataset_id, table_id) from the list of modified files.

    Args:
        modified_files (List[str]): List of modified files.

    Returns:
        List[Tuple[str, str]]: List of (dataset_id, table_id).
    """
    # Convert to Path
    modified_files: List[Path] = [Path(file) for file in modified_files]

    # Get SQL files
    sql_files: List[Path] = [file for file in modified_files if file.suffix == ".sql"]

    # Extract dataset_id and table_id from SQL files
    datasets_tables: List[Tuple[str, str]] = [
        (file.parent.name, file.stem) for file in sql_files
    ]

    # Post-process table_id:
    # - Some of `table_id` will have the format `{dataset_id}__{table_id}`. We must
    #   remove the `{dataset_id}__` part.
    new_datasets_tables: List[Tuple[str, str]] = []
    for dataset_id, table_id in datasets_tables:
        crop_str = f"{dataset_id}__"
        if table_id.startswith(crop_str):
            table_id = table_id[len(crop_str) :]
        new_datasets_tables.append((dataset_id, table_id))
    datasets_tables = new_datasets_tables

    # Get schema files
    schema_files: List[Path] = [
        file
        for file in modified_files
        if file.name.startswith("schema")
        and (file.suffix == ".yaml" or file.suffix == ".yml")
    ]

    # Extract dataset_id and table_id from schema files
    for schema_file in schema_files:
        dataset_id = schema_file.parent.name
        table_id = "__all__"
        datasets_tables.append((dataset_id, table_id))

    return datasets_tables


class Backend:
    def __init__(self, graphql_url: str):
        """
        Backend class for interacting with the backend.

        Args:
            graphql_url (str): URL of the GraphQL endpoint.
        """
        self._graphql_url: str = graphql_url

    @property
    def graphql_url(self) -> str:
        """
        GraphQL endpoint URL.
        """
        return self._graphql_url

    def _get_client(
        self, headers: Dict[str, str] = None, fetch_schema_from_transport: bool = False
    ) -> Client:
        """
        Get a GraphQL client.

        Args:
            headers (Dict[str, str], optional): Headers to be passed to the client. Defaults to
                None.
            fetch_schema_from_transport (bool, optional): Whether to fetch the schema from the
                transport. Defaults to False.

        Returns:
            Client: GraphQL client.
        """
        transport = RequestsHTTPTransport(
            url=self.graphql_url, headers=headers, use_json=True
        )
        return Client(
            transport=transport, fetch_schema_from_transport=fetch_schema_from_transport
        )

    def _execute_query(
        self,
        query: str,
        variables: Dict[str, str] = None,
        client: Client = None,
        headers: Dict[str, str] = None,
        fetch_schema_from_transport: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query.

        Args:
            query (str): GraphQL query.
            variables (Dict[str, str], optional): Variables to be passed to the query. Defaults
                to None.
            client (Client, optional): GraphQL client. Defaults to None.
            headers (Dict[str, str], optional): Headers to be passed to the client. Defaults to
                None.
            fetch_schema_from_transport (bool, optional): Whether to fetch the schema from the
                transport. Defaults to False.

        Returns:
            Dict: GraphQL response.
        """
        if not client:
            client = self._get_client(
                headers=headers, fetch_schema_from_transport=fetch_schema_from_transport
            )
        return client.execute(gql(query), variable_values=variables)

    def _get_dataset_id_from_slug(self, dataset_slug):
        query = """
            query ($slug: String!){
                allDataset(slug: $slug) {
                    edges {
                        node {
                            _id,
                        }
                    }
                }
            }
        """
        variables = {"slug": dataset_slug}
        response = self._execute_query(query=query, variables=variables)
        r = self._simplify_graphql_response(response)
        if r["allDataset"] != []:
            return r["allDataset"][0]["_id"]
        msg = f"{dataset_slug} not found. Please create the metadata first in {self.graphql_url}"
        raise Exception(msg)

    def _get_table_id_from_slug(self, dataset_slug, table_slug):
        query = """
            query ($dataset_Id: ID!, $table_slug: String!){
                allTable (dataset_Id:$dataset_Id slug:$table_slug){
                    edges {
                        node {
                            _id
                        }
                    }
                }
            }
        """

        variables = {
            "dataset_Id": self._get_dataset_id_from_slug(dataset_slug),
            "table_slug": table_slug,
        }
        response = self._execute_query(query=query, variables=variables)
        r = self._simplify_graphql_response(response)
        if r["allTable"] != []:
            return r["allTable"][0]["_id"]
        msg = f"No table {table_slug} found in {dataset_slug}."
        raise Exception(msg)
