import json
import logging
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote
import uuid

from mcp.server.fastmcp import FastMCP
import requests
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable

# Create MCP instance
mcp = FastMCP("fabric_schemas")

# Set up logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class FabricApiConfig:
    """Configuration for Fabric API"""

    base_url: str = "https://api.fabric.microsoft.com/v1"
    max_results: int = 100


class FabricApiClient:
    """Client for communicating with the Fabric API"""

    def __init__(self, credential=None, config: FabricApiConfig = None):
        self.credential = credential or DefaultAzureCredential()
        self.config = config or FabricApiConfig()
        # Initialize cached methods
        self._cached_resolve_workspace = lru_cache(maxsize=128)(self._resolve_workspace)
        self._cached_resolve_lakehouse = lru_cache(maxsize=128)(self._resolve_lakehouse)

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Fabric API calls"""
        return {
            "Authorization": f"Bearer {self.credential.get_token('https://api.fabric.microsoft.com/.default').token}"
        }

    async def _make_request(
        self, endpoint: str, params: Optional[Dict] = None, method: str = "GET"
    ) -> Dict[str, Any]:
        """Make an asynchronous call to the Fabric API"""
        # If endpoint is a full URL, use it directly, otherwise add base_url
        url = (
            endpoint
            if endpoint.startswith("http")
            else f"{self.config.base_url}/{endpoint.lstrip('/')}"
        )
        params = params or {}

        if "maxResults" not in params:
            params["maxResults"] = self.config.max_results

        try:
            response = requests.request(
                method=method, url=url, headers=self._get_headers(), params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API call failed: {str(e)}")
            return None

    async def paginated_request(
        self, endpoint: str, params: Optional[Dict] = None, data_key: str = "value"
    ) -> List[Dict]:
        """Make a paginated call to the Fabric API"""
        results = []
        params = params or {}
        continuation_token = None

        while True:
            # Construct full URL with continuation token if available
            url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
            if continuation_token:
                separator = "&" if "?" in url else "?"
                # URL-encode continuation token
                encoded_token = quote(continuation_token)
                url += f"{separator}continuationToken={encoded_token}"

            # Use params without continuation token
            request_params = params.copy()
            if "continuationToken" in request_params:
                del request_params["continuationToken"]

            data = await self._make_request(url, request_params)
            if not data:
                break

            results.extend(data[data_key])

            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break

        return results

    async def get_workspaces(self) -> List[Dict]:
        """Get all available workspaces"""
        return await self.paginated_request("workspaces")

    async def get_lakehouses(self, workspace_id: str) -> List[Dict]:
        """Get all lakehouses in a workspace"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/items", params={"type": "Lakehouse"}
        )

    async def get_tables(self, workspace_id: str, lakehouse_id: str) -> List[Dict]:
        """Get all tables in a lakehouse"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
            data_key="data",
        )

    async def resolve_workspace(self, workspace: str) -> str:
        """Convert workspace name or ID to workspace ID with caching"""
        return await self._cached_resolve_workspace(workspace)

    async def _resolve_workspace(self, workspace: str) -> str:
        """Internal method to convert workspace name or ID to workspace ID"""
        if is_valid_uuid(workspace):
            return workspace

        workspaces = await self.get_workspaces()
        matching_workspaces = [
            w for w in workspaces if w["displayName"].lower() == workspace.lower()
        ]

        if not matching_workspaces:
            raise ValueError(f"No workspaces found with name: {workspace}")
        if len(matching_workspaces) > 1:
            raise ValueError(f"Multiple workspaces found with name: {workspace}")

        return matching_workspaces[0]["id"]

    async def resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Convert lakehouse name or ID to lakehouse ID with caching"""
        return await self._cached_resolve_lakehouse(workspace_id, lakehouse)

    async def _resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Internal method to convert lakehouse name or ID to lakehouse ID"""
        if is_valid_uuid(lakehouse):
            return lakehouse

        lakehouses = await self.get_lakehouses(workspace_id)
        matching_lakehouses = [
            lh for lh in lakehouses if lh["displayName"].lower() == lakehouse.lower()
        ]

        if not matching_lakehouses:
            raise ValueError(f"No lakehouse found with name: {lakehouse}")
        if len(matching_lakehouses) > 1:
            raise ValueError(f"Multiple lakehouses found with name: {lakehouse}")

        return matching_lakehouses[0]["id"]


def is_valid_uuid(value: str) -> bool:
    """Check if a string is a valid UUID"""
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


async def get_delta_schemas(
    tables: List[Dict], credential: DefaultAzureCredential
) -> List[Tuple[Dict, object, object]]:
    """Get schema and metadata for each Delta table"""
    delta_tables = []
    logger.info(f"Starting schema extraction for {len(tables)} tables")

    # Get token for Azure Storage (not Fabric API)
    token = credential.get_token("https://storage.azure.com/.default").token
    storage_options = {"bearer_token": token, "use_fabric_endpoint": "true"}

    for table in tables:
        if table["format"].lower() == "delta":
            try:
                table_path = table["location"]
                logger.debug(f"Processing Delta table: {table['name']} at {table_path}")

                # Create DeltaTable instance with storage options
                delta_table = DeltaTable(table_path, storage_options=storage_options)

                # Get both schema and metadata
                delta_tables.append(
                    (table, delta_table.schema(), delta_table.metadata())
                )
                logger.info(f"Processed table: {table['name']}")

            except Exception as e:
                logger.error(f"Could not process table {table['name']}: {str(e)}")

    return delta_tables


def format_metadata_to_markdown(metadata: object) -> str:
    """Convert Delta table metadata to markdown format"""
    markdown = "### Metadata\n\n"

    markdown += f"**ID:** {metadata.id}\n\n"
    if metadata.name:
        markdown += f"**Name:** {metadata.name}\n\n"
    if metadata.description:
        markdown += f"**Description:** {metadata.description}\n\n"
    if metadata.partition_columns:
        markdown += (
            f"**Partition Columns:** {', '.join(metadata.partition_columns)}\n\n"
        )
    if metadata.created_time:
        created_time = datetime.fromtimestamp(metadata.created_time / 1000)
        markdown += f"**Created:** {created_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    if metadata.configuration:
        markdown += "**Configuration:**\n\n"
        markdown += "```json\n"
        markdown += json.dumps(metadata.configuration, indent=2)
        markdown += "\n```\n"

    return markdown


def format_schema_to_markdown(
    table_info: Dict, schema: object, metadata: object
) -> str:
    """Convert a Delta table schema and metadata to markdown format"""
    markdown = f"## Delta Table: `{table_info['name']}`\n\n"
    markdown += f"**Type:** {table_info['type']}\n\n"
    markdown += f"**Location:** `{table_info['location']}`\n\n"

    # Add schema information
    markdown += "### Schema\n\n"
    markdown += "| Column Name | Data Type | Nullable |\n"
    markdown += "|------------|-----------|----------|\n"

    for field in schema.fields:
        name = field.name
        dtype = field.type
        nullable = field.nullable
        markdown += f"| {name} | {dtype} | {nullable} |\n"

    markdown += "\n"

    # Add metadata information
    markdown += format_metadata_to_markdown(metadata)

    return markdown + "\n"


@mcp.tool()
async def get_table_schema(workspace: str, lakehouse: str, table_name: str) -> str:
    """Get schema for a specific table in a Fabric lakehouse.

    Args:
        workspace: Name or ID of the workspace
        lakehouse: Name or ID of the lakehouse
        table_name: Name of the table to retrieve
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)
        lakehouse_id = await client.resolve_lakehouse(workspace_id, lakehouse)

        # Get all tables
        tables = await client.get_tables(workspace_id, lakehouse_id)

        # Find the specific table
        matching_tables = [t for t in tables if t["name"].lower() == table_name.lower()]

        if not matching_tables:
            return (
                f"No table found with name '{table_name}' in lakehouse '{lakehouse}'."
            )

        table = matching_tables[0]

        # Check that it is a Delta table
        if table["format"].lower() != "delta":
            return f"The table '{table_name}' is not a Delta table (format: {table['format']})."

        # Get schema
        delta_tables = await get_delta_schemas([table], credential)

        if not delta_tables:
            return f"Could not retrieve schema for table '{table_name}'."

        # Format result as markdown
        table_info, schema, metadata = delta_tables[0]
        markdown = format_schema_to_markdown(table_info, schema, metadata)

        return markdown

    except Exception as e:
        return f"Error retrieving table schema: {str(e)}"


@mcp.tool()
async def get_all_schemas(workspace: str, lakehouse: str) -> str:
    """Get schemas for all Delta tables in a Fabric lakehouse.

    Args:
        workspace: Name or ID of the workspace
        lakehouse: Name or ID of the lakehouse
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)
        lakehouse_id = await client.resolve_lakehouse(workspace_id, lakehouse)

        # Get all tables
        tables = await client.get_tables(workspace_id, lakehouse_id)

        if not tables:
            return f"No tables found in lakehouse '{lakehouse}'."

        # Filter to only Delta tables
        delta_format_tables = [t for t in tables if t["format"].lower() == "delta"]

        if not delta_format_tables:
            return f"No Delta tables found in lakehouse '{lakehouse}'."

        # Get schema for all tables
        delta_tables = await get_delta_schemas(delta_format_tables, credential)

        if not delta_tables:
            return "Could not retrieve schemas for any tables."

        # Format the result as markdown
        markdown = f"# Delta Table Schemas\n\n"
        markdown += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        markdown += f"Workspace: {workspace}\n"
        markdown += f"Lakehouse: {lakehouse}\n\n"

        for table_info, schema, metadata in delta_tables:
            markdown += format_schema_to_markdown(table_info, schema, metadata)

        return markdown

    except Exception as e:
        return f"Error retrieving table schemas: {str(e)}"


@mcp.tool()
async def list_workspaces() -> str:
    """List all available Fabric workspaces."""
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        workspaces = await client.get_workspaces()

        if not workspaces:
            return "No workspaces found."

        markdown = "# Fabric Workspaces\n\n"
        markdown += "| ID | Name | Capacity |\n"
        markdown += "|-----|------|----------|\n"

        for ws in workspaces:
            markdown += f"| {ws['id']} | {ws['displayName']} | {ws.get('capacityId', 'N/A')} |\n"

        return markdown

    except Exception as e:
        return f"Error listing workspaces: {str(e)}"


@mcp.tool()
async def list_lakehouses(workspace: str) -> str:
    """List all lakehouses in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert name to ID
        workspace_id = await client.resolve_workspace(workspace)

        lakehouses = await client.get_lakehouses(workspace_id)

        if not lakehouses:
            return f"No lakehouses found in workspace '{workspace}'."

        markdown = f"# Lakehouses in workspace '{workspace}'\n\n"
        markdown += "| ID | Name |\n"
        markdown += "|-----|------|\n"

        for lh in lakehouses:
            markdown += f"| {lh['id']} | {lh['displayName']} |\n"

        return markdown

    except Exception as e:
        return f"Error listing lakehouses: {str(e)}"


@mcp.tool()
async def list_tables(workspace: str, lakehouse: str) -> str:
    """List all tables in a Fabric lakehouse.

    Args:
        workspace: Name or ID of the workspace
        lakehouse: Name or ID of the lakehouse
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)
        lakehouse_id = await client.resolve_lakehouse(workspace_id, lakehouse)

        tables = await client.get_tables(workspace_id, lakehouse_id)

        if not tables:
            return f"No tables found in lakehouse '{lakehouse}'."

        markdown = f"# Tables in lakehouse '{lakehouse}'\n\n"
        markdown += "| Name | Format | Type |\n"
        markdown += "|------|--------|------|\n"

        for table in tables:
            markdown += f"| {table['name']} | {table['format']} | {table['type']} |\n"

        return markdown

    except Exception as e:
        return f"Error listing tables: {str(e)}"


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport="stdio")
