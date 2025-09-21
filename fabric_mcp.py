import json
import logging
import requests
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote

from azure.identity import DefaultAzureCredential
from cachetools import TTLCache
from deltalake import DeltaTable
from mcp.server.fastmcp import FastMCP

# Global caches for name resolution (these never need clearing as name->ID mappings are permanent)
_global_workspace_cache = {}
_global_lakehouse_cache = {}

# Create MCP instance
mcp = FastMCP("fabric_schemas")

# Set up logging with more robust duplicate prevention
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear any existing handlers first
logger.handlers.clear()

# Add single handler
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Prevent propagation to parent loggers to avoid duplicates
logger.propagate = False

# Global shared caches for all FabricApiClient instances
_WORKSPACE_CACHE = TTLCache(maxsize=1, ttl=120)  # 2 min - single workspace list
_CONNECTIONS_CACHE = TTLCache(maxsize=1, ttl=600)  # 10 min - single connections list
_CAPACITIES_CACHE = TTLCache(maxsize=1, ttl=900)  # 15 min - single capacities list
_ITEMS_CACHE = TTLCache(maxsize=50, ttl=300)  # 5 min - items per workspace
_SHORTCUTS_CACHE = TTLCache(maxsize=100, ttl=300)  # 5 min - shortcuts per item
_JOB_INSTANCES_CACHE = TTLCache(maxsize=30, ttl=600)  # 10 min - jobs per workspace/item
_SCHEDULES_CACHE = TTLCache(maxsize=30, ttl=300)  # 5 min - schedules per item/workspace
_ENVIRONMENTS_CACHE = TTLCache(
    maxsize=20, ttl=600
)  # 10 min - environments per workspace


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
        """Get all available workspaces with caching"""
        cache_key = "workspaces"
        if cache_key in _WORKSPACE_CACHE:
            logger.info(
                f"🎯 CACHE HIT: Returning {len(_WORKSPACE_CACHE[cache_key])} workspaces from cache"
            )
            return _WORKSPACE_CACHE[cache_key]

        logger.info("🔄 CACHE MISS: Fetching workspaces from Fabric API")
        workspaces = await self.paginated_request("workspaces")
        _WORKSPACE_CACHE[cache_key] = workspaces
        logger.info(f"💾 CACHE STORE: Cached {len(workspaces)} workspaces")
        return workspaces

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
        # Check global cache first
        if workspace in _global_workspace_cache:
            return _global_workspace_cache[workspace]

        # Resolve and cache the result
        result = await self._resolve_workspace(workspace)
        _global_workspace_cache[workspace] = result
        return result

    async def _resolve_workspace(self, workspace: str) -> str:
        """Internal method to convert workspace name or ID to workspace ID"""
        # If it's already a valid UUID, return it directly
        if is_valid_uuid(workspace):
            return workspace

        # Otherwise, look up by name
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
        # Create cache key combining workspace_id and lakehouse name
        cache_key = f"{workspace_id}:{lakehouse}"

        # Check global cache first
        if cache_key in _global_lakehouse_cache:
            return _global_lakehouse_cache[cache_key]

        # Resolve and cache the result
        result = await self._resolve_lakehouse(workspace_id, lakehouse)
        _global_lakehouse_cache[cache_key] = result
        return result

    async def _resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Internal method to convert lakehouse name or ID to lakehouse ID"""
        if is_valid_uuid(lakehouse):
            # Cache UUID mappings too (workspace_id:UUID -> UUID)
            cache_key = f"{workspace_id}:{lakehouse}"
            _global_lakehouse_cache[cache_key] = lakehouse
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

    async def get_connections(self) -> List[Dict]:
        """Get all connections user has access to with caching"""
        cache_key = "connections"
        if cache_key in _CONNECTIONS_CACHE:
            logger.info(
                f"🎯 CACHE HIT: Returning {len(_CONNECTIONS_CACHE[cache_key])} connections from cache"
            )
            return _CONNECTIONS_CACHE[cache_key]

        logger.info("🔄 CACHE MISS: Fetching connections from Fabric API")
        connections = await self.paginated_request("connections")
        _CONNECTIONS_CACHE[cache_key] = connections
        logger.info(f"💾 CACHE STORE: Cached {len(connections)} connections")
        return connections

    async def get_items(self, workspace_id: str, item_type: str = None) -> List[Dict]:
        """Get all items in workspace, optionally filtered by type with caching"""
        cache_key = f"{workspace_id}:{item_type or 'all'}"
        if cache_key in _ITEMS_CACHE:
            logger.info(
                f"🎯 CACHE HIT: Returning {len(_ITEMS_CACHE[cache_key])} items from cache (key: {cache_key})"
            )
            return _ITEMS_CACHE[cache_key]

        logger.info(f"🔄 CACHE MISS: Fetching items from Fabric API (key: {cache_key})")
        params = {"type": item_type} if item_type else {}
        items = await self.paginated_request(
            f"workspaces/{workspace_id}/items", params=params
        )
        _ITEMS_CACHE[cache_key] = items
        logger.info(f"💾 CACHE STORE: Cached {len(items)} items (key: {cache_key})")
        return items

    async def get_item(self, workspace_id: str, item_id: str) -> Dict:
        """Get specific item details"""
        return await self._make_request(f"workspaces/{workspace_id}/items/{item_id}")

    async def get_workspace_details(self, workspace_id: str) -> Dict:
        """Get detailed workspace information"""
        return await self._make_request(f"workspaces/{workspace_id}")

    async def get_capacities(self) -> List[Dict]:
        """Get all capacities user has access to with caching"""
        cache_key = "capacities"
        if cache_key in _CAPACITIES_CACHE:
            return _CAPACITIES_CACHE[cache_key]

        capacities = await self.paginated_request("capacities")
        _CAPACITIES_CACHE[cache_key] = capacities
        return capacities

    async def get_job_instances(
        self, workspace_id: str, item_id: str = None, status: str = None
    ) -> List[Dict]:
        """Get job instances, optionally filtered by item and status"""
        # Create cache key based on parameters
        cache_key = f"{workspace_id}:{item_id or 'all'}:{status or 'all'}"
        if cache_key in _JOB_INSTANCES_CACHE:
            return _JOB_INSTANCES_CACHE[cache_key]

        if not item_id:
            # Get all items and collect their job instances
            items = await self.get_items(workspace_id)
            all_jobs = []
            for item in items:
                try:
                    jobs = await self.paginated_request(
                        f"workspaces/{workspace_id}/items/{item['id']}/jobs/instances"
                    )
                    for job in jobs:
                        job["itemName"] = item["displayName"]
                        job["itemType"] = item["type"]
                        if (
                            not status
                            or job.get("status", "").lower() == status.lower()
                        ):
                            all_jobs.append(job)
                except Exception:
                    continue

            # Cache the results
            _JOB_INSTANCES_CACHE[cache_key] = all_jobs
            return all_jobs
        else:
            jobs = await self.paginated_request(
                f"workspaces/{workspace_id}/items/{item_id}/jobs/instances"
            )
            if status:
                jobs = [
                    job
                    for job in jobs
                    if job.get("status", "").lower() == status.lower()
                ]

            # Cache the results
            _JOB_INSTANCES_CACHE[cache_key] = jobs
            return jobs

    async def get_job_instance(
        self, workspace_id: str, item_id: str, job_instance_id: str
    ) -> Dict:
        """Get specific job instance details"""
        return await self._make_request(
            f"workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}"
        )

    async def get_item_schedules(self, workspace_id: str, item_id: str) -> List[Dict]:
        """Get all schedules for a specific item"""
        # Create cache key for item schedules
        cache_key = f"item:{workspace_id}:{item_id}"
        if cache_key in _SCHEDULES_CACHE:
            return _SCHEDULES_CACHE[cache_key]

        schedules = await self.paginated_request(
            f"workspaces/{workspace_id}/items/{item_id}/schedules"
        )

        # Cache the results
        _SCHEDULES_CACHE[cache_key] = schedules
        return schedules

    async def get_workspace_schedules(self, workspace_id: str) -> List[Dict]:
        """Get all schedules across all items in workspace"""
        # Create cache key for workspace schedules
        cache_key = f"workspace:{workspace_id}"
        if cache_key in _SCHEDULES_CACHE:
            return _SCHEDULES_CACHE[cache_key]

        items = await self.get_items(workspace_id)
        all_schedules = []

        for item in items:
            try:
                schedules = await self.paginated_request(
                    f"workspaces/{workspace_id}/items/{item['id']}/schedules"
                )
                for schedule in schedules:
                    schedule["itemName"] = item["displayName"]
                    schedule["itemType"] = item["type"]
                    schedule["itemId"] = item["id"]
                    all_schedules.append(schedule)
            except Exception:
                continue

        # Cache the results
        _SCHEDULES_CACHE[cache_key] = all_schedules
        return all_schedules

    async def get_environments(self, workspace_id: str = None) -> List[Dict]:
        """Get environments, optionally filtered by workspace"""
        # Create cache key based on workspace filter
        cache_key = f"environments:{workspace_id or 'all'}"
        if cache_key in _ENVIRONMENTS_CACHE:
            return _ENVIRONMENTS_CACHE[cache_key]

        if workspace_id:
            environments = await self.paginated_request(
                f"workspaces/{workspace_id}/items", params={"type": "Environment"}
            )
        else:
            # Get all accessible workspaces and their environments
            workspaces = await self.get_workspaces()
            environments = []

            for ws in workspaces:
                try:
                    ws_environments = await self.paginated_request(
                        f"workspaces/{ws['id']}/items", params={"type": "Environment"}
                    )
                    for env in ws_environments:
                        env["workspaceName"] = ws["displayName"]
                        environments.append(env)
                except Exception:
                    continue

        # Cache the results
        _ENVIRONMENTS_CACHE[cache_key] = environments
        return environments

    async def get_environment_details(
        self, workspace_id: str, environment_id: str
    ) -> Dict:
        """Get detailed environment configuration"""
        try:
            environment = await self._make_request(
                f"workspaces/{workspace_id}/items/{environment_id}"
            )
            sparkcompute = await self._make_request(
                f"workspaces/{workspace_id}/environments/{environment_id}/sparkcompute"
            )
            libraries = await self._make_request(
                f"workspaces/{workspace_id}/environments/{environment_id}/libraries"
            )
            return {
                "environment": environment,
                "sparkcompute": sparkcompute,
                "libraries": libraries,
            }
        except Exception:
            # Return partial data if some calls fail
            environment = await self._make_request(
                f"workspaces/{workspace_id}/items/{environment_id}"
            )
            return {"environment": environment, "sparkcompute": None, "libraries": None}

    async def get_shortcuts(
        self, workspace_id: str, item_id: str, parent_path: str = None
    ) -> List[Dict]:
        """Get OneLake shortcuts for a specific item"""
        # Create cache key based on parameters
        cache_key = f"{workspace_id}:{item_id}:{parent_path or 'root'}"
        if cache_key in _SHORTCUTS_CACHE:
            return _SHORTCUTS_CACHE[cache_key]

        endpoint = f"workspaces/{workspace_id}/items/{item_id}/shortcuts"
        params = {"path": parent_path} if parent_path else {}
        shortcuts_response = await self._make_request(endpoint, params)
        shortcuts = (
            shortcuts_response.get("shortcuts", []) if shortcuts_response else []
        )

        # Cache the results
        _SHORTCUTS_CACHE[cache_key] = shortcuts
        return shortcuts

    async def get_shortcut(
        self,
        workspace_id: str,
        item_id: str,
        shortcut_name: str,
        parent_path: str = None,
    ) -> Dict:
        """Get specific shortcut details"""
        path_segment = f"/{parent_path.strip('/')}" if parent_path else ""
        endpoint = f"workspaces/{workspace_id}/items/{item_id}/shortcuts/{shortcut_name}{path_segment}"
        return await self._make_request(endpoint)

    async def get_workspace_shortcuts(self, workspace_id: str) -> List[Dict]:
        """Get all shortcuts across all items in workspace"""
        # Create cache key for workspace shortcuts
        cache_key = f"workspace:{workspace_id}"
        if cache_key in _SHORTCUTS_CACHE:
            logger.info(
                f"🎯 CACHE HIT: Returning {len(_SHORTCUTS_CACHE[cache_key])} workspace shortcuts from cache (key: {cache_key})"
            )
            return _SHORTCUTS_CACHE[cache_key]

        logger.info(
            f"🔄 CACHE MISS: Fetching workspace shortcuts from Fabric API (key: {cache_key})"
        )
        items = await self.get_items(workspace_id)
        all_shortcuts = []

        for item in items:
            if item["type"] in ["Lakehouse", "KqlDatabase"]:
                try:
                    shortcuts_response = await self._make_request(
                        f"workspaces/{workspace_id}/items/{item['id']}/shortcuts"
                    )
                    shortcuts = (
                        shortcuts_response.get("shortcuts", [])
                        if shortcuts_response
                        else []
                    )
                    for shortcut in shortcuts:
                        shortcut["itemName"] = item["displayName"]
                        shortcut["itemType"] = item["type"]
                        all_shortcuts.append(shortcut)
                except Exception:
                    continue

        # Cache the results
        _SHORTCUTS_CACHE[cache_key] = all_shortcuts
        logger.info(
            f"💾 CACHE STORE: Cached {len(all_shortcuts)} workspace shortcuts (key: {cache_key})"
        )
        return all_shortcuts


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


@mcp.tool()
async def list_connections() -> str:
    """List all connections the user has permission for across the entire Fabric tenant (READ-ONLY).

    ⚠️  SECURITY LIMITATION: This only returns connections the authenticated user/service principal
    has permission for. To get ALL tenant connections, you need:
    1. Service Principal with broader workspace access, OR
    2. Admin-level API access (if available), OR
    3. Aggregate results from multiple users

    This returns ALL connections the user can access, not limited to any specific workspace.
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Get connections from the connections endpoint (limited to user's permissions)
        connections = await client.get_connections()

        if not connections:
            return "No connections found for the current user/service principal."

        markdown = "# Fabric Connections (User-Scoped)\n\n"
        markdown += f"**Total Connections Accessible:** {len(connections)}\n\n"

        # Add security warning
        markdown += "⚠️ **Security Note**: This list only includes connections you have permission to access.\n"
        markdown += "To get ALL tenant connections, consider using a Service Principal with broader permissions.\n\n"

        markdown += "| ID | Display Name | Type | Connectivity Type | Privacy Level |\n"
        markdown += "|-----|--------------|------|------------------|---------------|\n"

        for conn in connections:
            conn_type = conn.get("connectionDetails", {}).get("type", "N/A")
            connectivity_type = conn.get("connectivityType", "N/A")
            privacy_level = conn.get("privacyLevel", "N/A")
            markdown += f"| {conn['id']} | {conn['displayName']} | {conn_type} | {connectivity_type} | {privacy_level} |\n"

        markdown += "\n## Getting More Connections\n\n"
        markdown += "To access more connections across the tenant:\n\n"
        markdown += "1. **Service Principal Approach**: Use a Service Principal with access to more workspaces\n"
        markdown += "2. **Multi-User Aggregation**: Run this tool with different user credentials and combine results\n"
        markdown += "3. **Admin Access**: Check if your organization has Admin APIs enabled for broader access\n"

        return markdown

    except Exception as e:
        return f"Error listing connections: {str(e)}"


@mcp.tool()
async def list_items(workspace: str, item_type: str = None) -> str:
    """List all items in a Fabric workspace (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
        item_type: Optional filter by item type (e.g., 'Lakehouse', 'Notebook', 'DataPipeline')
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert name to ID
        workspace_id = await client.resolve_workspace(workspace)

        # Get items with optional type filter
        items = await client.get_items(workspace_id, item_type)

        if not items:
            return f"No items found in workspace '{workspace}'."

        markdown = f"# Items in workspace '{workspace}'\n\n"
        if item_type:
            markdown += f"Filtered by type: **{item_type}**\n\n"

        markdown += "| ID | Display Name | Type | Description |\n"
        markdown += "|-----|--------------|------|-------------|\n"

        for item in items:
            description = item.get("description", "N/A")
            markdown += f"| {item['id']} | {item['displayName']} | {item['type']} | {description} |\n"

        return markdown

    except Exception as e:
        return f"Error listing items: {str(e)}"


@mcp.tool()
async def get_item(workspace: str, item_id: str) -> str:
    """Get details of a specific Fabric item (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
        item_id: ID of the item to retrieve
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert name to ID
        workspace_id = await client.resolve_workspace(workspace)

        # Get item details
        item = await client.get_item(workspace_id, item_id)

        if not item:
            return f"Item '{item_id}' not found in workspace '{workspace}'."

        markdown = f"# Item Details\n\n"
        markdown += f"**ID:** {item['id']}\n\n"
        markdown += f"**Display Name:** {item['displayName']}\n\n"
        markdown += f"**Type:** {item['type']}\n\n"
        markdown += f"**Workspace ID:** {item['workspaceId']}\n\n"

        if item.get("description"):
            markdown += f"**Description:** {item['description']}\n\n"

        return markdown

    except Exception as e:
        return f"Error getting item details: {str(e)}"


@mcp.tool()
async def get_workspace(workspace: str) -> str:
    """Get details of a specific Fabric workspace (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert name to ID
        workspace_id = await client.resolve_workspace(workspace)

        # Get workspace details
        workspace_details = await client._make_request(f"workspaces/{workspace_id}")

        if not workspace_details:
            return f"Workspace '{workspace}' not found."

        markdown = f"# Workspace Details\n\n"
        markdown += f"**ID:** {workspace_details['id']}\n\n"
        markdown += f"**Display Name:** {workspace_details['displayName']}\n\n"
        markdown += f"**Type:** {workspace_details.get('type', 'N/A')}\n\n"

        if workspace_details.get("description"):
            markdown += f"**Description:** {workspace_details['description']}\n\n"

        if workspace_details.get("capacityId"):
            markdown += f"**Capacity ID:** {workspace_details['capacityId']}\n\n"

        return markdown

    except Exception as e:
        return f"Error getting workspace details: {str(e)}"


@mcp.tool()
async def list_capacities() -> str:
    """List all Fabric capacities the user has access to (READ-ONLY)."""
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Get capacities
        capacities = await client.get_capacities()

        if not capacities:
            return "No capacities found."

        markdown = "# Fabric Capacities\n\n"
        markdown += "| ID | Display Name | SKU | Region | State |\n"
        markdown += "|-----|--------------|-----|--------|-------|\n"

        for capacity in capacities:
            sku = capacity.get("sku", "N/A")
            region = capacity.get("region", "N/A")
            state = capacity.get("state", "N/A")
            markdown += f"| {capacity['id']} | {capacity['displayName']} | {sku} | {region} | {state} |\n"

        return markdown

    except Exception as e:
        return f"Error listing capacities: {str(e)}"


@mcp.tool()
async def list_workspaces_with_identity() -> str:
    """List workspaces that have workspace identities configured (READ-ONLY).

    This identifies which workspaces have workspace identities for secure authentication.
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Get all workspaces
        workspaces = await client.get_workspaces()

        if not workspaces:
            return "No workspaces found."

        workspaces_with_identity = []

        # Check each workspace for workspace identity
        for workspace in workspaces:
            try:
                # Try to get workspace details which might include identity info
                workspace_details = await client._make_request(
                    f"workspaces/{workspace['id']}"
                )

                # Check if workspace has identity-related properties
                # Note: The exact property name may vary based on API response
                has_identity = False
                identity_info = {}

                # Check for various possible identity indicators
                if workspace_details:
                    # Look for identity-related fields
                    if (
                        workspace_details.get("hasWorkspaceIdentity")
                        or workspace_details.get("workspaceIdentity")
                        or workspace_details.get("identityId")
                    ):
                        has_identity = True
                        identity_info = {
                            "hasIdentity": workspace_details.get(
                                "hasWorkspaceIdentity", True
                            ),
                            "identityId": workspace_details.get("identityId", "N/A"),
                            "identityState": workspace_details.get(
                                "identityState", "N/A"
                            ),
                        }

                if has_identity:
                    workspaces_with_identity.append(
                        {"workspace": workspace, "identity": identity_info}
                    )

            except Exception as e:
                # Skip workspaces we can't access
                logger.debug(
                    f"Could not check identity for workspace {workspace['displayName']}: {str(e)}"
                )
                continue

        if not workspaces_with_identity:
            return "No workspaces with workspace identities found (or user lacks permission to view identity details)."

        markdown = "# Workspaces with Workspace Identities\n\n"
        markdown += "| Workspace ID | Workspace Name | Capacity | Identity State |\n"
        markdown += "|--------------|----------------|----------|----------------|\n"

        for item in workspaces_with_identity:
            workspace = item["workspace"]
            identity = item["identity"]
            capacity = workspace.get("capacityId", "N/A")
            identity_state = identity.get("identityState", "Active")

            markdown += f"| {workspace['id']} | {workspace['displayName']} | {capacity} | {identity_state} |\n"

        markdown += (
            f"\n**Total workspaces with identities:** {len(workspaces_with_identity)}\n"
        )
        markdown += "\n*Note: This tool identifies workspaces with workspace identities based on available API data. Some identity details may require admin permissions.*\n"

        return markdown

    except Exception as e:
        return f"Error listing workspaces with identities: {str(e)}"


@mcp.tool()
async def get_workspace_identity(workspace: str) -> str:
    """Get workspace identity details for a specific workspace (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert name to ID
        workspace_id = await client.resolve_workspace(workspace)

        # Get workspace details
        workspace_details = await client._make_request(f"workspaces/{workspace_id}")

        if not workspace_details:
            return f"Workspace '{workspace}' not found."

        markdown = f"# Workspace Identity Details for '{workspace}'\n\n"
        markdown += f"**Workspace ID:** {workspace_details['id']}\n\n"
        markdown += f"**Workspace Name:** {workspace_details['displayName']}\n\n"

        # Check for workspace identity information
        has_identity = (
            workspace_details.get("hasWorkspaceIdentity")
            or workspace_details.get("workspaceIdentity")
            or workspace_details.get("identityId")
        )

        if has_identity:
            markdown += "## ✅ Workspace Identity Configured\n\n"

            if workspace_details.get("identityId"):
                markdown += f"**Identity ID:** {workspace_details['identityId']}\n\n"

            if workspace_details.get("identityState"):
                markdown += (
                    f"**Identity State:** {workspace_details['identityState']}\n\n"
                )

            if workspace_details.get("workspaceIdentity"):
                identity = workspace_details["workspaceIdentity"]
                if isinstance(identity, dict):
                    markdown += "**Identity Details:**\n\n"
                    for key, value in identity.items():
                        markdown += f"- **{key}:** {value}\n"
                    markdown += "\n"
        else:
            markdown += "## ❌ No Workspace Identity Configured\n\n"
            markdown += (
                "This workspace does not have a workspace identity configured.\n\n"
            )

        # Add information about workspace identity benefits
        markdown += "## About Workspace Identity\n\n"
        markdown += "Workspace identity provides:\n"
        markdown += "- Secure authentication without managing credentials\n"
        markdown += "- Trusted workspace access to firewall-enabled storage accounts\n"
        markdown += "- Integration with Microsoft Entra ID\n"
        markdown += (
            "- Support for OneLake shortcuts, data pipelines, and semantic models\n"
        )

        return markdown

    except Exception as e:
        return f"Error getting workspace identity details: {str(e)}"


@mcp.tool()
async def list_shortcuts(
    workspace: str, item_name: str, parent_path: str = None
) -> str:
    """List all OneLake shortcuts in a specific Fabric item (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
        item_name: Name or ID of the item (Lakehouse, KQL Database, etc.)
        parent_path: Optional parent path to filter shortcuts (e.g., 'Files', 'Tables')
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items to find the specified item
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")

        # Find the target item
        target_item = None
        for item in items:
            if (
                item["displayName"].lower() == item_name.lower()
                or item["id"] == item_name
            ):
                target_item = item
                break

        if not target_item:
            return f"Item '{item_name}' not found in workspace '{workspace}'."

        # Build shortcuts endpoint
        endpoint = f"workspaces/{workspace_id}/items/{target_item['id']}/shortcuts"
        params = {}
        if parent_path:
            params["parentPath"] = parent_path

        # Get shortcuts
        shortcuts_response = await client._make_request(endpoint, params)

        if not shortcuts_response or not shortcuts_response.get("value"):
            return f"No shortcuts found in item '{item_name}'."

        shortcuts = shortcuts_response["value"]

        markdown = f"# OneLake Shortcuts in '{item_name}'\n\n"
        markdown += f"**Workspace:** {workspace}\n"
        markdown += f"**Item Type:** {target_item['type']}\n"
        if parent_path:
            markdown += f"**Parent Path:** {parent_path}\n"
        markdown += f"**Total Shortcuts:** {len(shortcuts)}\n\n"

        markdown += "| Name | Path | Target Type | Target Details |\n"
        markdown += "|------|------|-------------|----------------|\n"

        for shortcut in shortcuts:
            name = shortcut["name"]
            path = shortcut["path"]
            target = shortcut["target"]
            target_type = target["type"]

            # Build target details based on type
            target_details = "N/A"
            if target_type == "OneLake" and target.get("oneLake"):
                onelake = target["oneLake"]
                target_details = f"Workspace: {onelake.get('workspaceId', 'N/A')}, Item: {onelake.get('itemId', 'N/A')}"
            elif target_type == "AmazonS3" and target.get("amazonS3"):
                s3 = target["amazonS3"]
                target_details = f"Location: {s3.get('location', 'N/A')}"
            elif target_type == "AdlsGen2" and target.get("adlsGen2"):
                adls = target["adlsGen2"]
                target_details = f"Location: {adls.get('location', 'N/A')}"
            elif target_type == "GoogleCloudStorage" and target.get(
                "googleCloudStorage"
            ):
                gcs = target["googleCloudStorage"]
                target_details = f"Location: {gcs.get('location', 'N/A')}"
            elif target_type == "AzureBlobStorage" and target.get("azureBlobStorage"):
                blob = target["azureBlobStorage"]
                target_details = f"Location: {blob.get('location', 'N/A')}"

            markdown += f"| {name} | {path} | {target_type} | {target_details} |\n"

        # Add information about shortcut types
        markdown += "\n## About OneLake Shortcuts\n\n"
        markdown += "OneLake shortcuts provide references to data stored in:\n"
        markdown += "- **OneLake**: Other Fabric items (Lakehouses, KQL Databases)\n"
        markdown += "- **External Storage**: Amazon S3, Azure Data Lake Gen2, Google Cloud Storage, etc.\n"
        markdown += "- **Shortcuts appear as folders** and can be accessed by Spark, SQL, and other services\n"

        return markdown

    except Exception as e:
        return f"Error listing shortcuts: {str(e)}"


@mcp.tool()
async def get_shortcut(
    workspace: str, item_name: str, shortcut_path: str, shortcut_name: str
) -> str:
    """Get detailed information about a specific OneLake shortcut (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
        item_name: Name or ID of the item containing the shortcut
        shortcut_path: Path where the shortcut is located (e.g., 'Files', 'Tables/subfolder')
        shortcut_name: Name of the shortcut
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items to find the specified item
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")

        # Find the target item
        target_item = None
        for item in items:
            if (
                item["displayName"].lower() == item_name.lower()
                or item["id"] == item_name
            ):
                target_item = item
                break

        if not target_item:
            return f"Item '{item_name}' not found in workspace '{workspace}'."

        # Get shortcut details
        endpoint = f"workspaces/{workspace_id}/items/{target_item['id']}/shortcuts/{shortcut_path}/{shortcut_name}"
        shortcut = await client._make_request(endpoint)

        if not shortcut:
            return f"Shortcut '{shortcut_name}' not found at path '{shortcut_path}' in item '{item_name}'."

        markdown = f"# OneLake Shortcut Details\n\n"
        markdown += f"**Shortcut Name:** {shortcut['name']}\n\n"
        markdown += f"**Path:** {shortcut['path']}\n\n"
        markdown += f"**Workspace:** {workspace}\n\n"
        markdown += f"**Item:** {item_name} ({target_item['type']})\n\n"

        # Target information
        target = shortcut["target"]
        target_type = target["type"]
        markdown += f"**Target Type:** {target_type}\n\n"

        markdown += "## Target Configuration\n\n"

        if target_type == "OneLake" and target.get("oneLake"):
            onelake = target["oneLake"]
            markdown += (
                f"**Target Workspace ID:** {onelake.get('workspaceId', 'N/A')}\n\n"
            )
            markdown += f"**Target Item ID:** {onelake.get('itemId', 'N/A')}\n\n"
            markdown += f"**Target Path:** {onelake.get('path', 'N/A')}\n\n"
            if onelake.get("connectionId"):
                markdown += f"**Connection ID:** {onelake['connectionId']}\n\n"

        elif target_type == "AmazonS3" and target.get("amazonS3"):
            s3 = target["amazonS3"]
            markdown += f"**S3 Location:** {s3.get('location', 'N/A')}\n\n"
            markdown += f"**Subpath:** {s3.get('subpath', 'N/A')}\n\n"
            markdown += f"**Connection ID:** {s3.get('connectionId', 'N/A')}\n\n"

        elif target_type == "AdlsGen2" and target.get("adlsGen2"):
            adls = target["adlsGen2"]
            markdown += f"**ADLS Location:** {adls.get('location', 'N/A')}\n\n"
            markdown += f"**Subpath:** {adls.get('subpath', 'N/A')}\n\n"
            markdown += f"**Connection ID:** {adls.get('connectionId', 'N/A')}\n\n"

        elif target_type == "GoogleCloudStorage" and target.get("googleCloudStorage"):
            gcs = target["googleCloudStorage"]
            markdown += f"**GCS Location:** {gcs.get('location', 'N/A')}\n\n"
            markdown += f"**Subpath:** {gcs.get('subpath', 'N/A')}\n\n"
            markdown += f"**Connection ID:** {gcs.get('connectionId', 'N/A')}\n\n"

        elif target_type == "AzureBlobStorage" and target.get("azureBlobStorage"):
            blob = target["azureBlobStorage"]
            markdown += f"**Blob Storage Location:** {blob.get('location', 'N/A')}\n\n"
            markdown += f"**Subpath:** {blob.get('subpath', 'N/A')}\n\n"
            markdown += f"**Connection ID:** {blob.get('connectionId', 'N/A')}\n\n"

        elif target_type == "Dataverse" and target.get("dataverse"):
            dv = target["dataverse"]
            markdown += (
                f"**Environment Domain:** {dv.get('environmentDomain', 'N/A')}\n\n"
            )
            markdown += f"**Table Name:** {dv.get('tableName', 'N/A')}\n\n"
            markdown += f"**Delta Lake Folder:** {dv.get('deltaLakeFolder', 'N/A')}\n\n"
            markdown += f"**Connection ID:** {dv.get('connectionId', 'N/A')}\n\n"

        # Transform information (if any)
        if shortcut.get("transform"):
            transform = shortcut["transform"]
            markdown += "## Transform Configuration\n\n"
            markdown += f"**Transform Type:** {transform.get('type', 'N/A')}\n\n"

            if transform.get("properties"):
                props = transform["properties"]
                markdown += "**Transform Properties:**\n\n"
                for key, value in props.items():
                    markdown += f"- **{key}:** {value}\n"
                markdown += "\n"

        # Access information
        markdown += "## Access Information\n\n"
        markdown += "This shortcut can be accessed through:\n"
        markdown += "- **Apache Spark**: Use relative paths or SQL syntax\n"
        markdown += (
            "- **SQL Analytics Endpoint**: Query through T-SQL (if in Tables folder)\n"
        )
        markdown += "- **OneLake API**: Direct file access via REST API\n"
        markdown += "- **External Tools**: Any tool supporting ADLS Gen2 APIs\n"

        return markdown

    except Exception as e:
        return f"Error getting shortcut details: {str(e)}"


@mcp.tool()
async def list_workspace_shortcuts(workspace: str) -> str:
    """List all OneLake shortcuts across all items in a workspace (READ-ONLY).

    This aggregates shortcuts from all Lakehouses and KQL Databases in the workspace.

    Args:
        workspace: Name or ID of the workspace
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert name to ID
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items in workspace that can contain shortcuts
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")

        # Filter to items that can contain shortcuts (Lakehouse, KQLDatabase)
        shortcut_items = [
            item for item in items if item["type"] in ["Lakehouse", "KQLDatabase"]
        ]

        if not shortcut_items:
            return (
                f"No items that can contain shortcuts found in workspace '{workspace}'."
            )

        all_shortcuts = []
        total_shortcuts = 0

        # Get shortcuts from each item
        for item in shortcut_items:
            try:
                endpoint = f"workspaces/{workspace_id}/items/{item['id']}/shortcuts"
                shortcuts_response = await client._make_request(endpoint)

                if shortcuts_response and shortcuts_response.get("value"):
                    item_shortcuts = shortcuts_response["value"]
                    total_shortcuts += len(item_shortcuts)

                    for shortcut in item_shortcuts:
                        shortcut["_item_name"] = item["displayName"]
                        shortcut["_item_type"] = item["type"]
                        shortcut["_item_id"] = item["id"]

                    all_shortcuts.extend(item_shortcuts)

            except Exception as e:
                # Skip items we can't access
                logger.debug(
                    f"Could not get shortcuts for item {item['displayName']}: {str(e)}"
                )
                continue

        if not all_shortcuts:
            return f"No shortcuts found in any items in workspace '{workspace}'."

        markdown = f"# OneLake Shortcuts in Workspace '{workspace}'\n\n"
        markdown += f"**Total Items with Shortcuts:** {len([item for item in shortcut_items if any(s.get('_item_id') == item['id'] for s in all_shortcuts)])}\n"
        markdown += f"**Total Shortcuts:** {total_shortcuts}\n\n"

        # Group shortcuts by item
        from collections import defaultdict

        shortcuts_by_item = defaultdict(list)
        for shortcut in all_shortcuts:
            item_name = shortcut.get("_item_name", "Unknown")
            shortcuts_by_item[item_name].append(shortcut)

        for item_name, shortcuts in shortcuts_by_item.items():
            item_type = shortcuts[0].get("_item_type", "Unknown")
            markdown += f"## {item_name} ({item_type})\n\n"
            markdown += f"**Shortcuts in this item:** {len(shortcuts)}\n\n"

            markdown += "| Name | Path | Target Type | Target Details |\n"
            markdown += "|------|------|-------------|----------------|\n"

            for shortcut in shortcuts:
                name = shortcut["name"]
                path = shortcut["path"]
                target = shortcut["target"]
                target_type = target["type"]

                # Build target details based on type
                target_details = "N/A"
                if target_type == "OneLake" and target.get("oneLake"):
                    onelake = target["oneLake"]
                    target_details = f"Item: {onelake.get('itemId', 'N/A')[:8]}..."
                elif target_type in [
                    "AmazonS3",
                    "AdlsGen2",
                    "GoogleCloudStorage",
                    "AzureBlobStorage",
                ]:
                    # Get location from the appropriate target type
                    target_obj = target.get(target_type.lower()) or target.get(
                        target_type
                    )
                    if target_obj and target_obj.get("location"):
                        target_details = (
                            target_obj["location"][:50] + "..."
                            if len(target_obj["location"]) > 50
                            else target_obj["location"]
                        )

                markdown += f"| {name} | {path} | {target_type} | {target_details} |\n"

            markdown += "\n"

        # Summary by target type
        target_types = defaultdict(int)
        for shortcut in all_shortcuts:
            target_types[shortcut["target"]["type"]] += 1

        markdown += "## Summary by Target Type\n\n"
        for target_type, count in sorted(target_types.items()):
            markdown += f"- **{target_type}**: {count} shortcuts\n"

        markdown += "\n*Note: This aggregates shortcuts from all Lakehouses and KQL Databases in the workspace.*\n"

        return markdown

    except Exception as e:
        return f"Error listing workspace shortcuts: {str(e)}"


@mcp.tool()
async def list_job_instances(
    workspace: str, item_name: str = None, status: str = None
) -> str:
    """List all job instances for items in a workspace (READ-ONLY).

    Shows what's running, queued, completed, or failed - perfect for monitoring!

    Args:
        workspace: Name or ID of the workspace
        item_name: Optional item name to filter jobs for specific item
        status: Optional status filter (NotStarted, InProgress, Completed, Failed, Cancelled)
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Resolve item_name to item_id if provided
        item_id = None
        if item_name:
            items = await client.get_items(workspace_id)
            matching_items = [
                item
                for item in items
                if item_name.lower() in item["displayName"].lower()
            ]
            if not matching_items:
                return (
                    f"No items matching '{item_name}' found in workspace '{workspace}'."
                )
            item_id = matching_items[0]["id"]

        # Get job instances using client method
        all_jobs = await client.get_job_instances(workspace_id, item_id, status)

        if not all_jobs:
            return f"No job instances found in workspace '{workspace}'."

        # Sort by start time (most recent first)
        all_jobs.sort(key=lambda x: x.get("startTimeUtc", ""), reverse=True)

        markdown = f"# Job Instances in '{workspace}'\n\n"
        if status:
            markdown += f"**Status Filter**: {status}\n\n"
        if item_name:
            markdown += f"**Item Filter**: {item_name}\n\n"

        markdown += "| Item | Type | Job Type | Status | Invoke Type | Start Time | Duration |\n"
        markdown += "|------|------|----------|--------|-------------|------------|----------|\n"

        for job in all_jobs[:50]:  # Limit to 50 most recent
            start_time = job.get("startTimeUtc", "N/A")
            end_time = job.get("endTimeUtc", "")
            duration = (
                "Running"
                if job.get("status") == "InProgress"
                else ("N/A" if not end_time else f"{end_time}")
            )

            markdown += f"| {job['itemName']} | {job['itemType']} | {job.get('jobType', 'N/A')} | "
            markdown += (
                f"{job.get('status', 'N/A')} | {job.get('invokeType', 'N/A')} | "
            )
            markdown += f"{start_time} | {duration} |\n"

        if len(all_jobs) > 50:
            markdown += (
                f"\n*Showing 50 most recent jobs out of {len(all_jobs)} total.*\n"
            )

        return markdown

    except Exception as e:
        return f"Error listing job instances: {str(e)}"


@mcp.tool()
async def get_job_instance(workspace: str, item_name: str, job_instance_id: str) -> str:
    """Get detailed information about a specific job instance (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
        item_name: Name or ID of the item
        job_instance_id: ID of the job instance
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items to find the specified item
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")
        item = None
        for i in items:
            if item_name.lower() in i["displayName"].lower() or i["id"] == item_name:
                item = i
                break

        if not item:
            return f"Item '{item_name}' not found in workspace '{workspace}'."

        # Get job instance details
        job = await client._make_request(
            f"workspaces/{workspace_id}/items/{item['id']}/jobs/instances/{job_instance_id}"
        )

        markdown = f"# Job Instance Details\n\n"
        markdown += f"**Item**: {item['displayName']} ({item['type']})\n"
        markdown += f"**Workspace**: {workspace}\n\n"

        markdown += "## Job Information\n"
        markdown += f"- **Job ID**: {job.get('id', 'N/A')}\n"
        markdown += f"- **Job Type**: {job.get('jobType', 'N/A')}\n"
        markdown += f"- **Status**: {job.get('status', 'N/A')}\n"
        markdown += f"- **Invoke Type**: {job.get('invokeType', 'N/A')}\n"

        markdown += "\n## Timing\n"
        markdown += f"- **Start Time**: {job.get('startTimeUtc', 'N/A')}\n"
        markdown += f"- **End Time**: {job.get('endTimeUtc', 'N/A')}\n"

        if job.get("failureReason"):
            markdown += f"\n## Error Details\n"
            markdown += f"- **Failure Reason**: {job['failureReason']}\n"

        if job.get("rootActivityId"):
            markdown += f"\n## Technical Details\n"
            markdown += f"- **Root Activity ID**: {job['rootActivityId']}\n"

        return markdown

    except Exception as e:
        return f"Error getting job instance: {str(e)}"


@mcp.tool()
async def list_item_schedules(workspace: str, item_name: str) -> str:
    """List all schedules for a specific item - see what's scheduled to run! (READ-ONLY)

    Args:
        workspace: Name or ID of the workspace
        item_name: Name or ID of the item to check schedules for
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items to find the specified item
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")
        item = None
        for i in items:
            if item_name.lower() in i["displayName"].lower() or i["id"] == item_name:
                item = i
                break

        if not item:
            return f"Item '{item_name}' not found in workspace '{workspace}'."

        # Get schedules for the item
        schedules = await client.paginated_request(
            f"workspaces/{workspace_id}/items/{item['id']}/schedules"
        )

        if not schedules:
            return f"No schedules found for item '{item['displayName']}'."

        markdown = f"# Schedules for '{item['displayName']}'\n\n"
        markdown += f"**Item Type**: {item['type']}\n"
        markdown += f"**Workspace**: {workspace}\n\n"

        markdown += "| Schedule ID | Job Type | Enabled | Frequency | Start Date | End Date | Timezone |\n"
        markdown += "|-------------|----------|---------|-----------|------------|----------|----------|\n"

        for schedule in schedules:
            enabled = "✅ Yes" if schedule.get("enabled", False) else "❌ No"
            frequency = schedule.get("recurrence", {}).get("frequency", "N/A")
            start_date = schedule.get("recurrence", {}).get("startDate", "N/A")
            end_date = schedule.get("recurrence", {}).get("endDate", "N/A")
            timezone = schedule.get("recurrence", {}).get("timeZone", "N/A")

            markdown += (
                f"| {schedule.get('id', 'N/A')} | {schedule.get('jobType', 'N/A')} | "
            )
            markdown += (
                f"{enabled} | {frequency} | {start_date} | {end_date} | {timezone} |\n"
            )

        return markdown

    except Exception as e:
        return f"Error listing item schedules: {str(e)}"


@mcp.tool()
async def list_workspace_schedules(workspace: str) -> str:
    """List ALL schedules across all items in a workspace - see everything that's scheduled to run! (READ-ONLY)

    This aggregates schedules from all items in the workspace to give you a complete view
    of what's scheduled to run when.

    Args:
        workspace: Name or ID of the workspace
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items in workspace
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")

        if not items:
            return f"No items found in workspace '{workspace}'."

        all_schedules = []

        # Get schedules for each item
        for item in items:
            try:
                schedules = await client.paginated_request(
                    f"workspaces/{workspace_id}/items/{item['id']}/schedules"
                )
                if schedules:
                    for schedule in schedules:
                        schedule["itemName"] = item["displayName"]
                        schedule["itemType"] = item["type"]
                        schedule["itemId"] = item["id"]
                        all_schedules.append(schedule)
            except Exception:
                # Skip items that don't support schedules
                continue

        if not all_schedules:
            return f"No schedules found in workspace '{workspace}'."

        # Sort by enabled status first, then by item name
        all_schedules.sort(key=lambda x: (not x.get("enabled", False), x["itemName"]))

        markdown = f"# All Schedules in Workspace '{workspace}'\n\n"
        markdown += f"**Total Schedules**: {len(all_schedules)}\n\n"

        # Count enabled vs disabled
        enabled_count = sum(1 for s in all_schedules if s.get("enabled", False))
        disabled_count = len(all_schedules) - enabled_count
        markdown += f"- ✅ **Enabled**: {enabled_count}\n"
        markdown += f"- ❌ **Disabled**: {disabled_count}\n\n"

        markdown += (
            "| Item | Type | Job Type | Status | Frequency | Next Run | Timezone |\n"
        )
        markdown += (
            "|------|------|----------|--------|-----------|----------|----------|\n"
        )

        for schedule in all_schedules:
            status = "✅ Enabled" if schedule.get("enabled", False) else "❌ Disabled"
            frequency = schedule.get("recurrence", {}).get("frequency", "N/A")
            next_run = schedule.get("recurrence", {}).get("startDate", "N/A")
            timezone = schedule.get("recurrence", {}).get("timeZone", "N/A")

            markdown += f"| {schedule['itemName']} | {schedule['itemType']} | "
            markdown += f"{schedule.get('jobType', 'N/A')} | {status} | "
            markdown += f"{frequency} | {next_run} | {timezone} |\n"

        return markdown

    except Exception as e:
        return f"Error listing workspace schedules: {str(e)}"


@mcp.tool()
async def list_environments(workspace: str = None) -> str:
    """List all Fabric environments for compute and library management (READ-ONLY).

    Args:
        workspace: Optional workspace name or ID to filter environments
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        if workspace:
            # Get environments for specific workspace
            workspace_id = await client.resolve_workspace(workspace)
            environments = await client.paginated_request(
                f"workspaces/{workspace_id}/items", params={"type": "Environment"}
            )
            workspace_filter = f" in workspace '{workspace}'"
        else:
            # Get all accessible workspaces and their environments
            workspaces = await client.paginated_request("workspaces")
            environments = []

            for ws in workspaces:
                try:
                    ws_environments = await client.paginated_request(
                        f"workspaces/{ws['id']}/items", params={"type": "Environment"}
                    )
                    for env in ws_environments:
                        env["workspaceName"] = ws["displayName"]
                        environments.append(env)
                except Exception:
                    # Skip workspaces we can't access
                    continue

            workspace_filter = " across all accessible workspaces"

        if not environments:
            return f"No environments found{workspace_filter}."

        markdown = f"# Fabric Environments{workspace_filter}\n\n"
        markdown += f"**Total Environments**: {len(environments)}\n\n"

        markdown += "| Name | Workspace | Description | Created | Modified |\n"
        markdown += "|------|-----------|-------------|---------|----------|\n"

        for env in environments:
            workspace_name = env.get("workspaceName", "Current")
            description = env.get("description", "No description")[:50] + (
                "..." if len(env.get("description", "")) > 50 else ""
            )
            created = env.get("createdDate", "N/A")
            modified = env.get("lastModifiedDate", "N/A")

            markdown += f"| {env['displayName']} | {workspace_name} | {description} | {created} | {modified} |\n"

        return markdown

    except Exception as e:
        return f"Error listing environments: {str(e)}"


@mcp.tool()
async def get_environment_details(workspace: str, environment_name: str) -> str:
    """Get detailed configuration of a Fabric environment (READ-ONLY).

    Args:
        workspace: Name or ID of the workspace
        environment_name: Name or ID of the environment
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Find the environment
        environments = await client.paginated_request(
            f"workspaces/{workspace_id}/items", params={"type": "Environment"}
        )
        environment = None
        for env in environments:
            if (
                environment_name.lower() in env["displayName"].lower()
                or env["id"] == environment_name
            ):
                environment = env
                break

        if not environment:
            return f"Environment '{environment_name}' not found in workspace '{workspace}'."

        markdown = f"# Environment Details: {environment['displayName']}\n\n"
        markdown += f"**Workspace**: {workspace}\n"
        markdown += f"**Environment ID**: {environment['id']}\n"
        markdown += (
            f"**Description**: {environment.get('description', 'No description')}\n\n"
        )

        # Get Spark compute configuration
        try:
            spark_config = await client._make_request(
                f"workspaces/{workspace_id}/environments/{environment['id']}/sparkcompute"
            )

            markdown += "## Spark Compute Configuration\n"
            markdown += (
                f"- **Runtime Version**: {spark_config.get('runtimeVersion', 'N/A')}\n"
            )
            markdown += f"- **Pool Name**: {spark_config.get('poolName', 'N/A')}\n"
            markdown += (
                f"- **Driver Cores**: {spark_config.get('driverCores', 'N/A')}\n"
            )
            markdown += (
                f"- **Driver Memory**: {spark_config.get('driverMemory', 'N/A')}\n"
            )
            markdown += (
                f"- **Executor Cores**: {spark_config.get('executorCores', 'N/A')}\n"
            )
            markdown += (
                f"- **Executor Memory**: {spark_config.get('executorMemory', 'N/A')}\n"
            )
            markdown += f"- **Dynamic Executor Allocation**: {spark_config.get('dynamicExecutorAllocation', {}).get('enabled', 'N/A')}\n\n"

            # Spark properties
            if spark_config.get("sparkProperties"):
                markdown += "### Spark Properties\n"
                for key, value in spark_config["sparkProperties"].items():
                    markdown += f"- **{key}**: {value}\n"
                markdown += "\n"

        except Exception:
            markdown += (
                "## Spark Compute Configuration\n*Not available or access denied*\n\n"
            )

        # Get libraries
        try:
            libraries = await client._make_request(
                f"workspaces/{workspace_id}/environments/{environment['id']}/libraries"
            )

            if libraries and libraries.get("libraries"):
                markdown += "## Installed Libraries\n"

                public_libs = [
                    lib for lib in libraries["libraries"] if lib.get("type") == "Public"
                ]
                custom_libs = [
                    lib for lib in libraries["libraries"] if lib.get("type") == "Custom"
                ]

                if public_libs:
                    markdown += "### Public Libraries\n"
                    for lib in public_libs[:20]:  # Limit to first 20
                        markdown += f"- **{lib.get('name', 'N/A')}**: {lib.get('version', 'N/A')}\n"
                    if len(public_libs) > 20:
                        markdown += f"- *... and {len(public_libs) - 20} more public libraries*\n"
                    markdown += "\n"

                if custom_libs:
                    markdown += "### Custom Libraries\n"
                    for lib in custom_libs:
                        markdown += f"- **{lib.get('name', 'N/A')}** ({lib.get('size', 'N/A')} bytes)\n"
                    markdown += "\n"
            else:
                markdown += (
                    "## Installed Libraries\n*No additional libraries installed*\n\n"
                )

        except Exception:
            markdown += "## Installed Libraries\n*Not available or access denied*\n\n"

        return markdown

    except Exception as e:
        return f"Error getting environment details: {str(e)}"


@mcp.tool()
async def list_compute_usage(workspace: str = None, time_range_hours: int = 24) -> str:
    """Monitor current compute resource consumption across Fabric workloads (READ-ONLY).

    Shows active Spark jobs, resource allocation, and capacity utilization.

    Args:
        workspace: Optional workspace name or ID to filter usage
        time_range_hours: Hours to look back for usage data (default: 24)
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        markdown = f"# Compute Usage Report\n\n"
        markdown += f"**Time Range**: Last {time_range_hours} hours\n"

        if workspace:
            workspace_id = await client.resolve_workspace(workspace)
            markdown += f"**Workspace**: {workspace}\n\n"
        else:
            workspace_id = None
            markdown += f"**Scope**: All accessible workspaces\n\n"

        # Get active job instances to show current compute usage
        active_jobs = []
        total_active_jobs = 0

        if workspace_id:
            # Single workspace
            workspaces_to_check = [{"id": workspace_id, "displayName": workspace}]
        else:
            # All workspaces
            workspaces_to_check = await client.paginated_request("workspaces")

        for ws in workspaces_to_check:
            try:
                # Get items in workspace
                items = await client.paginated_request(f"workspaces/{ws['id']}/items")

                for item in items:
                    try:
                        # Get active job instances
                        jobs = await client.paginated_request(
                            f"workspaces/{ws['id']}/items/{item['id']}/jobs/instances",
                            params={"status": "InProgress"},  # Only active jobs
                        )

                        for job in jobs:
                            if job.get("status") == "InProgress":
                                job["workspaceName"] = ws["displayName"]
                                job["itemName"] = item["displayName"]
                                job["itemType"] = item["type"]
                                active_jobs.append(job)
                                total_active_jobs += 1

                    except Exception:
                        continue

            except Exception:
                continue

        # Summary section
        markdown += "## Current Usage Summary\n"
        markdown += f"- **Active Jobs**: {total_active_jobs}\n"

        # Estimate resource usage (this is approximate since we don't have direct CU API)
        if total_active_jobs > 0:
            # Rough estimation: assume average job uses 4-8 cores
            estimated_cores = total_active_jobs * 6  # Average estimate
            estimated_cus = estimated_cores / 2  # 1 CU = 2 Spark cores
            markdown += (
                f"- **Estimated Active Cores**: ~{estimated_cores} Spark vCores\n"
            )
            markdown += f"- **Estimated Capacity Usage**: ~{estimated_cus:.1f} CUs\n"
        else:
            markdown += f"- **Estimated Active Cores**: 0 Spark vCores\n"
            markdown += f"- **Estimated Capacity Usage**: 0 CUs\n"

        markdown += "\n"

        if active_jobs:
            markdown += "## Active Jobs Details\n"
            markdown += "| Workspace | Item | Type | Job Type | Status | Start Time | Duration |\n"
            markdown += "|-----------|------|------|----------|--------|------------|----------|\n"

            for job in active_jobs[:30]:  # Limit to 30 most recent
                start_time = job.get("startTimeUtc", "N/A")
                # Calculate rough duration if we have start time
                duration = "Running"

                markdown += f"| {job['workspaceName']} | {job['itemName']} | {job['itemType']} | "
                markdown += (
                    f"{job.get('jobType', 'N/A')} | {job.get('status', 'N/A')} | "
                )
                markdown += f"{start_time} | {duration} |\n"

            if len(active_jobs) > 30:
                markdown += f"\n*Showing 30 most recent active jobs out of {len(active_jobs)} total.*\n"
        else:
            markdown += "## Active Jobs Details\n*No active jobs found.*\n"

        # Resource recommendations
        markdown += "\n## Usage Insights\n"
        if total_active_jobs == 0:
            markdown += "✅ **Low Usage**: No active compute jobs detected.\n"
        elif total_active_jobs <= 5:
            markdown += "🟡 **Moderate Usage**: Few active jobs running.\n"
        else:
            markdown += (
                "🔴 **High Usage**: Many active jobs - consider capacity planning.\n"
            )

        markdown += "\n*Note: This shows current job activity. For detailed capacity metrics and historical usage, use the Microsoft Fabric Capacity Metrics app.*\n"

        return markdown

    except Exception as e:
        return f"Error getting compute usage: {str(e)}"


@mcp.tool()
async def get_item_lineage(workspace: str, item_name: str) -> str:
    """Get data lineage information for a Fabric item (READ-ONLY).

    Shows upstream and downstream dependencies, data flow relationships.
    Note: This provides item-level lineage based on workspace relationships.

    Args:
        workspace: Name or ID of the workspace
        item_name: Name or ID of the item to analyze
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Find the target item
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")
        target_item = None
        for item in items:
            if (
                item_name.lower() in item["displayName"].lower()
                or item["id"] == item_name
            ):
                target_item = item
                break

        if not target_item:
            return f"Item '{item_name}' not found in workspace '{workspace}'."

        markdown = f"# Data Lineage: {target_item['displayName']}\n\n"
        markdown += f"**Item Type**: {target_item['type']}\n"
        markdown += f"**Workspace**: {workspace}\n"
        markdown += f"**Item ID**: {target_item['id']}\n\n"

        # Analyze relationships based on item type and connections
        upstream_items = []
        downstream_items = []

        # Get all shortcuts if this is a lakehouse
        if target_item["type"] == "Lakehouse":
            try:
                shortcuts = await client.paginated_request(
                    f"workspaces/{workspace_id}/items/{target_item['id']}/shortcuts"
                )
                if shortcuts:
                    markdown += "## Data Sources (via OneLake Shortcuts)\n"
                    for shortcut in shortcuts:
                        target_path = shortcut.get("target", {})
                        if target_path.get("lakehouse"):
                            upstream_items.append(
                                {
                                    "name": f"Lakehouse: {target_path['lakehouse'].get('workspaceName', 'Unknown')}/{target_path['lakehouse'].get('itemName', 'Unknown')}",
                                    "type": "Lakehouse",
                                    "relationship": "Data Source",
                                }
                            )
                        elif target_path.get("adlsGen2"):
                            upstream_items.append(
                                {
                                    "name": f"ADLS Gen2: {target_path['adlsGen2'].get('url', 'Unknown')}",
                                    "type": "External Storage",
                                    "relationship": "Data Source",
                                }
                            )
            except Exception:
                pass

        # Find items that might depend on this item (basic analysis)
        for item in items:
            if item["id"] == target_item["id"]:
                continue

            # Notebooks often depend on lakehouses
            if item["type"] == "Notebook" and target_item["type"] == "Lakehouse":
                downstream_items.append(
                    {
                        "name": item["displayName"],
                        "type": item["type"],
                        "relationship": "Likely Consumer",
                    }
                )

            # Reports might depend on semantic models/datasets
            elif item["type"] == "Report" and target_item["type"] in [
                "SemanticModel",
                "Dataset",
            ]:
                downstream_items.append(
                    {
                        "name": item["displayName"],
                        "type": item["type"],
                        "relationship": "Likely Consumer",
                    }
                )

            # Pipelines might depend on various items
            elif item["type"] == "DataPipeline":
                if target_item["type"] in ["Lakehouse", "Warehouse", "KqlDatabase"]:
                    downstream_items.append(
                        {
                            "name": item["displayName"],
                            "type": item["type"],
                            "relationship": "Possible Consumer",
                        }
                    )

        # Display upstream dependencies
        if upstream_items:
            markdown += "## Upstream Dependencies\n"
            markdown += "*Items that this item depends on for data*\n\n"
            markdown += "| Name | Type | Relationship |\n"
            markdown += "|------|------|-------------|\n"
            for item in upstream_items:
                markdown += (
                    f"| {item['name']} | {item['type']} | {item['relationship']} |\n"
                )
            markdown += "\n"
        else:
            markdown += (
                "## Upstream Dependencies\n*No upstream dependencies detected*\n\n"
            )

        # Display downstream dependencies
        if downstream_items:
            markdown += "## Downstream Dependencies\n"
            markdown += "*Items that likely depend on this item*\n\n"
            markdown += "| Name | Type | Relationship |\n"
            markdown += "|------|------|-------------|\n"
            for item in downstream_items[:20]:  # Limit to 20
                markdown += (
                    f"| {item['name']} | {item['type']} | {item['relationship']} |\n"
                )
            if len(downstream_items) > 20:
                markdown += f"\n*... and {len(downstream_items) - 20} more potential dependencies*\n"
            markdown += "\n"
        else:
            markdown += (
                "## Downstream Dependencies\n*No downstream dependencies detected*\n\n"
            )

        # Impact analysis summary
        markdown += "## Impact Analysis Summary\n"
        total_dependencies = len(upstream_items) + len(downstream_items)
        if total_dependencies == 0:
            markdown += (
                "✅ **Low Impact**: This item appears to have minimal dependencies.\n"
            )
        elif total_dependencies <= 5:
            markdown += (
                "🟡 **Moderate Impact**: This item has some dependencies to consider.\n"
            )
        else:
            markdown += "🔴 **High Impact**: This item has many dependencies - changes should be carefully planned.\n"

        markdown += "\n*Note: This provides basic lineage analysis based on item types and relationships. For detailed column-level lineage, use the Fabric Lineage view in the UI or Microsoft Purview integration.*\n"

        return markdown

    except Exception as e:
        return f"Error getting item lineage: {str(e)}"


@mcp.tool()
async def list_item_dependencies(workspace: str, item_type: str = None) -> str:
    """List dependencies between items in a workspace (READ-ONLY).

    Shows which items depend on which other items for comprehensive dependency mapping.

    Args:
        workspace: Name or ID of the workspace
        item_type: Optional item type filter (Lakehouse, Notebook, Report, etc.)
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        # Convert names to IDs
        workspace_id = await client.resolve_workspace(workspace)

        # Get all items in workspace
        items = await client.paginated_request(f"workspaces/{workspace_id}/items")

        if not items:
            return f"No items found in workspace '{workspace}'."

        # Filter by item type if specified
        if item_type:
            items = [
                item for item in items if item["type"].lower() == item_type.lower()
            ]
            if not items:
                return (
                    f"No items of type '{item_type}' found in workspace '{workspace}'."
                )

        markdown = f"# Item Dependencies in '{workspace}'\n\n"
        if item_type:
            markdown += f"**Item Type Filter**: {item_type}\n"
        markdown += f"**Total Items**: {len(items)}\n\n"

        # Analyze dependencies
        dependencies = []

        for item in items:
            item_deps = {"item": item, "depends_on": [], "used_by": []}

            # Check for shortcuts (data dependencies)
            if item["type"] == "Lakehouse":
                try:
                    shortcuts = await client.paginated_request(
                        f"workspaces/{workspace_id}/items/{item['id']}/shortcuts"
                    )
                    for shortcut in shortcuts:
                        target = shortcut.get("target", {})
                        if target.get("lakehouse"):
                            item_deps["depends_on"].append(
                                {
                                    "name": f"{target['lakehouse'].get('itemName', 'Unknown')}",
                                    "type": "Lakehouse",
                                    "relationship": "Data Source (Shortcut)",
                                }
                            )
                except Exception:
                    pass

            # Analyze potential relationships based on item types
            for other_item in items:
                if other_item["id"] == item["id"]:
                    continue

                # Common dependency patterns
                if item["type"] == "Notebook":
                    if other_item["type"] in ["Lakehouse", "Warehouse", "KqlDatabase"]:
                        item_deps["depends_on"].append(
                            {
                                "name": other_item["displayName"],
                                "type": other_item["type"],
                                "relationship": "Likely Data Source",
                            }
                        )

                elif item["type"] == "Report":
                    if other_item["type"] in [
                        "SemanticModel",
                        "Dataset",
                        "Lakehouse",
                        "Warehouse",
                    ]:
                        item_deps["depends_on"].append(
                            {
                                "name": other_item["displayName"],
                                "type": other_item["type"],
                                "relationship": "Likely Data Source",
                            }
                        )

                elif item["type"] == "DataPipeline":
                    if other_item["type"] in [
                        "Lakehouse",
                        "Warehouse",
                        "Notebook",
                        "DataFlow",
                    ]:
                        item_deps["depends_on"].append(
                            {
                                "name": other_item["displayName"],
                                "type": other_item["type"],
                                "relationship": "Pipeline Component",
                            }
                        )

            dependencies.append(item_deps)

        # Display dependency matrix
        markdown += "## Dependency Overview\n"
        markdown += "| Item | Type | Dependencies | Used By |\n"
        markdown += "|------|------|--------------|--------|\n"

        for dep in dependencies:
            item = dep["item"]
            dep_count = len(dep["depends_on"])
            used_count = sum(
                1
                for other_dep in dependencies
                for depends in other_dep["depends_on"]
                if depends["name"] == item["displayName"]
            )

            markdown += f"| {item['displayName']} | {item['type']} | {dep_count} | {used_count} |\n"

        # Detailed dependencies
        markdown += "\n## Detailed Dependencies\n"

        for dep in dependencies:
            if dep["depends_on"]:
                item = dep["item"]
                markdown += f"\n### {item['displayName']} ({item['type']})\n"
                markdown += "**Depends on:**\n"
                for depends in dep["depends_on"]:
                    markdown += f"- **{depends['name']}** ({depends['type']}) - {depends['relationship']}\n"

        # Dependency insights
        high_dependency_items = [
            dep for dep in dependencies if len(dep["depends_on"]) > 3
        ]
        highly_used_items = []

        for dep in dependencies:
            item = dep["item"]
            usage_count = sum(
                1
                for other_dep in dependencies
                for depends in other_dep["depends_on"]
                if depends["name"] == item["displayName"]
            )
            if usage_count > 2:
                highly_used_items.append((item, usage_count))

        markdown += "\n## Dependency Insights\n"

        if high_dependency_items:
            markdown += "### High Dependency Items (>3 dependencies)\n"
            for dep in high_dependency_items:
                markdown += f"- **{dep['item']['displayName']}** ({dep['item']['type']}) - {len(dep['depends_on'])} dependencies\n"
            markdown += "\n"

        if highly_used_items:
            markdown += "### Highly Used Items (>2 dependents)\n"
            for item, count in highly_used_items:
                markdown += f"- **{item['displayName']}** ({item['type']}) - Used by {count} items\n"
            markdown += "\n"

        markdown += "*Note: Dependencies are inferred based on common patterns and item types. For exact lineage, use the Fabric Lineage view or detailed analysis.*\n"

        return markdown

    except Exception as e:
        return f"Error listing item dependencies: {str(e)}"


@mcp.tool()
async def get_data_source_usage(
    workspace: str = None, connection_name: str = None
) -> str:
    """Analyze where data sources and connections are used across Fabric items (READ-ONLY).

    Args:
        workspace: Optional workspace name or ID to scope analysis
        connection_name: Optional connection name to focus on specific data source
    """
    try:
        credential = DefaultAzureCredential()
        client = FabricApiClient(credential)

        markdown = f"# Data Source Usage Analysis\n\n"

        # Get connections
        connections = await client.paginated_request("connections")

        if connection_name:
            # Filter to specific connection
            connections = [
                conn
                for conn in connections
                if connection_name.lower() in conn["displayName"].lower()
            ]
            if not connections:
                return f"Connection '{connection_name}' not found."
            markdown += f"**Connection Filter**: {connection_name}\n"

        if workspace:
            workspace_id = await client.resolve_workspace(workspace)
            workspaces_to_check = [{"id": workspace_id, "displayName": workspace}]
            markdown += f"**Workspace Filter**: {workspace}\n"
        else:
            workspaces_to_check = await client.paginated_request("workspaces")
            markdown += f"**Scope**: All accessible workspaces\n"

        markdown += f"**Connections Found**: {len(connections)}\n\n"

        usage_analysis = {}

        # Analyze each connection
        for connection in connections:
            conn_usage = {
                "connection": connection,
                "used_in_items": [],
                "total_usage": 0,
            }

            # Check usage across workspaces
            for ws in workspaces_to_check:
                try:
                    items = await client.paginated_request(
                        f"workspaces/{ws['id']}/items"
                    )

                    for item in items:
                        # Items that commonly use connections
                        if item["type"] in [
                            "DataPipeline",
                            "Dataflow",
                            "Notebook",
                            "Report",
                            "SemanticModel",
                        ]:
                            # This is a heuristic - in practice, we'd need to inspect item definitions
                            # For now, we'll identify potential usage based on item types
                            conn_usage["used_in_items"].append(
                                {
                                    "workspace": ws["displayName"],
                                    "item": item["displayName"],
                                    "type": item["type"],
                                    "usage_type": "Potential Usage",
                                }
                            )
                            conn_usage["total_usage"] += 1

                except Exception:
                    continue

            if conn_usage["total_usage"] > 0 or not connection_name:
                usage_analysis[connection["id"]] = conn_usage

        # Display results
        if not usage_analysis:
            return "No data source usage found with the specified filters."

        # Summary
        markdown += "## Usage Summary\n"
        total_connections = len(usage_analysis)
        total_usages = sum(
            analysis["total_usage"] for analysis in usage_analysis.values()
        )

        markdown += f"- **Connections Analyzed**: {total_connections}\n"
        markdown += f"- **Total Potential Usages**: {total_usages}\n\n"

        # Top used connections
        sorted_connections = sorted(
            usage_analysis.values(), key=lambda x: x["total_usage"], reverse=True
        )

        markdown += "## Connection Usage Details\n"

        for analysis in sorted_connections[:20]:  # Top 20
            connection = analysis["connection"]
            conn_type = connection.get("connectionDetails", {}).get("type", "Unknown")

            markdown += f"\n### {connection['displayName']} ({conn_type})\n"
            markdown += f"**Connection ID**: {connection['id']}\n"
            markdown += f"**Privacy Level**: {connection.get('privacyLevel', 'N/A')}\n"
            markdown += f"**Total Potential Usages**: {analysis['total_usage']}\n"

            if analysis["used_in_items"]:
                markdown += "\n**Used in Items:**\n"
                markdown += "| Workspace | Item | Type | Usage Type |\n"
                markdown += "|-----------|------|------|------------|\n"

                for usage in analysis["used_in_items"][
                    :10
                ]:  # Limit to 10 per connection
                    markdown += f"| {usage['workspace']} | {usage['item']} | {usage['type']} | {usage['usage_type']} |\n"

                if len(analysis["used_in_items"]) > 10:
                    markdown += f"\n*... and {len(analysis['used_in_items']) - 10} more usages*\n"

            markdown += "\n"

        # Usage insights
        markdown += "## Usage Insights\n"

        unused_connections = [
            analysis
            for analysis in usage_analysis.values()
            if analysis["total_usage"] == 0
        ]
        highly_used = [
            analysis
            for analysis in usage_analysis.values()
            if analysis["total_usage"] > 5
        ]

        if unused_connections:
            markdown += (
                f"### Potentially Unused Connections ({len(unused_connections)})\n"
            )
            for analysis in unused_connections[:5]:
                conn = analysis["connection"]
                markdown += f"- **{conn['displayName']}** ({conn.get('connectionDetails', {}).get('type', 'Unknown')})\n"
            if len(unused_connections) > 5:
                markdown += f"- *... and {len(unused_connections) - 5} more*\n"
            markdown += "\n"

        if highly_used:
            markdown += f"### Highly Used Connections (>5 usages)\n"
            for analysis in highly_used:
                conn = analysis["connection"]
                markdown += f"- **{conn['displayName']}**: {analysis['total_usage']} potential usages\n"
            markdown += "\n"

        markdown += "*Note: Usage analysis is based on item types and potential patterns. For exact connection usage, detailed item definition analysis would be required.*\n"

        return markdown

    except Exception as e:
        return f"Error analyzing data source usage: {str(e)}"


@mcp.tool()
async def clear_fabric_data_cache(show_stats: bool = True) -> str:
    """Clear Fabric data list caches to see newly created resources immediately (ADMIN).

    Clears caches for: workspaces, connections, items, capacities, environments, jobs, shortcuts, schedules.
    Does NOT clear name→ID resolution caches (those never become invalid).

    Use this after creating new workspaces, connections, or items to see them immediately.

    Args:
        show_stats: Show cache statistics before clearing
    """
    try:
        markdown = "# 🗑️ Fabric Data Cache Management\n\n"

        cleared_caches = []
        cache_stats = []

        # Collect cache stats and clear TTL-based data caches
        cache_info = [
            ("Workspace list", _WORKSPACE_CACHE, "workspaces"),
            ("Connections list", _CONNECTIONS_CACHE, "connections"),
            ("Capacities list", _CAPACITIES_CACHE, "capacities"),
            ("Items lists", _ITEMS_CACHE, "workspace items"),
            ("Shortcuts lists", _SHORTCUTS_CACHE, "item shortcuts"),
            ("Job instances", _JOB_INSTANCES_CACHE, "job instances"),
            ("Schedules", _SCHEDULES_CACHE, "schedules"),
            ("Environments", _ENVIRONMENTS_CACHE, "environments"),
        ]

        if show_stats:
            markdown += "## 📊 Cache Statistics (Before Clearing)\n\n"
            markdown += "| Cache Type | Entries | TTL (seconds) | Description |\n"
            markdown += "|------------|---------|---------------|-------------|\n"

            for name, cache, desc in cache_info:
                entries = len(cache)
                ttl = cache.ttl if hasattr(cache, "ttl") else "N/A"
                markdown += f"| {name} | {entries} | {ttl} | {desc} |\n"
                cache_stats.append((name, entries))

            markdown += "\n"

        # Clear all TTL caches
        for name, cache, desc in cache_info:
            size = len(cache)
            cache.clear()
            if size > 0:
                cleared_caches.append(f"{name} ({size} entries)")

        markdown += "## ✅ Caches Cleared\n\n"

        if cleared_caches:
            for cache in cleared_caches:
                markdown += f"- 🗑️ {cache}\n"
        else:
            markdown += "- ℹ️  No cached data found to clear\n"

        markdown += "\n## 🔒 Preserved Caches\n\n"
        markdown += "- ✅ Workspace name → ID mappings (never become invalid)\n"
        markdown += "- ✅ Lakehouse name → ID mappings (never become invalid)\n"

        markdown += "\n## 🎯 Result\n\n"
        markdown += (
            "**Next API calls will fetch fresh data from Microsoft Fabric!**\n\n"
        )
        markdown += "⚠️  **Performance Impact:** Initial calls will be slower until caches rebuild.\n"
        markdown += "⚡ **Cache Rebuild:** Caches will automatically repopulate on next use with fresh data."

        return markdown

    except Exception as e:
        return f"Error clearing data cache: {str(e)}"


@mcp.tool()
async def clear_name_resolution_cache(show_stats: bool = True) -> str:
    """Clear global name→ID resolution caches for workspaces and lakehouses (ADMIN).

    This clears the permanent name→ID mapping caches. Use this if:
    - A workspace was renamed or deleted/recreated with the same name
    - A lakehouse was renamed or deleted/recreated with the same name
    - You suspect stale name→ID mappings are causing issues

    Note: These caches normally never need clearing as name→ID mappings are permanent.

    Args:
        show_stats: Show cache statistics before clearing
    """
    try:
        markdown = "# 🔄 Name Resolution Cache Management\n\n"

        if show_stats:
            workspace_count = len(_global_workspace_cache)
            lakehouse_count = len(_global_lakehouse_cache)

            markdown += "## 📊 Current Cache Statistics\n\n"
            markdown += (
                f"- 🏢 Workspace name→ID mappings: **{workspace_count}** entries\n"
            )
            markdown += (
                f"- 🏠 Lakehouse name→ID mappings: **{lakehouse_count}** entries\n\n"
            )

            if workspace_count > 0:
                markdown += "### Workspace Mappings\n"
                for name, workspace_id in list(_global_workspace_cache.items())[
                    :10
                ]:  # Show first 10
                    display_name = name if len(name) <= 30 else f"{name[:27]}..."
                    display_id = (
                        workspace_id
                        if len(workspace_id) <= 20
                        else f"{workspace_id[:17]}..."
                    )
                    markdown += f"- `{display_name}` → `{display_id}`\n"
                if workspace_count > 10:
                    markdown += f"- ... and {workspace_count - 10} more\n"
                markdown += "\n"

            if lakehouse_count > 0:
                markdown += "### Lakehouse Mappings\n"
                for cache_key, lakehouse_id in list(_global_lakehouse_cache.items())[
                    :10
                ]:  # Show first 10
                    display_key = (
                        cache_key if len(cache_key) <= 40 else f"{cache_key[:37]}..."
                    )
                    display_id = (
                        lakehouse_id
                        if len(lakehouse_id) <= 20
                        else f"{lakehouse_id[:17]}..."
                    )
                    markdown += f"- `{display_key}` → `{display_id}`\n"
                if lakehouse_count > 10:
                    markdown += f"- ... and {lakehouse_count - 10} more\n"
                markdown += "\n"

        # Clear the caches
        workspace_cleared = len(_global_workspace_cache)
        lakehouse_cleared = len(_global_lakehouse_cache)

        _global_workspace_cache.clear()
        _global_lakehouse_cache.clear()

        markdown += "## ✅ Caches Cleared\n\n"
        markdown += (
            f"- 🗑️ Workspace name→ID cache: **{workspace_cleared}** entries cleared\n"
        )
        markdown += (
            f"- 🗑️ Lakehouse name→ID cache: **{lakehouse_cleared}** entries cleared\n\n"
        )

        markdown += "## 🎯 Result\n\n"
        if workspace_cleared > 0 or lakehouse_cleared > 0:
            markdown += "✅ **Name resolution caches cleared successfully!**\n\n"
            markdown += "Next workspace/lakehouse name lookups will make fresh API calls to resolve names to IDs.\n"
        else:
            markdown += "ℹ️  **No cached name mappings found to clear.**\n\n"
            markdown += "The name resolution caches were already empty.\n"

        return markdown

    except Exception as e:
        return f"Error clearing name resolution cache: {str(e)}"


def main():
    """Main entry point for the MCP server."""
    # Initialize and run the server
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
