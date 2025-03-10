import argparse
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable


def setup_logging(verbosity: int) -> None:
    """Configure logging based on verbosity level"""
    # Set format for all handlers
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    
    # Root logger (for dependencies) - start with WARNING
    logging.basicConfig(
        level=logging.WARNING,
        format=log_format,
        datefmt=datefmt
    )
    
    # Our script's logger
    logger = logging.getLogger(__name__)
    
    # Map verbosity to log levels
    verbosity_map = {
        0: logging.WARNING,  # -v
        1: logging.INFO,     # -vv
        2: logging.DEBUG,    # -vvv
    }
    
    # Set our script's log level based on verbosity
    logger.setLevel(verbosity_map.get(verbosity, logging.DEBUG))
    
    # Only enable dependency debugging at highest verbosity
    if verbosity >= 3:
        logging.getLogger().setLevel(logging.DEBUG)
    
    return logger

@dataclass
class FabricApiConfig:
    """Configuration for Fabric API"""
    base_url: str = "https://api.fabric.microsoft.com/v1"
    max_results: int = 100

class FabricApiClient:
    """Client for interacting with Fabric API"""
    def __init__(self, credential: DefaultAzureCredential, config: FabricApiConfig = FabricApiConfig()):
        self.credential = credential
        self.config = config
        # Initialize cached methods
        self._cached_resolve_workspace = lru_cache(maxsize=128)(self._resolve_workspace)
        self._cached_resolve_lakehouse = lru_cache(maxsize=128)(self._resolve_lakehouse)
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Fabric API calls"""
        return {
            "Authorization": f"Bearer {self.credential.get_token('https://api.fabric.microsoft.com/.default').token}"
        }
    
    def _make_request(self, 
                     endpoint: str, 
                     params: Optional[Dict] = None, 
                     method: str = "GET") -> requests.Response:
        """Make a request to the Fabric API"""
        # If endpoint is a full URL, use it directly, otherwise prepend base_url
        url = endpoint if endpoint.startswith('http') else f"{self.config.base_url}/{endpoint.lstrip('/')}"
        params = params or {}
        
        if "maxResults" not in params:
            params["maxResults"] = self.config.max_results
            
        response = requests.request(
            method=method,
            url=url,
            headers=self._get_headers(),
            params=params
        )
        response.raise_for_status()
        return response
    
    def paginated_request(self, endpoint: str, params: Optional[Dict] = None, data_key: str = "value") -> List[Dict]:
        """Make a paginated request to the Fabric API"""
        results = []
        params = params or {}
        continuation_token = None
        
        while True:
            # Construct full URL with continuation token if present
            url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
            if continuation_token:
                separator = '&' if '?' in url else '?'
                # URL encode the continuation token
                encoded_token = quote(continuation_token)
                url += f"{separator}continuationToken={encoded_token}"
            
            # Use params without continuation token
            request_params = params.copy()
            if 'continuationToken' in request_params:
                del request_params['continuationToken']
            
            response = self._make_request(url, request_params)
            data = response.json()
            
            results.extend(data[data_key])
            
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break
        
        return results
    
    def get_workspaces(self) -> List[Dict]:
        """Get all accessible workspaces"""
        return self.paginated_request("workspaces")
    
    def get_lakehouses(self, workspace_id: str) -> List[Dict]:
        """Get all lakehouses in a workspace"""
        return self.paginated_request(
            f"workspaces/{workspace_id}/items",
            params={"type": "Lakehouse"}
        )
    
    def get_tables(self, workspace_id: str, lakehouse_id: str) -> List[Dict]:
        """Get all tables in a lakehouse"""
        return self.paginated_request(
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
            data_key="data"
        )
    
    def resolve_workspace(self, workspace: str) -> str:
        """Resolve workspace name or ID to workspace ID with caching"""
        return self._cached_resolve_workspace(workspace)
    
    def _resolve_workspace(self, workspace: str) -> str:
        """Internal method to resolve workspace name or ID to workspace ID"""
        if is_valid_uuid(workspace):
            return workspace
        
        workspaces = self.get_workspaces()
        matching_workspaces = [w for w in workspaces if w["displayName"].lower() == workspace.lower()]
        
        if not matching_workspaces:
            raise ValueError(f"No workspace found with name: {workspace}")
        if len(matching_workspaces) > 1:
            raise ValueError(f"Multiple workspaces found with name: {workspace}")
        
        return matching_workspaces[0]["id"]
    
    def resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Resolve lakehouse name or ID to lakehouse ID with caching"""
        return self._cached_resolve_lakehouse(workspace_id, lakehouse)
    
    def _resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Internal method to resolve lakehouse name or ID to lakehouse ID"""
        if is_valid_uuid(lakehouse):
            return lakehouse
        
        lakehouses = self.get_lakehouses(workspace_id)
        matching_lakehouses = [lh for lh in lakehouses if lh["displayName"].lower() == lakehouse.lower()]
        
        if not matching_lakehouses:
            raise ValueError(f"No lakehouse found with name: {lakehouse}")
        if len(matching_lakehouses) > 1:
            raise ValueError(f"Multiple lakehouses found with name: {lakehouse}")
        
        return matching_lakehouses[0]["id"]

def is_valid_uuid(value: str) -> bool:
    """Check if string is a valid UUID"""
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False

def get_delta_schemas(tables: List[Dict], credential: DefaultAzureCredential) -> List[Tuple[Dict, object, object]]:
    """Get schema and metadata for each Delta table"""
    delta_tables = []
    logger.info(f"Starting schema extraction for {len(tables)} tables")
    
    # Get token for Azure Storage (not Fabric API)
    token = credential.get_token("https://storage.azure.com/.default").token
    storage_options = {
        "bearer_token": token,
        "use_fabric_endpoint": "true"
    }
    
    for table in tables:
        if table['format'].lower() == 'delta':
            try:
                table_path = table['location']
                logger.debug(f"Processing Delta table: {table['name']} at {table_path}")
                
                # Create DeltaTable instance with storage options
                delta_table = DeltaTable(table_path, storage_options=storage_options)
                
                # Get both schema and metadata
                delta_tables.append((table, delta_table.schema(), delta_table.metadata()))
                logger.info(f"Successfully processed table: {table['name']}")
                
            except Exception as e:
                logger.error(f"Failed to process table {table['name']}: {str(e)}")
    
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
        markdown += f"**Partition Columns:** {', '.join(metadata.partition_columns)}\n\n"
    if metadata.created_time:
        created_time = datetime.fromtimestamp(metadata.created_time / 1000)
        markdown += f"**Created Time:** {created_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    if metadata.configuration:
        markdown += "**Configuration:**\n\n"
        markdown += "```json\n"
        markdown += json.dumps(metadata.configuration, indent=2)
        markdown += "\n```\n"
    
    return markdown

def format_schema_to_markdown(table_info: Dict, schema: object, metadata: object) -> str:
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

def get_output_filename(workspace: str, lakehouse: str) -> str:
    """Generate output filename based on workspace and lakehouse names"""
    # Clean names to be filesystem-friendly
    def clean_name(name: str) -> str:
        return "".join(c if c.isalnum() else "_" for c in name)
    
    workspace_name = clean_name(workspace)
    lakehouse_name = clean_name(lakehouse)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    return f"delta_schemas_{workspace_name}_{lakehouse_name}_{timestamp}.md"

def main():
    parser = argparse.ArgumentParser(description='Extract Delta table schemas from Fabric lakehouses')
    parser.add_argument('-w', '--workspace', required=True,
                       help='Workspace name or ID')
    parser.add_argument('-l', '--lakehouses', nargs='*',
                       help='Optional list of lakehouse names or IDs. If not provided, will process all lakehouses.')
    parser.add_argument('-o', '--output-dir', default='.',
                       help='Output directory for markdown files (default: current directory)')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                       help='Increase verbosity level (-v, -vv, -vvv)')
    args = parser.parse_args()
    
    # Setup logging based on verbosity
    global logger
    logger = setup_logging(args.verbose)
    
    logger.info("Starting Delta schema extraction process")
    
    try:
        # Initialize Azure credential
        logger.debug("Initializing Azure credential")
        credential = DefaultAzureCredential()
        
        # Test the credential by getting a token
        logger.debug("Testing Azure credential by acquiring a token")
        try:
            credential.get_token("https://storage.azure.com/.default")
        except Exception as e:
            logger.error(f"Failed to authenticate with Azure: {str(e)}")
            exit(1)
            
        # Initialize API client
        logger.debug("Initializing Fabric API client")
        client = FabricApiClient(credential)
        
        # Resolve workspace ID
        logger.info("Resolving workspace identifier...")
        workspace_id = client.resolve_workspace(args.workspace)
        logger.debug(f"Resolved workspace '{args.workspace}' to ID: {workspace_id}")
        
        # Get all lakehouses in the workspace
        logger.info("Retrieving lakehouses from workspace...")
        all_lakehouses = client.get_lakehouses(workspace_id)
        
        # Filter lakehouses if specific ones were requested
        if args.lakehouses:
            requested_lakehouses = [lh.lower() for lh in args.lakehouses]
            lakehouses_to_process = [
                lh for lh in all_lakehouses 
                if lh['id'].lower() in requested_lakehouses or 
                lh['displayName'].lower() in requested_lakehouses
            ]
            if not lakehouses_to_process:
                logger.error("None of the specified lakehouses were found in the workspace")
                exit(1)
        else:
            lakehouses_to_process = all_lakehouses
        
        logger.info(f"Will process {len(lakehouses_to_process)} lakehouses")
        
        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Process each lakehouse
        for lakehouse in lakehouses_to_process:
            lakehouse_name = lakehouse['displayName']
            lakehouse_id = lakehouse['id']
            logger.info(f"Processing lakehouse: {lakehouse_name}")
            
            # Get tables from Fabric API
            logger.info(f"Retrieving tables from lakehouse {lakehouse_name}...")
            tables = client.get_tables(workspace_id, lakehouse_id)
            logger.info(f"Found {len(tables)} tables in lakehouse {lakehouse_name}")
            
            if not tables:
                logger.warning(f"No tables found in lakehouse {lakehouse_name}")
                continue
            
            # Get schemas for all Delta tables
            logger.info(f"Retrieving Delta table schemas for {lakehouse_name}...")
            delta_tables = get_delta_schemas(tables, credential)
            logger.info(f"Successfully processed {len(delta_tables)} Delta tables in {lakehouse_name}")
            
            # Generate markdown content
            logger.debug(f"Generating markdown documentation for {lakehouse_name}")
            markdown_content = "# Delta Table Schemas\n\n"
            markdown_content += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            markdown_content += f"Workspace: {args.workspace}\n"
            markdown_content += f"Lakehouse: {lakehouse_name}\n\n"
            
            for table_info, schema, metadata in delta_tables:
                markdown_content += format_schema_to_markdown(table_info, schema, metadata)
            
            # Determine output file name
            output_file = os.path.join(args.output_dir, get_output_filename(args.workspace, lakehouse_name))
            logger.debug(f"Writing output to: {output_file}")
            
            # Save to file
            with open(output_file, "w") as f:
                f.write(markdown_content)
            
            logger.info(f"Schema documentation for {lakehouse_name} has been saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Fatal error during execution: {str(e)}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()