# Microsoft Fabric MCP

## Introduction

This MCP server provides data engineers with **safe, read-only access** to Microsoft Fabric resources through AI assistants like Cursor, Claude, and other MCP-compatible tools.

Built around the Fabric REST API using **only GET requests**, it includes 27 tools that let you query workspace details, examine table schemas, monitor job execution, and analyze data dependencies - all without any risk of modifying your production data.

Instead of switching between the Fabric portal and your IDE, you can now ask your AI assistant questions like "What tables are in my lakehouse?" or "Show me the schema for the sales table" and get immediate, accurate responses.

## 🔍 Key Features

- **100% Safe Operations**: Uses only GET requests - no data modification possible
- **99% READ-ONLY**: 25 read-only tools + 2 cache management tools - safe for production use
- **Comprehensive Coverage**: 25 tools covering all major Fabric operational areas
- **Smart Filtering**: Most tools support optional filtering for targeted analysis
- **Operational Intelligence**: Advanced tools for lineage, dependencies, and resource monitoring
- **High Performance**: TTL caching for fast responses with cache invalidation on demand
- **Enterprise Ready**: Designed for production Fabric environments and governance

## Available MCP Tools

This MCP server provides **27 comprehensive tools** for complete Fabric operational visibility:

> **📝 Parameter Note**: When you see `workspace` as a parameter, it accepts either the **workspace name** (like "DWH-PROD", "Analytics-Dev") or the **workspace ID/GUID**. Workspace names are more user-friendly and recommended for most use cases. The system automatically resolves names to IDs with smart caching for performance.

### 🏢 Core Fabric Management
| Tool | Description | Inputs |
|------|-------------|---------|
| `list_workspaces` | List all accessible Fabric workspaces | None |
| `get_workspace` | Get detailed workspace info including workspace identity status | `workspace` (name/ID) |
| `list_items` | List all items in workspace with optional type filtering | `workspace` (name/ID), `item_type` (optional) |
| `get_item` | Get detailed properties and metadata for specific item | `workspace` (name/ID), `item_name` (name/ID) |
| `list_connections` | List all connections user has access to across entire tenant | None |
| `list_lakehouses` | List all lakehouses in specified workspace | `workspace` (name/ID) |
| `list_capacities` | List all Fabric capacities user has access to | None |
| `get_workspace_identity` | Get workspace identity details for a specific workspace | `workspace` (name/ID) |
| `list_workspaces_with_identity` | List workspaces that have workspace identities configured | None |

### 📊 Data & Schema Management
| Tool | Description | Inputs |
|------|-------------|---------|
| `get_all_schemas` | Get schemas for all Delta tables in lakehouse | `workspace` (name/ID), `lakehouse` (name/ID) |
| `get_table_schema` | Get detailed schema for specific table | `workspace` (name/ID), `lakehouse` (name/ID), `table_name` |
| `list_tables` | List all tables in lakehouse with format/type info | `workspace` (name/ID), `lakehouse` (name/ID) |
| `list_shortcuts` | List OneLake shortcuts for specific item | `workspace` (name/ID), `item_name` (name/ID), `parent_path` (optional) |
| `get_shortcut` | Get detailed shortcut configuration and target | `workspace` (name/ID), `item_name` (name/ID), `shortcut_name`, `parent_path` (optional) |
| `list_workspace_shortcuts` | Aggregate all shortcuts across workspace items | `workspace` (name/ID) |

### ⚡ Job Monitoring & Scheduling  
| Tool | Description | Inputs |
|------|-------------|---------|
| `list_job_instances` | List job instances with status/item filtering for monitoring | `workspace` (name/ID), `item_name` (optional), `status` (optional) |
| `get_job_instance` | Get detailed job info including errors and timing | `workspace` (name/ID), `item_name` (name/ID), `job_instance_id` |
| `list_item_schedules` | List all schedules for specific item | `workspace` (name/ID), `item_name` (name/ID) |
| `list_workspace_schedules` | Aggregate all schedules across workspace - complete scheduling overview | `workspace` (name/ID) |

### 🎯 Operational Intelligence
| Tool | Description | Inputs |
|------|-------------|---------|
| `list_compute_usage` | Monitor active jobs and estimate resource consumption | `workspace` (optional), `time_range_hours` (default: 24) |
| `get_item_lineage` | Analyze data flow dependencies upstream/downstream | `workspace` (name/ID), `item_name` (name/ID) |
| `list_item_dependencies` | Map all item dependencies in workspace | `workspace` (name/ID), `item_type` (optional) |
| `get_data_source_usage` | Analyze connection usage patterns across items | `workspace` (optional), `connection_name` (optional) |
| `list_environments` | List Fabric environments for compute/library management | `workspace` (optional) |
| `get_environment_details` | Get detailed environment config including Spark settings and libraries | `workspace` (name/ID), `environment_name` (name/ID) |

### 🛠️ Cache Management & Administration
| Tool | Description | Inputs |
|------|-------------|---------|
| `clear_fabric_data_cache` | Clear all data list caches to see newly created resources immediately | `show_stats` (optional, default: true) |
| `clear_name_resolution_cache` | Clear global name→ID resolution caches for workspaces and lakehouses | `show_stats` (optional, default: true) |


## Cache Management System

The MCP server uses a sophisticated two-tier caching system for optimal performance:

### 🔄 Data List Caches (TTL-based)
These caches store lists of resources (workspaces, items, connections, etc.) and automatically expire after a set time:
- **Purpose**: Speed up repeated queries for resource lists
- **Behavior**: Automatically refresh when expired
- **Use Case**: When you create new resources and want to see them immediately in lists

**Clear with**: `clear_fabric_data_cache`

### 🏷️ Name Resolution Caches (Global, Permanent)
These caches store name→ID mappings and persist across all requests:
- **Purpose**: Avoid repeated API calls to resolve workspace/lakehouse names to IDs
- **Behavior**: Never expire automatically (name→ID mappings are permanent)
- **Use Case**: When a workspace/lakehouse is renamed or deleted/recreated with the same name

**Clear with**: `clear_name_resolution_cache`

### When to Use Each Cache Tool

| Scenario | Tool to Use | Reason |
|----------|-------------|---------|
| Created a new workspace/lakehouse | `clear_fabric_data_cache` | See new resources in lists |
| Renamed a workspace/lakehouse | `clear_name_resolution_cache` | Update name→ID mappings |
| Deleted and recreated a resource with same name | `clear_name_resolution_cache` | New resource has different ID |
| General performance troubleshooting | `clear_fabric_data_cache` | Refresh all data lists |
| Suspect stale name resolution | `clear_name_resolution_cache` | Force fresh name lookups |

Both tools are safe to use and will show detailed statistics about what was cleared.

## Getting Started

1. Clone this repository
2. Install required dependencies using UV (see "Setting Up UV Project" section below)
3. Set up Azure CLI authentication (see "Azure CLI Authentication" section below)
4. Use the tools as needed for your data engineering tasks

## Setting Up UV Project

After cloning this repository, follow these steps to set up the UV project:

1. Install UV (if not already installed):
```bash
# On macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows (using PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

2. Create a virtual environment:
```bash
uv venv
```

3. Activate the virtual environment:
```bash
# On macOS/Linux
source .venv/bin/activate

# On Windows
.venv\Scripts\activate
```

4. Install dependencies:
```bash
uv pip install -e .
```

5. Verify installation:
```bash
uv run fabric_mcp.py
```
This confirms that everything is working correctly.

## Azure CLI Authentication

This toolkit requires Azure CLI to be installed and properly configured for authentication with Microsoft Fabric services.

### Azure CLI Setup

1. Install Azure CLI (if not already installed):
```bash
# For macOS
brew install azure-cli

# For Windows
# Last ned installasjonen fra: https://aka.ms/installazurecliwindows
# Eller bruk winget:
winget install -e --id Microsoft.AzureCLI

# For other platforms, see the official Azure CLI documentation
```

2. Log in to Azure with CLI:
```bash
az login
```

3. Verify the login works:
```bash
az account show
```

4. If you have multiple subscriptions, select the one you want to use:
```bash
az account set --subscription "Name-or-ID-of-subscription"
```

When this is done, the `DefaultAzureCredential` in our code will automatically find and use your Azure CLI authentication.

## Setting up MCP

To use the MCP (Module Context Protocol) with this toolkit, follow these steps:

1. Make sure you have completed the UV setup and Azure CLI authentication steps above.

2. Add an MCP with a suitable name (like "fabric") in the Cursor settings under the MCP section. Use the following command format:
```bash
uv --directory PATH_TO_YOUR_FOLDER run fabric_mcp.py
```

For example:
```bash
uv --directory /Users/augbir/Documents/coding-assistant-tips/coding-assistant-tips/ run fabric_mcp.py
```

Replace `PATH_TO_YOUR_FOLDER` with the path to the folder containing this toolkit. This command configures the MCP server with the Fabric-specific tools.

3. Once the MCP is configured, you can interact with Microsoft Fabric resources directly from your tools and applications.

4. You can use the provided MCP tools to list workspaces, lakehouses, and tables, as well as extract schema information as documented in the tools section.

5. When successfully configured, your MCP will appear in Cursor settings like this:

![Successful MCP setup in Cursor](images/cursor_mcp_setup.png "MCP setup as shown in Cursor settings")

## Windows Setup

### Setting up the MCP Command

On Windows, you can create a batch file to easily run the MCP command:

1. Create a file named `run_mcp.bat` with the following content:
   ```
   @echo off
   SET PATH=C:\Users\YourUsername\.local\bin;%PATH%
   cd C:\path\to\your\microsoft_fabric_mcp\
   C:\Users\YourUsername\.local\bin\uv.exe run fabric_mcp.py
   ```

   Example with real paths:
   ```
   @echo off
   SET PATH=C:\Users\YourUsername\.local\bin;%PATH%
   cd C:\Users\YourUsername\source\repos\microsoft_fabric_mcp\
   C:\Users\YourUsername\.local\bin\uv.exe run fabric_mcp.py
   ```

2. You can then run the MCP command by executing:
   ```
   cmd /c C:\path\to\your\microsoft_fabric_mcp\run_mcp.bat
   ```

   Example:
   ```
   cmd /c C:\Users\YourUsername\source\repos\microsoft_fabric_mcp\run_mcp.bat
   ```

### Virtual Environment Permissions

When activating the virtual environment using `.venv\Scripts\activate` on Windows, you might encounter permission issues. To resolve this, run the following command in PowerShell before activation:

```powershell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process
```

This temporarily changes the execution policy for the current PowerShell session only, allowing scripts to run.

## Example Usage

Once you have set up the MCP server, you can start interacting with your Fabric resources through your AI assistant. Here's an example of how to use it:

### Listing Workspaces in Fabric

You can simply ask your AI assistant to list your workspaces in Fabric:

```
Can you list my workspaces in Fabric?
```

Then use either workspace names or IDs in subsequent commands:

```
Can you show me all the lakehouses in the "DWH-PROD" workspace?
Can you get the schema for the "sales" table in the "GK_Bronze" lakehouse in "DWH-PROD"?

# Or using workspace ID if you have it:
Can you list items in workspace "abc-123-def-456"?
```

The LLM will automatically understand which MCP tool to use based on your query. It will invoke the `list_workspaces` tool and display the results:

<div align="center">
  <img src="images/list_workspaces_example.png" alt="Example of listing Fabric workspaces" title="Example of listing workspaces in Fabric" width="450" />
</div>

### Advanced Use Cases

The main advantage of this MCP integration becomes clear when working with more complex tasks. For example, you can ask Claude to create a notebook that reads data from a specific table in one lakehouse and upserts it into another table in a silver lakehouse:

```
Can you create a notebook that reads data from the 'sales' table in the Bronze lakehouse and upserts it into the 'sales_processed' table in the Silver lakehouse? The notebook should take into consideration the schema of both tables.
```

In this scenario, Claude can use the MCP tools to:
1. Get the schema information for both tables
2. Understand the data structure and relationships
3. Generate appropriate code that handles data types correctly
4. Create an efficient upsert operation based on the actual table schemas

This level of context-aware assistance would be impossible without the MCP integration giving Claude access to your actual Fabric resources and schemas.

### Permission Handling

By default, the AI assistant will ask for your permission before running MCP tools that interact with your data. This gives you control over what actions are performed.

If you're using Cursor and want to enable faster interactions, you can enable YOLO mode in the settings. With YOLO mode enabled, the AI assistant will execute MCP tools without asking for permission each time.

> **Note**: YOLO mode is convenient but should be used with caution, as it grants the AI assistant more autonomous access to your data sources.

## What is Model Context Protocol (MCP)?

The Model Context Protocol (MCP) is an open protocol that standardizes how applications provide context to Large Language Models (LLMs). Think of MCP like a standardized connection port for AI applications - it provides a standardized way to connect AI models to different data sources and tools.

### How MCP Works

MCP follows a client-server architecture:

- **MCP Hosts**: Programs like Cursor IDE, Windsurf, Claude CLI, or other AI tools that want to access data through MCP
- **MCP Clients**: Protocol clients that maintain connections with servers
- **MCP Servers**: Lightweight programs (like this Microsoft Fabric MCP) that expose specific capabilities through the standardized protocol
- **Data Sources**: Your Fabric resources, databases, and other services that MCP servers can securely access

This architecture allows LLMs to interact with your data and tools in a standardized way, making it possible to:

1. Connect to pre-built integrations that your LLM can directly use
2. Maintain flexibility to switch between LLM providers
3. Keep your data secure within your infrastructure

For this project, we recommend using Cursor as your IDE for the best experience, though Windsurf and Claude CLI are also compatible options.

## Contributing

Feel free to contribute additional tools, utilities, or improvements to existing code. Please follow the existing code structure and include appropriate documentation.