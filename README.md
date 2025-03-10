# Data Engineering Toolkit

A collection of tools, utilities, and code snippets to assist with data engineering tasks. This repository provides reusable components to streamline common data engineering workflows.

## Repository Structure

```
coding-assistant-tips/
├── .python-version                       # Python version specifier for this project
├── extract_lakehouse_delta_schemas.py    # Tool to extract Delta table schemas from Fabric lakehouses
├── fabric_mcp.py                         # MCP server implementation for Fabric API integration
├── main.py                               # Simple entry point for the project
├── pyproject.toml                        # Project dependencies and metadata
├── README.md                             # Documentation and usage guide (this file)
└── uv.lock                               # Dependency lock file for UV package manager
```

## Important Note

The `fabric_mcp.py` builds a MCP based on the `extract_lakehouse_delta_schemas.py` file. We have kept the `extract_lakehouse_delta_schemas.py` if one does not want to use the MCP.

## Available Tools

### Delta Schema Extractor

The `extract_lakehouse_delta_schemas.py` script extracts schema information from Delta tables in Microsoft Fabric lakehouses and generates markdown documentation. This tool helps data engineers document their data assets and understand the structure of their Delta tables.

#### Features:
- Connects to Microsoft Fabric API using Azure credentials
- Extracts schema and metadata from Delta tables
- Generates comprehensive markdown documentation
- Supports filtering by specific lakehouses
- Configurable verbosity levels for debugging

#### Usage:
```bash
az login --tenant <tenant_id>
python extract_lakehouse_delta_schemas.py -w <workspace_name> [-l <lakehouse1> <lakehouse2>] [-o <output_dir>] [-v]
```

# MCP

## Getting Started

1. Clone this repository
2. Install required dependencies using UV (see "Setting Up UV Project" section below)
3. Set up Azure CLI authentication (see "Azure CLI Authentication" section below)
4. Use the tools as needed for your data engineering tasks

## Setting Up UV Project

After cloning this repository, follow these steps to set up the UV project:

1. Install UV (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Create a virtual environment and install dependencies:
```bash
uv venv
uv pip install -e .
```

3. Activate the virtual environment:
```bash
# On macOS/Linux
source .venv/bin/activate

# On Windows
.venv\Scripts\activate
```

## Azure CLI Authentication

This toolkit requires Azure CLI to be installed and properly configured for authentication with Microsoft Fabric services.

### Azure CLI Setup

1. Install Azure CLI (if not already installed):
```bash
# For macOS
brew install azure-cli

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

To use the MCP (Model Composition Platform) with this toolkit, follow these steps:

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

## Contributing

Feel free to contribute additional tools, utilities, or improvements to existing code. Please follow the existing code structure and include appropriate documentation.

