# Microsoft Fabric MCP

## Introduction

This MCP server is created to make it easier for data engineers working in Microsoft Fabric to use generative AI tools without requiring access to Microsoft Fabric Copilot (F64), which can be prohibitively expensive for many organizations.

We have built MCP tools around the endpoints available in the Fabric REST API. Currently, we've focused on providing schema information for tables in lakehouses, but we plan to expand with more tools covering additional Fabric REST API endpoints as listed in the [Microsoft Fabric REST API documentation](https://learn.microsoft.com/en-us/rest/api/fabric/articles/).

By leveraging these tools, data engineers can enhance their productivity and gain AI assistance capabilities without the need for premium licensing.

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

## Contributing

Feel free to contribute additional tools, utilities, or improvements to existing code. Please follow the existing code structure and include appropriate documentation.