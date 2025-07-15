from fastmcp import FastMCP
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, Filter, FilterExpression, FilterExpressionList
)
import os
import sys
import json
import base64
import tempfile

# Aggiungi questi import per REST API
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
from typing import Optional
import threading
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_credentials():
    """Setup Google Analytics credentials from environment variables"""

    # Method 1: Base64 encoded JSON credential (GOOGLE_APPLICATION_CREDENTIALS)
    if creds_b64 := os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        # Se sembra base64 (non inizia con / o finisce con .json)
        if not creds_b64.startswith('/') and not creds_b64.endswith('.json'):
            try:
                print(f"Decoding base64 credentials from GOOGLE_APPLICATION_CREDENTIALS", file=sys.stderr)
                creds_json = base64.b64decode(creds_b64).decode('utf-8')
                # Write to temporary file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    f.write(creds_json)
                    temp_path = f.name
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_path
                print(f"✅ Loaded credentials from GOOGLE_APPLICATION_CREDENTIALS (base64) to {temp_path}", file=sys.stderr)
                return temp_path
            except Exception as e:
                print(f"❌ Failed to decode base64 credentials: {e}", file=sys.stderr)
                return None
        else:
            # It's a file path
            if os.path.exists(creds_b64):
                print(f"✅ Using existing credentials file: {creds_b64}", file=sys.stderr)
                return creds_b64
            else:
                print(f"❌ Credentials file not found: {creds_b64}", file=sys.stderr)
                return None

    # Method 2: Alternative environment variable for base64
    elif creds_b64 := os.getenv("GOOGLE_CREDENTIALS_JSON"):
        try:
            print(f"Decoding base64 credentials from GOOGLE_CREDENTIALS_JSON", file=sys.stderr)
            creds_json = base64.b64decode(creds_b64).decode('utf-8')
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(creds_json)
                temp_path = f.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_path
            print(f"✅ Loaded credentials from GOOGLE_CREDENTIALS_JSON (base64) to {temp_path}", file=sys.stderr)
            return temp_path
        except Exception as e:
            print(f"❌ Failed to decode base64 credentials: {e}", file=sys.stderr)
            return None

    else:
        print("❌ No Google credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_CREDENTIALS_JSON", file=sys.stderr)
        return None

# Setup credentials
CREDENTIALS_PATH = setup_credentials()
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")

# Validate required environment variables
if not CREDENTIALS_PATH:
    print("ERROR: Unable to setup Google credentials", file=sys.stderr)
    print("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable with base64 encoded JSON", file=sys.stderr)
    sys.exit(1)

if not GA4_PROPERTY_ID:
    print("ERROR: GA4_PROPERTY_ID environment variable not set", file=sys.stderr)
    print("Please set it to your GA4 property ID (e.g., 123456789)", file=sys.stderr)
    sys.exit(1)

# Initialize FastMCP
mcp = FastMCP("Google Analytics 4")

# [All the dimensions and metrics data remains the same - shortened for brevity]
GA4_DIMENSIONS = {
    "time": {"date": "The date of the event in YYYYMMDD format.", "dateHour": "The date and hour of the event in YYYYMMDDHH format.", "day": "The day of the month (01-31).", "month": "The month of the year (01-12).", "year": "The year (e.g., 2024)."},
    "geography": {"city": "The city of the user.", "country": "The country of the user.", "region": "The region of the user."},
    "technology": {"browser": "The browser used by the user.", "deviceCategory": "The category of the device (e.g., 'desktop', 'mobile', 'tablet').", "operatingSystem": "The operating system of the user's device."},
    "traffic_source": {"source": "The source of the traffic.", "medium": "The medium of the traffic source.", "campaignName": "The name of the campaign."},
    "content": {"pagePath": "The path of the page (e.g., '/home').", "pageTitle": "The title of the page."},
    "events": {"eventName": "The name of the event."},
    "ecommerce": {"itemName": "The name of the item.", "transactionId": "The ID of the transaction."},
    "user_demographics": {"newVsReturning": "Whether the user is new or returning."}
}

GA4_METRICS = {
    "user_metrics": {"totalUsers": "The total number of unique users.", "newUsers": "The number of users who interacted with your site or app for the first time.", "activeUsers": "The number of distinct users who have logged an engaged session on your site or app."},
    "session_metrics": {"sessions": "The total number of sessions.", "bounceRate": "The percentage of sessions that were not engaged.", "averageSessionDuration": "The average duration of a session in seconds."},
    "pageview_metrics": {"screenPageViews": "The total number of app screens or web pages your users saw."},
    "event_metrics": {"eventCount": "The total number of events.", "conversions": "The total number of conversion events."},
    "ecommerce_metrics": {"totalRevenue": "The total revenue from all sources.", "transactions": "The total number of transactions."}
}

def load_dimensions():
    """Load available dimensions from embedded data"""
    return GA4_DIMENSIONS

def load_metrics():
    """Load available metrics from embedded data"""
    return GA4_METRICS

@mcp.tool()
def list_dimension_categories():
    """List all available GA4 dimension categories with descriptions."""
    dimensions = load_dimensions()
    result = {}
    for category, dims in dimensions.items():
        result[category] = {
            "count": len(dims),
            "dimensions": list(dims.keys())
        }
    return result

@mcp.tool()
def list_metric_categories():
    """List all available GA4 metric categories with descriptions."""
    metrics = load_metrics()
    result = {}
    for category, mets in metrics.items():
        result[category] = {
            "count": len(mets),
            "metrics": list(mets.keys())
        }
    return result

@mcp.tool()
def get_dimensions_by_category(category):
    """Get all dimensions in a specific category with their descriptions."""
    dimensions = load_dimensions()
    if category in dimensions:
        return dimensions[category]
    else:
        available_categories = list(dimensions.keys())
        return {"error": f"Category '{category}' not found. Available categories: {available_categories}"}

@mcp.tool()
def get_metrics_by_category(category):
    """Get all metrics in a specific category with their descriptions."""
    metrics = load_metrics()
    if category in metrics:
        return metrics[category]
    else:
        available_categories = list(metrics.keys())
        return {"error": f"Category '{category}' not found. Available categories: {available_categories}"}

@mcp.tool()
def get_ga4_data(
    dimensions=["date"],
    metrics=["totalUsers", "newUsers"],
    date_range_start="7daysAgo",
    date_range_end="yesterday",
    dimension_filter=None
):
    """Retrieve GA4 metrics data broken down by the specified dimensions."""
    try:
        # Handle string input for dimensions and metrics
        if isinstance(dimensions, str):
            try:
                dimensions = json.loads(dimensions)
            except json.JSONDecodeError:
                dimensions = [d.strip() for d in dimensions.split(',')]

        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except json.JSONDecodeError:
                metrics = [m.strip() for m in metrics.split(',')]

        # Validate inputs
        if not dimensions or not metrics:
            return {"error": "Both dimensions and metrics are required"}

        # GA4 API Call
        client = BetaAnalyticsDataClient()
        dimension_objects = [Dimension(name=d) for d in dimensions]
        metric_objects = [Metric(name=m) for m in metrics]

        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=dimension_objects,
            metrics=metric_objects,
            date_ranges=[DateRange(start_date=date_range_start, end_date=date_range_end)]
        )

        response = client.run_report(request)

        result = []
        for row in response.rows:
            data_row = {}
            for i, dimension_header in enumerate(response.dimension_headers):
                if i < len(row.dimension_values):
                    data_row[dimension_header.name] = row.dimension_values[i].value
            for i, metric_header in enumerate(response.metric_headers):
                if i < len(row.metric_values):
                    data_row[metric_header.name] = row.metric_values[i].value
            result.append(data_row)

        return result

    except Exception as e:
        error_message = f"Error fetching GA4 data: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}

# ====== FIXED REST API IMPLEMENTATION ======

# Create FastAPI app
rest_app = FastAPI(title="GA4 MCP REST API", version="1.0.0")

# Add CORS
rest_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global session state
sessions = {}

@rest_app.get("/")
async def root():
    return {"message": "GA4 MCP Server is running", "status": "ok", "version": "1.0.0"}

@rest_app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": time.time()}

@rest_app.post("/")
async def mcp_endpoint(request: Request):
    """Main MCP endpoint - FIXED VERSION"""
    try:
        # Parse JSON request
        data = await request.json()
        method = data.get("method")
        params = data.get("params", {})
        msg_id = data.get("id", "unknown")

        logger.info(f"MCP Request: {method} with ID: {msg_id}")

        # Handle initialize - CRITICAL FIX
        if method == "initialize":
            logger.info("Handling initialize request")

            # Quick response - don't do any heavy operations here
            response = {
                "jsonrpc": "2.0",
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {"listChanged": True},
                        "resources": {"subscribe": False, "listChanged": True}
                    },
                    "serverInfo": {
                        "name": "Google Analytics 4",
                        "version": "1.0.0"
                    }
                },
                "id": msg_id
            }

            logger.info("Initialize response ready")
            return response

        # Handle tools/list
        elif method == "tools/list":
            logger.info("Handling tools/list request")

            tools = [
                {
                    "name": "list_dimension_categories",
                    "description": "List all available GA4 dimension categories with descriptions",
                    "inputSchema": {"type": "object", "properties": {}, "required": []}
                },
                {
                    "name": "list_metric_categories",
                    "description": "List all available GA4 metric categories with descriptions",
                    "inputSchema": {"type": "object", "properties": {}, "required": []}
                },
                {
                    "name": "get_dimensions_by_category",
                    "description": "Get all dimensions in a specific category with their descriptions",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"category": {"type": "string", "description": "Category name"}},
                        "required": ["category"]
                    }
                },
                {
                    "name": "get_metrics_by_category",
                    "description": "Get all metrics in a specific category with their descriptions",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"category": {"type": "string", "description": "Category name"}},
                        "required": ["category"]
                    }
                },
                {
                    "name": "get_ga4_data",
                    "description": "Retrieve GA4 metrics data broken down by the specified dimensions",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dimensions": {"type": "array", "items": {"type": "string"}, "default": ["date"]},
                            "metrics": {"type": "array", "items": {"type": "string"}, "default": ["totalUsers", "newUsers"]},
                            "date_range_start": {"type": "string", "default": "7daysAgo"},
                            "date_range_end": {"type": "string", "default": "yesterday"}
                        },
                        "required": []
                    }
                }
            ]

            return {
                "jsonrpc": "2.0",
                "result": {"tools": tools},
                "id": msg_id
            }

        # Handle tools/call
        elif method == "tools/call":
            logger.info(f"Handling tools/call request for tool: {params.get('name')}")

            tool_name = params.get("name")
            tool_args = params.get("arguments", {})

            try:
                # Execute the tool
                if tool_name == "list_dimension_categories":
                    result = list_dimension_categories()
                elif tool_name == "list_metric_categories":
                    result = list_metric_categories()
                elif tool_name == "get_dimensions_by_category":
                    result = get_dimensions_by_category(tool_args.get("category"))
                elif tool_name == "get_metrics_by_category":
                    result = get_metrics_by_category(tool_args.get("category"))
                elif tool_name == "get_ga4_data":
                    result = get_ga4_data(
                        dimensions=tool_args.get("dimensions", ["date"]),
                        metrics=tool_args.get("metrics", ["totalUsers", "newUsers"]),
                        date_range_start=tool_args.get("date_range_start", "7daysAgo"),
                        date_range_end=tool_args.get("date_range_end", "yesterday"),
                        dimension_filter=tool_args.get("dimension_filter")
                    )
                else:
                    return {
                        "jsonrpc": "2.0",
                        "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
                        "id": msg_id
                    }

                # Return successful result
                return {
                    "jsonrpc": "2.0",
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(result, indent=2)}]
                    },
                    "id": msg_id
                }

            except Exception as e:
                logger.error(f"Tool execution error: {str(e)}")
                return {
                    "jsonrpc": "2.0",
                    "error": {"code": -32000, "message": f"Tool execution error: {str(e)}"},
                    "id": msg_id
                }

        # Handle unknown methods
        else:
            logger.warning(f"Unknown method: {method}")
            return {
                "jsonrpc": "2.0",
                "error": {"code": -32601, "message": f"Unknown method: {method}"},
                "id": msg_id
            }

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32700, "message": f"Parse error: {str(e)}"},
            "id": "error"
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32000, "message": f"Internal error: {str(e)}"},
            "id": "error"
        }

def main():
    """Main entry point for the MCP server"""
    import argparse

    parser = argparse.ArgumentParser(description='GA4 MCP Server')
    parser.add_argument('--transport', choices=['stdio', 'http'], default='http')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8000)

    args = parser.parse_args()

    if args.transport == 'stdio':
        print("Starting GA4 MCP server with stdio transport...", file=sys.stderr)
        mcp.run(transport="stdio")
    else:
        print(f"Starting GA4 MCP server with HTTP transport on {args.host}:{args.port}...", file=sys.stderr)

        # Configure uvicorn with timeout settings
        uvicorn.run(
            rest_app,
            host=args.host,
            port=args.port,
            timeout_keep_alive=30,
            timeout_notify=30,
            limit_concurrency=100,
            access_log=True
        )

if __name__ == "__main__":
    main()
