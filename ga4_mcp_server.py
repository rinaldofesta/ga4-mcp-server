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

# REST API imports
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
from typing import Optional
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_credentials():
    """Setup Google Analytics credentials from environment variables"""
    if creds_b64 := os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        if not creds_b64.startswith('/') and not creds_b64.endswith('.json'):
            try:
                print(f"Decoding base64 credentials from GOOGLE_APPLICATION_CREDENTIALS", file=sys.stderr)
                creds_json = base64.b64decode(creds_b64).decode('utf-8')
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
            if os.path.exists(creds_b64):
                print(f"✅ Using existing credentials file: {creds_b64}", file=sys.stderr)
                return creds_b64
            else:
                print(f"❌ Credentials file not found: {creds_b64}", file=sys.stderr)
                return None
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
    sys.exit(1)

if not GA4_PROPERTY_ID:
    print("ERROR: GA4_PROPERTY_ID environment variable not set", file=sys.stderr)
    sys.exit(1)

# Initialize FastMCP
mcp = FastMCP("Google Analytics 4")

# GA4 Dimensions and Metrics - Simplified for faster loading
GA4_DIMENSIONS = {
    "time": {"date": "The date of the event in YYYYMMDD format.", "month": "The month of the year (01-12).", "year": "The year (e.g., 2024)."},
    "geography": {"city": "The city of the user.", "country": "The country of the user.", "region": "The region of the user."},
    "technology": {"browser": "The browser used by the user.", "deviceCategory": "The category of the device.", "operatingSystem": "The operating system."},
    "traffic_source": {"source": "The source of the traffic.", "medium": "The medium of the traffic source.", "campaignName": "The name of the campaign."},
    "content": {"pagePath": "The path of the page.", "pageTitle": "The title of the page."},
    "events": {"eventName": "The name of the event."},
    "ecommerce": {"itemName": "The name of the item.", "transactionId": "The ID of the transaction."},
    "user_demographics": {"newVsReturning": "Whether the user is new or returning."}
}

GA4_METRICS = {
    "user_metrics": {"totalUsers": "The total number of unique users.", "newUsers": "The number of new users.", "activeUsers": "The number of active users."},
    "session_metrics": {"sessions": "The total number of sessions.", "bounceRate": "The percentage of sessions that were not engaged.", "averageSessionDuration": "The average duration of a session."},
    "pageview_metrics": {"screenPageViews": "The total number of app screens or web pages viewed."},
    "event_metrics": {"eventCount": "The total number of events.", "conversions": "The total number of conversion events."},
    "ecommerce_metrics": {"totalRevenue": "The total revenue from all sources.", "transactions": "The total number of transactions."}
}

# CRITICAL: Initialize GA4 client at startup to avoid timeout during requests
GA4_CLIENT = None

def get_ga4_client():
    """Get GA4 client, initialize if needed"""
    global GA4_CLIENT
    if GA4_CLIENT is None:
        try:
            GA4_CLIENT = BetaAnalyticsDataClient()
            logger.info("GA4 client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GA4 client: {e}")
            raise
    return GA4_CLIENT

# Tool functions
def list_dimension_categories():
    """List all available GA4 dimension categories"""
    result = {}
    for category, dims in GA4_DIMENSIONS.items():
        result[category] = {"count": len(dims), "dimensions": list(dims.keys())}
    return result

def list_metric_categories():
    """List all available GA4 metric categories"""
    result = {}
    for category, mets in GA4_METRICS.items():
        result[category] = {"count": len(mets), "metrics": list(mets.keys())}
    return result

def get_dimensions_by_category(category):
    """Get dimensions in a specific category"""
    if category in GA4_DIMENSIONS:
        return GA4_DIMENSIONS[category]
    else:
        return {"error": f"Category '{category}' not found. Available: {list(GA4_DIMENSIONS.keys())}"}

def get_metrics_by_category(category):
    """Get metrics in a specific category"""
    if category in GA4_METRICS:
        return GA4_METRICS[category]
    else:
        return {"error": f"Category '{category}' not found. Available: {list(GA4_METRICS.keys())}"}

def get_ga4_data(dimensions=["date"], metrics=["totalUsers", "newUsers"], date_range_start="7daysAgo", date_range_end="yesterday", dimension_filter=None):
    """Retrieve GA4 data"""
    try:
        # Handle string input
        if isinstance(dimensions, str):
            try:
                dimensions = json.loads(dimensions)
            except:
                dimensions = [d.strip() for d in dimensions.split(',')]

        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except:
                metrics = [m.strip() for m in metrics.split(',')]

        # Get GA4 client
        client = get_ga4_client()

        # Build request
        dimension_objects = [Dimension(name=d) for d in dimensions]
        metric_objects = [Metric(name=m) for m in metrics]

        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=dimension_objects,
            metrics=metric_objects,
            date_ranges=[DateRange(start_date=date_range_start, end_date=date_range_end)]
        )

        # Execute request
        response = client.run_report(request)

        # Process response
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
        logger.error(f"Error fetching GA4 data: {e}")
        return {"error": f"Error fetching GA4 data: {str(e)}"}

# FastAPI app
app = FastAPI(title="GA4 MCP Server", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "GA4 MCP Server is running", "status": "ok", "version": "1.0.0"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": time.time()}

@app.post("/")
async def mcp_endpoint(request: Request):
    """Main MCP endpoint - OPTIMIZED FOR SPEED"""
    start_time = time.time()

    try:
        # Parse JSON
        data = await request.json()
        method = data.get("method")
        params = data.get("params", {})
        msg_id = data.get("id", "unknown")

        logger.info(f"MCP Request: {method} (ID: {msg_id})")

        # CRITICAL: Handle initialize immediately - no heavy operations
        if method == "initialize":
            logger.info("Handling initialize - immediate response")

            response = {
                "jsonrpc": "2.0",
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {"listChanged": True}
                    },
                    "serverInfo": {
                        "name": "Google Analytics 4",
                        "version": "1.0.0"
                    }
                },
                "id": msg_id
            }

            elapsed = time.time() - start_time
            logger.info(f"Initialize completed in {elapsed:.2f}s")
            return response

        # Handle tools/list
        elif method == "tools/list":
            logger.info("Handling tools/list")

            tools = [
                {"name": "list_dimension_categories", "description": "List all available GA4 dimension categories", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "list_metric_categories", "description": "List all available GA4 metric categories", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "get_dimensions_by_category", "description": "Get dimensions in a specific category", "inputSchema": {"type": "object", "properties": {"category": {"type": "string"}}, "required": ["category"]}},
                {"name": "get_metrics_by_category", "description": "Get metrics in a specific category", "inputSchema": {"type": "object", "properties": {"category": {"type": "string"}}, "required": ["category"]}},
                {"name": "get_ga4_data", "description": "Retrieve GA4 data", "inputSchema": {"type": "object", "properties": {"dimensions": {"type": "array", "items": {"type": "string"}}, "metrics": {"type": "array", "items": {"type": "string"}}, "date_range_start": {"type": "string"}, "date_range_end": {"type": "string"}}}}
            ]

            elapsed = time.time() - start_time
            logger.info(f"Tools/list completed in {elapsed:.2f}s")
            return {"jsonrpc": "2.0", "result": {"tools": tools}, "id": msg_id}

        # Handle tools/call
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})

            logger.info(f"Calling tool: {tool_name}")

            try:
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
                        date_range_end=tool_args.get("date_range_end", "yesterday")
                    )
                else:
                    return {"jsonrpc": "2.0", "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}, "id": msg_id}

                elapsed = time.time() - start_time
                logger.info(f"Tool {tool_name} completed in {elapsed:.2f}s")

                return {
                    "jsonrpc": "2.0",
                    "result": {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]},
                    "id": msg_id
                }

            except Exception as e:
                logger.error(f"Tool execution error: {e}")
                return {"jsonrpc": "2.0", "error": {"code": -32000, "message": f"Tool error: {str(e)}"}, "id": msg_id}

        # Unknown method
        else:
            return {"jsonrpc": "2.0", "error": {"code": -32601, "message": f"Unknown method: {method}"}, "id": msg_id}

    except Exception as e:
        logger.error(f"Request error: {e}")
        return {"jsonrpc": "2.0", "error": {"code": -32000, "message": f"Request error: {str(e)}"}, "id": "error"}

def main():
    """Main entry point"""
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
        print(f"Starting GA4 MCP server on {args.host}:{args.port}...", file=sys.stderr)

        # Initialize GA4 client at startup
        try:
            get_ga4_client()
            print("✅ GA4 client pre-initialized successfully", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  GA4 client initialization failed: {e}", file=sys.stderr)
            print("Server will continue, but GA4 calls may fail", file=sys.stderr)

        # Run server with optimized settings
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            timeout_keep_alive=60,
            timeout_graceful_shutdown=30,
            access_log=False,  # Disable for performance
            log_level="info"
        )

if __name__ == "__main__":
    main()
