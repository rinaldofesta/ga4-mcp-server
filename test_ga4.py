import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient

# Set credentials path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/rinaldofesta/dev/ga4-mcp/keys/ga4-mcp-server-466008-a4ce9318d0e0.json"

# Test connection
try:
    client = BetaAnalyticsDataClient()
    print("✅ GA4 credentials working!")
except Exception as e:
    print(f"❌ Errore: {e}")
