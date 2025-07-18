import asyncio
import aiohttp
from aiohttp import web
import json
import re
import random
from collections import deque
import base64
import uuid
import urllib.parse

# --- Configuration ---
ES_HOST = 'http://localhost:9200'
PROXY_HOST = 'localhost'
PROXY_PORT = 3123

# Basic Auth credentials for the underlying Elasticsearch
ES_USERNAME = 'elastic'
ES_PASSWORD = 'changeme'
ES_AUTH_HEADER = f"Basic {base64.b64encode(f'{ES_USERNAME}:{ES_PASSWORD}'.encode()).decode()}"

# In-memory storage for active sampling sessions and their collected documents.
# Structure:
# {
#   "datastream-name": {
#     "percentage": 0.5,
#     "if": "ctx.http.response.status_code >= 400",
#     "samples": deque([...], maxlen=100)
#   }
# }
SAMPLING_SESSIONS = {}

# --- Helper Functions ---

def parse_bulk_body(body_text):
    """Parses an NDJSON bulk body into a list of docs with _index, _id, and _source."""
    docs = []
    lines = body_text.strip().split('\n')
    for i in range(0, len(lines), 2):
        try:
            action_meta = json.loads(lines[i])
            # The action line is a dict with a single key (e.g., "index", "create", etc.)
            action_type, meta = next(iter(action_meta.items()))
            index = meta.get('_index')
            doc_id = meta.get('_id') or str(uuid.uuid4())
            if not index:
                continue
            source = json.loads(lines[i+1])
            docs.append({"_index": index, "_id": doc_id, "_source": source})
        except (IndexError, json.JSONDecodeError, StopIteration, AttributeError):
            # Ignore malformed lines
            continue
    return docs

async def run_simulation(session, docs, pipeline_def=None, endpoint="_ingest/pipeline/_simulate"):
    """Runs a simulation against the specified Elasticsearch simulate endpoint."""
    simulate_url = f"{ES_HOST}{endpoint}"
    payload = {"docs": docs}
    if pipeline_def:
        payload["pipeline"] = pipeline_def

    try:
        async with session.post(simulate_url, json=payload) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Error during simulation: {response.status} {await response.text()}")
                return None
    except Exception as e:
        print(f"Exception during simulation request: {e}")
        return None


async def process_sampling_for_bulk(body_text, app_session):
    """The core logic for sampling documents from a bulk request."""
    if not SAMPLING_SESSIONS:
        return # No active sessions, nothing to do.

    docs_to_simulate = parse_bulk_body(body_text)
    if not docs_to_simulate:
        return # No valid documents found in bulk request.

    # Step 1: Run a general simulation to find out the destination index for all docs.
    # Use the _ingest/_simulate endpoint (no pipeline specified).
    initial_simulation_result = await run_simulation(
        app_session, docs_to_simulate, pipeline_def=None, endpoint="/_ingest/_simulate"
    )
    if not initial_simulation_result or "docs" not in initial_simulation_result:
        return

    # Map from _id to the original doc as seen in the bulk request
    original_doc_map = { doc["_id"]: doc["_source"] for doc in docs_to_simulate }

    # A map of {destination_index: [list_of_transformed_docs]}
    docs_by_destination = {}
    for result in initial_simulation_result["docs"]:
        if "doc" in result and "_index" in result["doc"]:
            dest_index = result["doc"]["_index"]
            if dest_index not in docs_by_destination:
                docs_by_destination[dest_index] = []
            docs_by_destination[dest_index].append(result["doc"])

    # Step 2: For each active session, check if any documents match.
    for stream_name, session_config in SAMPLING_SESSIONS.items():
        if stream_name not in docs_by_destination:
            continue

        candidate_docs = docs_by_destination[stream_name]

        # Only keep docs that pass the percentage sampling
        sampled_docs = []
        sampled_doc_ids = []
        for doc in candidate_docs:
            doc_id = doc.get("_id")
            if random.random() < (session_config['percentage'] / 100.0):
                sampled_docs.append(doc)
                sampled_doc_ids.append(doc_id)
        if not sampled_docs:
            continue

        # Create a temporary pipeline to check the user's 'if' condition.
        # The 'drop' processor will remove docs that don't match the condition.
        conditional_pipeline = {
            "processors": [
                {
                    "drop": {
                        "if": f"!({session_config['if']})"
                    }
                }
            ]
        }

        # Run the second simulation with the conditional pipeline.
        conditional_sim_result = await run_simulation(
            app_session, sampled_docs, conditional_pipeline, endpoint="/_ingest/pipeline/_simulate"
        )
        if not conditional_sim_result or "docs" not in conditional_sim_result:
            continue

        # Documents that were NOT dropped are our matches.
        added_count = 0
        for idx, result in enumerate(conditional_sim_result.get("docs", [])):
            # The 'doc' key exists if the processor ran, but is missing if dropped.
            if result and "doc" in result:
                doc_id = result["doc"].get("_id")
                # Use the original doc from the bulk request
                orig_doc = original_doc_map.get(doc_id)
                if orig_doc is not None:
                    session_config["samples"].append(orig_doc)
                    added_count += 1
        if added_count > 0:
            print(f"Sampled {added_count} document(s) for '{stream_name}'")


# --- Request Handlers ---

def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET,POST,DELETE,OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    return response

async def handle_options(request):
    # Handles CORS preflight requests
    resp = web.Response(status=204)
    add_cors_headers(resp)
    return resp

async def handle_bulk_request(request):
    """
    Handles _bulk requests. It proxies the request immediately
    and then processes sampling in the background.
    """
    body = await request.read()
    
    # Start the sampling logic as a background task. It won't block the response.
    # We pass the application's client session to the task.
    asyncio.create_task(process_sampling_for_bulk(body.decode('utf-8', 'ignore'), request.app['es_session']))
    
    # Immediately proxy the original request to Elasticsearch.
    return await proxy_request(request, body_override=body)


async def handle_start_sampling(request):
    """API: POST /<datastream_name>/_samples"""
    if request.method == 'OPTIONS':
        return await handle_options(request)
    datastream_name = request.match_info['datastream_name']
    try:
        data = await request.json()
        percentage = float(data['percentage'])
        if_condition = str(data['if'])
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        resp = web.json_response({"error": f"Invalid request body. Required: 'percentage' (number) and 'if' (string). Details: {e}"}, status=400)
        return add_cors_headers(resp)

    SAMPLING_SESSIONS[datastream_name] = {
        "percentage": percentage,
        "if": if_condition,
        "samples": deque(maxlen=100)
    }
    print(f"Started sampling for datastream '{datastream_name}'")
    resp = web.json_response({"acknowledged": True, "message": f"Sampling enabled for '{datastream_name}'."})
    return add_cors_headers(resp)

async def handle_get_samples(request):
    """API: GET /<datastream_name>/_samples"""
    if request.method == 'OPTIONS':
        return await handle_options(request)
    datastream_name = request.match_info['datastream_name']
    if datastream_name in SAMPLING_SESSIONS:
        samples = list(SAMPLING_SESSIONS[datastream_name]["samples"])
        resp = web.json_response({"count": len(samples), "samples": samples})
        return add_cors_headers(resp)
    else:
        resp = web.json_response({"error": "No active sampling session found for this datastream."}, status=404)
        return add_cors_headers(resp)

async def handle_stop_sampling(request):
    """API: DELETE /<datastream_name>/_samples"""
    if request.method == 'OPTIONS':
        return await handle_options(request)
    datastream_name = request.match_info['datastream_name']
    if datastream_name in SAMPLING_SESSIONS:
        del SAMPLING_SESSIONS[datastream_name]
        print(f"Stopped sampling for datastream '{datastream_name}'")
        resp = web.json_response({"acknowledged": True, "message": f"Sampling disabled for '{datastream_name}'."})
        return add_cors_headers(resp)
    else:
        resp = web.json_response({"error": "No active sampling session found for this datastream."}, status=404)
        return add_cors_headers(resp)

async def proxy_request(request, body_override=None):
    """Proxies all other requests to the actual Elasticsearch instance."""
    es_session = request.app['es_session']
    
    es_url = f"{ES_HOST}{request.url.path}"
    if request.url.query_string:
        es_url += f"?{request.url.query_string}"

    headers = request.headers.copy()
    
    # Let aiohttp handle content-length and transfer-encoding for the outbound request.
    headers.pop('Content-Length', None)
    headers.pop('Transfer-Encoding', None)

    # --- Handle basic auth in the incoming URL ---
    # If the request URL contains userinfo, use it for Authorization header.
    parsed_url = urllib.parse.urlparse(str(request.url))
    if parsed_url.username and parsed_url.password:
        userpass = f"{parsed_url.username}:{parsed_url.password}"
        basic_auth = base64.b64encode(userpass.encode()).decode()
        headers['Authorization'] = f"Basic {basic_auth}"
    # else: leave the default Authorization header set by the session

    # Use the body passed from the bulk handler if available, otherwise read from the request.
    data = body_override if body_override is not None else await request.read()
    
    try:
        async with es_session.request(
            method=request.method,
            url=es_url,
            headers=headers,
            data=data,
            allow_redirects=False
        ) as es_response:
            response_body = await es_response.read()
            response_headers = es_response.headers.copy()
            # Clean hop-by-hop headers from the response from Elasticsearch.
            response_headers.pop('Transfer-Encoding', None)
            response_headers.pop('Content-Encoding', None) # Often problematic

            # Ensure response_body is bytes, even if empty
            if response_body is None:
                response_body = b''

            # Only set Content-Length if upstream had it and we did not change encoding
            if 'Content-Length' in response_headers:
                response_headers['Content-Length'] = str(len(response_body))
            else:
                # If upstream did not send Content-Length, do not set it
                response_headers.pop('Content-Length', None)

            # Debug: print response headers and body length
            # print(f"Proxy response headers: {response_headers}")
            # print(f"Proxy response body length: {len(response_body)}")

            resp = web.Response(
                status=es_response.status,
                headers=response_headers,
                body=response_body
            )
            add_cors_headers(resp)
            return resp
    except aiohttp.ClientConnectorError as e:
        print(f"Error connecting to Elasticsearch: {e}")
        resp = web.Response(status=503, text="Could not connect to Elasticsearch")
        add_cors_headers(resp)
        return resp
    except Exception as e:
        print(f"An error occurred during proxying: {e}")
        resp = web.Response(status=500, text="Internal Proxy Error")
        add_cors_headers(resp)
        return resp

# --- Application Setup ---

async def startup(app):
    """Create a persistent client session on application startup."""
    headers = {'Authorization': ES_AUTH_HEADER}
    app['es_session'] = aiohttp.ClientSession(headers=headers)
    print("Created persistent aiohttp.ClientSession for Elasticsearch.")

async def cleanup(app):
    """Close the client session on application cleanup."""
    await app['es_session'].close()
    print("Closed aiohttp.ClientSession.")

async def main():
    app = web.Application()
    
    # Setup persistent session
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    # Add routes for the sampling feature.
    # The regex ':.+' allows datastream names with dots or slashes.
    app.router.add_post('/{datastream_name:.+}/_samples', handle_start_sampling)
    app.router.add_get('/{datastream_name:.+}/_samples', handle_get_samples)
    app.router.add_delete('/{datastream_name:.+}/_samples', handle_stop_sampling)
    
    # Add specific routes for _bulk requests. These must come before the general proxy.
    app.router.add_post('/_bulk', handle_bulk_request)
    app.router.add_put('/_bulk', handle_bulk_request)
    # This also catches /<index>/_bulk
    app.router.add_post('/{index_name:.+}/_bulk', handle_bulk_request)
    app.router.add_put('/{index_name:.+}/_bulk', handle_bulk_request)

    # The general proxy route must be last as it's a catch-all.
    app.router.add_route('*', '/{path:.*}', proxy_request)

    # Add CORS preflight (OPTIONS) handlers for the sampling API routes
    app.router.add_options('/{datastream_name:.+}/_samples', handle_options)
    app.router.add_options('/_bulk', handle_options)
    app.router.add_options('/{index_name:.+}/_bulk', handle_options)
    app.router.add_options('/{path:.*}', handle_options)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, PROXY_HOST, PROXY_PORT)
    print(f"Starting reverse proxy on http://{PROXY_HOST}:{PROXY_PORT}")
    print(f"Proxying requests to Elasticsearch at {ES_HOST}")
    await site.start()

    # Wait forever
    await asyncio.Event().wait()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nReverse proxy stopped.")