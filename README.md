# Elasticsearch Sampling Proxy

A reverse proxy for Elasticsearch that allows sampling of documents from bulk ingest requests based on configurable percentage and condition.

## Requirements

- Python 3.8+
- Elasticsearch (tested with 7.x/8.x)
- pip

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/your-org/sampling_proxy.git
   cd sampling_proxy
   ```

2. Install dependencies:

   ```bash
   pip install aiohttp
   ```

   (You may want to use a virtual environment.)

## Configuration

Edit `proxy.py` to set your Elasticsearch host, username, and password:

```python
ES_HOST = 'http://localhost:9200'
ES_USERNAME = 'elastic'
ES_PASSWORD = 'changeme'
```

## Running the Proxy

Start the proxy server:

```bash
python proxy.py
```

By default, the proxy listens on `localhost:3123` and forwards requests to your configured Elasticsearch.

## Usage

### Proxying Elasticsearch

Point your Elasticsearch clients to `http://localhost:3123` instead of directly to Elasticsearch.

### Sampling API

#### Start Sampling

Enable sampling for a datastream (index):

```bash
curl -XPOST 'http://localhost:3123/my-index/_samples' \
  -H 'Content-Type: application/json' \
  -d '{"percentage": 10, "if": "ctx.http.response.status_code >= 400"}'
```

- `percentage`: Percentage of docs to sample (0-100).
- `if`: Elasticsearch ingest pipeline condition (see [docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/condition.html)).

#### Get Samples

Retrieve sampled documents:

```bash
curl 'http://localhost:3123/my-index/_samples'
```

#### Stop Sampling

Disable sampling for a datastream:

```bash
curl -XDELETE 'http://localhost:3123/my-index/_samples'
```

## Notes

- Only `_bulk` ingest requests are sampled.
- Samples are stored in memory (max 100 per datastream).
- The proxy supports CORS for the sampling API.
