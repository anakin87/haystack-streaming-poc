# haystack-streaming-poc

Two ways to try `AsyncPipeline.stream()` from the haystack `streaming-poc` branch:

1. **`demo.py`** — a small standalone async script that runs a BM25 RAG pipeline
   and streams the LLM answer to the terminal. Also shows the error path.
2. **`pipelines/chat_with_website_streaming/`** — a tiny Hayhooks app exposing the
   new streaming method through an OpenAI-compatible chat-completion endpoint.

## Layout

```
haystack-streaming-poc/
├── pyproject.toml
├── demo.py
└── pipelines/
    └── chat_with_website_streaming/
        ├── chat_with_website.yml
        └── pipeline_wrapper.py
```

## Setup

`pyproject.toml` pins `haystack-ai` to the `streaming-poc` branch of
`github.com/deepset-ai/haystack` via `[tool.uv.sources]`, so `uv` will fetch
that branch directly when syncing.

```bash
uv sync
export OPENAI_API_KEY=sk-...
```

## Run the standalone demo

```bash
uv run python demo.py
```

Runs two paths in sequence: a happy path that streams a RAG answer, and an
error path that triggers a failure mid-call and shows how it surfaces through
`async for` and `handle.result`.

## Run the Hayhooks app

```bash
uv run hayhooks run --pipelines-dir pipelines
```

Then in another terminal:

```bash
# non-streaming endpoint
curl -X POST http://localhost:1416/chat_with_website_streaming/run \
  -H 'Content-Type: application/json' \
  -d '{"urls": ["https://haystack.deepset.ai"], "question": "What is Haystack?"}'

# streaming chat-completion endpoint
curl -N -X POST http://localhost:1416/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "chat_with_website_streaming",
    "messages": [{"role": "user", "content": "What is Haystack?"}],
    "stream": true
  }'
```

## What this demonstrates

- `AsyncPipeline.stream(data=...)` returns a `PipelineStreamHandle` that is async-iterable
  over `StreamingChunk`s. After iteration ends, `handle.result` holds the final pipeline
  output (used in `demo.py`, logged but not returned by the Hayhooks wrapper).
- Composes cleanly with Hayhooks's OpenAI-compatible chat completion endpoint, since
  the wrapper only needs to yield `StreamingChunk` objects.

## Note on the inner generator (worth discussing with Hayhooks)

The wrapper goes through an inner `stream_chunks` generator instead of doing
`return self.pipeline.stream(...)` directly. The constraint is on the Hayhooks
side: `server/routers/openai.py` does `isinstance(result, AsyncGenerator)` on
the awaited value, and `PipelineStreamHandle` is async-iterable but is not an
`AsyncGenerator` (it also exposes `.result` and `.aclose`, which the bare
protocol cannot express).

Cleanest fix lives on the Hayhooks side: accept any `AsyncIterable`, or skip
the type check on the streaming path. Then the wrapper could simply do
`return self.pipeline.stream(data=...)` and the inner generator would go away.
