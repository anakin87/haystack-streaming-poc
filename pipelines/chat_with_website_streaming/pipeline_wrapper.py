from collections.abc import AsyncGenerator
from pathlib import Path

from haystack import AsyncPipeline
from haystack.dataclasses import StreamingChunk

from hayhooks import BasePipelineWrapper, get_last_user_message, log

URLS = ["https://haystack.deepset.ai", "https://www.redis.io", "https://ssi.inc"]


class PipelineWrapper(BasePipelineWrapper):
    """
    Chat-with-website pipeline using `AsyncPipeline.stream()` from the haystack
    streaming-poc branch, instead of Hayhooks's `async_streaming_generator`.

    The pipeline fetches a fixed set of URLs, converts the HTML to documents, builds
    a chat prompt from them, and asks the LLM to answer the user's question.
    """

    def setup(self) -> None:
        pipeline_yaml = (Path(__file__).parent / "chat_with_website.yml").read_text()
        self.pipeline = AsyncPipeline.loads(pipeline_yaml)

    async def run_api_async(self, urls: list[str], question: str) -> str:
        """Non-streaming async endpoint that returns the final answer as a string."""
        log.trace("Running pipeline with urls: {} and question: {}", urls, question)
        result = await self.pipeline.run_async({"fetcher": {"urls": urls}, "prompt": {"query": question}})
        return result["llm"]["replies"][0].text

    async def run_chat_completion_async(self, model: str, messages: list[dict], body: dict) -> AsyncGenerator:
        """
        OpenAI-compatible chat completion endpoint that streams LLM tokens as they arrive.

        Uses `AsyncPipeline.stream(...)` directly: it returns a handle that is async-iterable
        over `StreamingChunk`s. We re-yield each chunk; Hayhooks formats them into the
        OpenAI streaming response shape.
        """
        log.trace("Running pipeline with model: {}, messages: {}, body: {}", model, messages, body)
        question = get_last_user_message(messages)

        # Why the inner generator (instead of `return self.pipeline.stream(...)` directly):
        # Hayhooks does `isinstance(result, AsyncGenerator)` on the awaited value (see
        # `server/routers/openai.py`). `PipelineStreamHandle` is async-iterable but is
        # not an `AsyncGenerator` (it also exposes `.result` and `.aclose`, which the
        # bare protocol cannot express), so it fails that check. Cleanest fix lives on
        # the Hayhooks side: accept any `AsyncIterable` (or skip the check on the
        # streaming path). Then this inner generator could go away.
        async def stream_chunks() -> AsyncGenerator[StreamingChunk, None]:
            handle = self.pipeline.stream(data={"fetcher": {"urls": URLS}, "prompt": {"query": question}})
            async for chunk in handle:
                yield chunk
            log.info("Assembled reply: {}", handle.result["llm"]["replies"][0])

        return stream_chunks()
