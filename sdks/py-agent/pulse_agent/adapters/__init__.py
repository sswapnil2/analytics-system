"""Zero-dependency adapters for OpenAI, Anthropic, Google Gemini, LangChain, and RAG."""

from .anthropic import anthropic_adapter, wrap_anthropic
from .gemini import gemini_adapter, wrap_gemini
from .langchain import LangChainCallbackHandler, PulseLangChainCallbackHandler
from .openai import openai_adapter, wrap_openai
from .rag import rag_adapter, trace_rag_retrieval, wrap_retriever

__all__ = [
    "wrap_openai",
    "openai_adapter",
    "wrap_anthropic",
    "anthropic_adapter",
    "wrap_gemini",
    "gemini_adapter",
    "LangChainCallbackHandler",
    "PulseLangChainCallbackHandler",
    "trace_rag_retrieval",
    "rag_adapter",
    "wrap_retriever",
]
