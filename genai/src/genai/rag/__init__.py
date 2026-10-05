"""Hybrid RAG over `rag.chunks`: `search(query, k=10) -> list[Hit]`, `dedupe_products(hits)`."""

from genai.rag.search import Hit, dedupe_products, search

__all__ = ["Hit", "dedupe_products", "search"]
