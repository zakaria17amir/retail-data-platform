import os
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.runnables import Runnable
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ValidationError

MAX_ATTEMPTS = 3


class StructuredOutputError(Exception):
    pass


def chat_model(alias: str = "chat") -> BaseChatModel:
    """OpenAI-compatible client pointed at the LiteLLM proxy; never a vendor endpoint."""
    key = os.environ.get("LITELLM_MASTER_KEY", "")
    if not key:
        raise RuntimeError("LITELLM_MASTER_KEY is not set")
    return ChatOpenAI(
        model=alias,
        base_url=os.environ.get("LITELLM_URL", "http://127.0.0.1:4000"),
        api_key=key,  # type: ignore[arg-type]
        temperature=0,
        timeout=120,
    )


def _json_text(message: BaseMessage) -> str:
    text = str(message.content).strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    return fenced.group(1) if fenced else text


def invoke_structured[M: BaseModel](
    llm: Runnable[Any, BaseMessage], messages: list[BaseMessage], model: type[M]
) -> M:
    """JSON reply validated by Pydantic; the validation error is fed back, max 3 attempts."""
    history = list(messages)
    error = ""
    for _ in range(MAX_ATTEMPTS):
        reply = llm.invoke(history)
        try:
            return model.model_validate_json(_json_text(reply))
        except ValidationError as exc:
            error = str(exc)
            history += [
                AIMessage(content=str(reply.content)),
                HumanMessage(content=f"Invalid JSON: {error}\nReply with corrected JSON only."),
            ]
    raise StructuredOutputError(f"{model.__name__} rejected after {MAX_ATTEMPTS} attempts: {error}")
