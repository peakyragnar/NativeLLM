"""
Embedding utilities for the SEC SQL Agent.
"""

import os
import numpy as np
from anthropic import Anthropic
from openai import OpenAI

def get_anthropic_client():
    """Get an Anthropic client."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    return Anthropic(api_key=api_key)

def get_openai_client():
    """Get an OpenAI client."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    return OpenAI(api_key=api_key)

def get_anthropic_embeddings(texts, model="claude-3-haiku-20240307"):
    """Get embeddings from Anthropic."""
    client = get_anthropic_client()
    
    # Process in batches if needed
    if isinstance(texts, str):
        texts = [texts]
    
    embeddings = []
    for text in texts:
        response = client.embeddings.create(
            model=model,
            input=text
        )
        embeddings.append(response.embedding)
    
    return embeddings

def get_openai_embeddings(texts, model="text-embedding-3-small"):
    """Get embeddings from OpenAI."""
    client = get_openai_client()
    
    # Process in batches if needed
    if isinstance(texts, str):
        texts = [texts]
    
    response = client.embeddings.create(
        model=model,
        input=texts
    )
    
    return [item.embedding for item in response.data]

def get_embeddings(texts, provider="anthropic"):
    """Get embeddings from the specified provider."""
    if provider.lower() == "anthropic":
        return get_anthropic_embeddings(texts)
    elif provider.lower() == "openai":
        return get_openai_embeddings(texts)
    else:
        raise ValueError(f"Unsupported embedding provider: {provider}")

def cosine_similarity(a, b):
    """Calculate cosine similarity between two vectors."""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
