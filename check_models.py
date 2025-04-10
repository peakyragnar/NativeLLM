#!/usr/bin/env python3
"""
Check available OpenAI models.
"""

import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key
api_key = os.environ.get("OPENAI_API_KEY")
print(f"API key starts with: {api_key[:10]}...")

# Create client
client = OpenAI(api_key=api_key)

# List models
try:
    models = client.models.list()
    print("Available models:")
    for model in models.data:
        print(f"- {model.id}")
except Exception as e:
    print(f"Error: {e}")
