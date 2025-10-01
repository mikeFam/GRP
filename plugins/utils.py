import os, json, hashlib, pathlib, re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

def ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def env_or_default(name: str, default: str = "") -> str:
    return os.environ.get(name, default)

def expand_env_template(value: str) -> str:
    # Supports ${ENV:NAME} replacement within strings
    def repl(match):
        var = match.group(1)
        return os.environ.get(var, "")
    return re.sub(r"\$\{ENV:([A-Z0-9_]+)\}", repl, value)
