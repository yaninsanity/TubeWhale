from __future__ import annotations

import io
import os
from datetime import datetime
from typing import Dict

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.conf import settings

from dotenv import dotenv_values


ENV_FILE = os.path.join(settings.BASE_DIR, ".env")
BACKUP_PATTERN = ".env.bak_%Y%m%d_%H%M%S"

# Only allow updating these keys via Admin to minimize risk
ALLOWED_KEYS = [
    "KEYWORD",
    "YOUTUBE_API_KEYS",
    "OPENAI_API_KEY",
    "DB_PATH",
    "PERSIST_AGENT_SUMMARIES",
    "FULL_AUDIO_ANALYSIS",
    "DRY_RUN",
    "MAX_N",
    "TOP_K",
    "FILTER_TYPE",
    "CONCURRENCY",
    "PURE_YOUTUBE",
    # Optional CLI remote config
    "CLI_CONFIG_URL",
    "CLI_CONFIG_AUTH",
]


def _read_env_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _write_env_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _update_env_content(original: str, updates: Dict[str, str]) -> str:
    # Preserve unknown lines and comments; update only ALLOWED_KEYS if present or append at end.
    lines = original.splitlines() if original else []
    kv_index = {k: None for k in ALLOWED_KEYS}
    # Index existing allowed keys
    for idx, line in enumerate(lines):
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        k = line.split("=", 1)[0].strip()
        if k in kv_index and kv_index[k] is None:
            kv_index[k] = idx

    # Apply updates (only for keys in ALLOWED_KEYS)
    for k, v in updates.items():
        if k not in ALLOWED_KEYS:
            continue
        # Normalize YOUTUBE_API_KEYS to newline-separated for readability
        if k == "YOUTUBE_API_KEYS":
            # Accept comma/semicolon/newline/space separated input
            tmp = v.replace("\t", "\n").replace(";", "\n").replace(",", "\n")
            parts = [p.strip() for p in tmp.splitlines() if p.strip()]
            v = "\n".join(parts)
        # Quote values that contain spaces or special chars
        needs_quote = any(ch in v for ch in [" ", "#", "\n"])
        val = f'"{v}"' if needs_quote and not (v.startswith('"') and v.endswith('"')) else v

        assignment = f"{k}={val}"
        if kv_index.get(k) is not None:
            lines[kv_index[k]] = assignment
        else:
            lines.append(assignment)
    return "\n".join(lines) + ("\n" if lines else "")


@staff_member_required
def env_config_view(request: HttpRequest) -> HttpResponse:
    # Load existing values
    env_map = dotenv_values(ENV_FILE) if os.path.exists(ENV_FILE) else {}
    context = {"env": {k: env_map.get(k, "") for k in ALLOWED_KEYS}, "env_path": ENV_FILE}

    if request.method == "POST":
        # Build updates from POST safely
        updates: Dict[str, str] = {}
        for k in ALLOWED_KEYS:
            updates[k] = request.POST.get(k, "").strip()

        original = _read_env_text(ENV_FILE)

        # Backup
        backup_name = datetime.now().strftime(BACKUP_PATTERN)
        backup_path = os.path.join(settings.BASE_DIR, backup_name)
        try:
            if original:
                _write_env_text(backup_path, original)
        except Exception:
            messages.warning(request, f"Failed to create .env backup at {backup_path}")

        # Write new content
        try:
            new_content = _update_env_content(original, updates)
            _write_env_text(ENV_FILE, new_content)
            messages.success(request, ".env updated successfully. Restart may be required to take effect.")
            return redirect(reverse("admin-env-config"))
        except Exception as e:
            messages.error(request, f"Failed to update .env: {e}")

    return render(request, "admin/env_config.html", context)
