#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env from project root
    dotenv_path = Path(__file__).resolve().parent / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        print(f"✅ Loaded .env from {dotenv_path}")
    else:
        print(f"⚠️  No .env file found at {dotenv_path}")
except ImportError:
    print("⚠️  python-dotenv not installed, skipping .env loading")


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tubewhale_project.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
