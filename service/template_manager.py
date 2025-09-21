"""Lightweight adapter around the enterprise TemplateEngine.

Goals:
1. Lazy import to avoid slowing Django startup or causing circular imports.
2. Graceful degradation: if engine fails, return empty lists instead of crashing views.
3. Provide a narrow surface consumed by Django admin/simple views.
4. Central place to add caching or permission filtering later.
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional
import threading

_engine_instance = None
_engine_lock = threading.Lock()


def _load_engine():
	"""Internal: import and instantiate the enterprise engine safely."""
	from service.enterprise_template_engine import TemplateEngine  # local import
	return TemplateEngine(validation_strict=False, enable_caching=True)


def get_engine():
	"""Get (singleton) engine instance with thread safety.

	Returns a live TemplateEngine or None if initialization fails.
	"""
	global _engine_instance
	if _engine_instance is not None:
		return _engine_instance
	with _engine_lock:
		if _engine_instance is not None:
			return _engine_instance
		try:
			_engine_instance = _load_engine()
		except Exception:
			_engine_instance = None
	return _engine_instance


def list_templates(include_metadata: bool = True) -> List[Dict[str, Any]]:
	"""List templates from engine; on failure returns []."""
	engine = get_engine()
	if not engine:
		return []
	try:
		return engine.list_templates(include_metadata=include_metadata)
	except Exception:
		return []


def get_template(template_id: str) -> Optional[Dict[str, Any]]:
	engine = get_engine()
	if not engine:
		return None
	try:
		return engine.get_template(template_id)
	except Exception:
		return None


def search_templates(q: str) -> List[Dict[str, Any]]:
	engine = get_engine()
	if not engine:
		return []
	if not q:
		return list_templates(include_metadata=True)
	try:
		return engine.search_templates(q)
	except Exception:
		return []


def engine_health() -> Dict[str, Any]:
	engine = get_engine()
	if not engine:
		return {'ok': False, 'loaded': False}
	try:
		return {
			'ok': True,
			'loaded': True,
			'total': len(engine.list_templates(include_metadata=True)),
		}
	except Exception as e:
		return {'ok': False, 'loaded': True, 'error': str(e)}


__all__ = [
	'get_engine', 'list_templates', 'get_template', 'search_templates', 'engine_health'
]
