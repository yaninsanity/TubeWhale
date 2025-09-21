from django import template
from django.conf import settings
from django.utils.translation import get_language, gettext as _
from urllib.parse import urlencode

register = template.Library()


@register.inclusion_tag('admin/partials/language_dropdown.html', takes_context=True)
def language_dropdown(context):
    """Render a language dropdown based on settings.LANGUAGES.

    Adds current language, builds change URLs preserving query parameters.
    """
    request = context['request']
    current = get_language()
    langs = getattr(settings, 'LANGUAGES', [('en', 'English')])

    # Normalize current to lower for matching (zh-hans vs zh-Hans)
    current_lower = (current or 'en').lower()
    items = []
    for code, label in langs:
        active = code.lower() == current_lower
        # preserve existing query params but override admin_lang
        params = request.GET.copy()
        params['admin_lang'] = code
        url = f"{request.path}?{urlencode(params, doseq=True)}"
        items.append({
            'code': code,
            'label': label,
            'active': active,
            'url': url,
        })
    return {
        'languages': items,
        'current_code': current,
        'request': request,
        'title': _('Language'),
    }
