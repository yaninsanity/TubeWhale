"""
Tier-based template access control utilities
"""
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from django.contrib.auth import get_user_model
from .tier_models import UserProfile, UserTier, TemplateUsage
from .models import UserTemplateSelection, TemplateType
from .access_control import get_active_selections, BASIC_LIMIT

if TYPE_CHECKING:
    from django.contrib.auth.models import AbstractUser

User = get_user_model()


def get_user_profile(user) -> UserProfile:
    """Get or create user profile with tier information"""
    profile, created = UserProfile.objects.get_or_create(
        user=user,
        defaults={'tier': UserTier.BASIC}
    )
    return profile


def can_access_template(user, template_id: str, template_type: str | None = None) -> bool:
    """Selection-aware access check.

    Rules:
    - Staff: all templates
    - ENTERPRISE: all templates (builtin + custom)
    - PRO: all builtin (core/domain), custom if profile.can_customize flag
    - BASIC: only templates explicitly selected (UserTemplateSelection.active)
    """
    if not user.is_authenticated:
        return False
    if user.is_staff:
        return True
    profile = get_user_profile(user)
    # Enterprise unrestricted
    if profile.tier == UserTier.ENTERPRISE:
        return True
    # Pro: access all builtin; custom if allowed (future flag)
    if profile.tier == UserTier.PRO:
        if template_type in [TemplateType.CORE, TemplateType.DOMAIN, 'core', 'domain']:
            return True
        # For custom templates, rely on profile.can_customize or staff override
        if template_type in [TemplateType.CUSTOM, 'custom'] and getattr(profile, 'can_customize', False):
            return True
        return False
    # Basic: must be in active selections
    if profile.tier == UserTier.BASIC:
        return UserTemplateSelection.objects.filter(user=user, template_id=template_id, active=True).exists()
    return False


def filter_accessible_templates(user, templates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter template list based on selection-aware access rules."""
    if not user.is_authenticated:
        return []
    if user.is_staff:
        return templates
    profile = get_user_profile(user)
    if profile.tier in [UserTier.PRO, UserTier.ENTERPRISE]:
        return templates
    # BASIC: include only selected
    selected_ids = {s.template_id for s in get_active_selections(user)}
    return [t for t in templates if t.get('id') in selected_ids]


def track_template_usage(user, template_id: str, template_name: str = None):
    """Track template usage for analytics and limiting"""
    if not user.is_authenticated:
        return
    
    usage, created = TemplateUsage.objects.get_or_create(
        user=user,
        template_id=template_id,
        defaults={'template_name': template_name or template_id}
    )
    
    if not created:
        usage.usage_count += 1
        if template_name:
            usage.template_name = template_name
        usage.save()


def get_user_template_stats(user) -> Dict[str, Any]:
    """Selection-aware stats.

    For BASIC users we report active selection counts (not historical usage).
    For higher tiers we retain legacy semantics but 'remaining' is None (unlimited).
    """
    if not user.is_authenticated:
        return {}
    profile = get_user_profile(user)
    if profile.tier == UserTier.BASIC:
        active_count = UserTemplateSelection.objects.filter(user=user, active=True).count()
        max_allowed = min(profile.max_templates, BASIC_LIMIT)
        return {
            'tier': profile.tier,
            'tier_display': profile.get_tier_display(),
            'max_templates': max_allowed,
            'used_templates': active_count,
            'remaining_templates': max(0, max_allowed - active_count),
            'can_customize': False,
            'can_access_all_builtin': False,
        }
    # PRO / ENTERPRISE
    return {
        'tier': profile.tier,
        'tier_display': profile.get_tier_display(),
        'max_templates': None,
        'used_templates': None,
        'remaining_templates': None,
        'can_customize': getattr(profile, 'can_customize', False),
        'can_access_all_builtin': True,
    }