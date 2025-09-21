from typing import List, Dict, Any
from django.contrib.auth import get_user_model
from .tier_models import UserTier, UserProfile
from .models import UserTemplateSelection, CustomTemplate, TemplateType
# Local helper to avoid circular import with tier_utils
def get_user_profile(user) -> UserProfile:
    profile, _ = UserProfile.objects.get_or_create(
        user=user,
        defaults={'tier': UserTier.BASIC}
    )
    return profile

User = get_user_model()

BASIC_LIMIT = 3


def get_active_selections(user) -> List[UserTemplateSelection]:
    return list(UserTemplateSelection.objects.filter(user=user, active=True).order_by('locked_at'))


def enforce_basic_limit(user):
    sels = get_active_selections(user)
    if len(sels) <= BASIC_LIMIT:
        return
    # prune oldest beyond limit
    for s in sels[:-BASIC_LIMIT]:
        s.active = False
        s.save(update_fields=['active'])


def select_template_for_basic(user, template_id: str, template_name: str, template_type: str) -> UserTemplateSelection:
    sels = get_active_selections(user)
    if any(s.template_id == template_id for s in sels):
        return [s for s in sels if s.template_id == template_id][0]
    if len(sels) >= BASIC_LIMIT:
        raise ValueError(f"Selection limit {BASIC_LIMIT} reached")
    return UserTemplateSelection.objects.create(
        user=user,
        template_id=template_id,
        template_name=template_name,
        template_type=template_type,
    )


def user_can_create_custom(user) -> bool:
    profile = get_user_profile(user)
    return profile.tier == UserTier.ENTERPRISE or user.is_staff


def user_has_access_to_template(user, template_id: str, template_type: str) -> bool:
    profile = get_user_profile(user)
    if user.is_staff:
        return True
    if profile.tier == UserTier.ENTERPRISE:
        return True
    if profile.tier == UserTier.PRO and template_type in [TemplateType.CORE, TemplateType.DOMAIN]:
        return True
    if profile.tier == UserTier.BASIC:
        # must be selected and active
        return UserTemplateSelection.objects.filter(user=user, template_id=template_id, active=True).exists()
    return False


def list_effective_templates(user, builtin_templates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    builtin_templates: list of dict objects each with id, name, template_type
    """
    profile = get_user_profile(user)
    if user.is_staff or profile.tier in [UserTier.PRO, UserTier.ENTERPRISE]:
        return builtin_templates
    if profile.tier == UserTier.BASIC:
        selected_ids = {s.template_id for s in get_active_selections(user)}
        return [t for t in builtin_templates if t.get('id') in selected_ids]
    return []


# ================== Reconciliation & Signals ==================
from django.db.models.signals import post_save
from django.dispatch import receiver


def reconcile_user_selections(user):
    """Ensure BASIC user does not exceed BASIC_LIMIT (prune oldest)."""
    profile = get_user_profile(user)
    if profile.tier == UserTier.BASIC:
        enforce_basic_limit(user)


@receiver(post_save, sender=UserProfile)
def _userprofile_post_save(sender, instance: UserProfile, created: bool, **kwargs):  # pragma: no cover - signal
    # On tier changes (including creation) enforce selection limits if BASIC.
    try:
        reconcile_user_selections(instance.user)
    except Exception:
        # Defensive: do not break save pipeline
        pass
