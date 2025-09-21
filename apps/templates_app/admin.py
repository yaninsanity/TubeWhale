from django.contrib import admin
from django.urls import path, reverse
from django.shortcuts import redirect
from django.contrib import messages
from django.utils.html import format_html
from django.template.loader import render_to_string
from django.http import HttpResponse
from django.utils.translation import gettext_lazy as _
from .models import (
    CustomTemplate,
    ExpertPrompt,
    CLIExecution,
    Job,
    JobStatus,
    SystemHotspotEvent,
)
from .tier_models import UserProfile, TemplateUsage
# from service.enterprise_template_engine import TemplateEngine


class TemplatePreviewMixin:
    """Mixin to add template preview functionality to admin classes"""

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                '<path:object_id>/preview/',
                self.admin_site.admin_view(self.template_preview_view),
                name=f'{self.model._meta.app_label}_{self.model._meta.model_name}_preview'
            ),
        ]
        return custom_urls + urls

    def template_preview_view(self, request, object_id):
        """Inline template preview view"""
        try:
            obj = self.get_object(request, object_id)
            if not obj:
                messages.error(request, "Template not found")
                return redirect('..')

            # (Engine integration temporarily disabled; avoid NameError if engine not available)
            template_data = None

            # Prepare context for preview
            context = {
                'template': obj,
                'engine_template': template_data,
                'is_popup': True,
            }

            return render_to_string('admin/template_preview_popup.html', context, request)

        except Exception as e:
            messages.error(request, f"Preview error: {e}")
            return redirect('..')

    def preview_link(self, obj):
        """Generate preview link for list view"""
        url = reverse(f'admin:{self.model._meta.app_label}_{self.model._meta.model_name}_preview',
                     args=[obj.pk])
        return format_html('<a href="{}" target="_blank" class="button">👁️ Preview</a>', url)
    preview_link.short_description = _("Preview")


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ['user', 'tier', 'max_templates', 'can_customize', 'created_at']
    list_filter = ['tier', 'created_at']
    search_fields = ['user__username', 'user__email']
    readonly_fields = ['created_at', 'updated_at']

    fieldsets = (
        (None, {
            'fields': ('user', 'tier')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(TemplateUsage)
class TemplateUsageAdmin(admin.ModelAdmin):
    list_display = ['user', 'template_name', 'usage_count', 'last_used']
    list_filter = ['last_used', 'created_at']
    search_fields = ['user__username', 'template_id', 'template_name']
    readonly_fields = ['created_at', 'last_used']

    def has_add_permission(self, request):
        return False  # Only track usage automatically


@admin.register(CustomTemplate)
class CustomTemplateAdmin(TemplatePreviewMixin, admin.ModelAdmin):
    list_display = ("template_id", "name", "domain", "template_type", "version", "updated_at", "preview_link")
    list_filter = ("domain", "template_type")
    search_fields = ("template_id", "name", "description", "tags")
    ordering = ("template_id",)

    readonly_fields = ('template_id', 'created_at', 'updated_at', 'applicable_expert_prompts')

    fieldsets = (
        (None, {
            'fields': ('template_id', 'name', 'domain', 'description')
        }),
        ('Template Content', {
            'fields': ('prompt', 'parameters'),
            'classes': ('collapse',)
        }),
        ('Configuration', {
            'fields': ('template_type', 'immutable', 'version', 'tags'),
            'classes': ('collapse',)
        }),
        ('Expert Coverage', {
            'fields': ('applicable_expert_prompts',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    actions = ['bulk_preview', 'export_templates']

    def bulk_preview(self, request, queryset):
        """Bulk preview action"""
        if queryset.count() == 1:
            template = queryset.first()
            url = reverse(f'admin:{self.model._meta.app_label}_{self.model._meta.model_name}_preview',
                         args=[template.pk])
            return redirect(url)
        else:
            self.message_user(request, f"Select exactly one template to preview. Selected: {queryset.count()}")
    bulk_preview.short_description = _("👁️ Preview Selected Template")

    def export_templates(self, request, queryset):
        """Export selected templates"""
        # Implementation for export functionality
        self.message_user(request, f"Exported {queryset.count()} templates")
    export_templates.short_description = _("📤 Export Templates")

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        if obj and obj.immutable:
            # Make fields readonly for immutable templates
            for field_name in ['name', 'prompt', 'parameters']:
                if field_name in form.base_fields:
                    form.base_fields[field_name].disabled = True
        return form

    def save_model(self, request, obj, form, change):
        """Track template usage when modified"""
        super().save_model(request, obj, form, change)

        # Track usage for auditing
        try:
            from .tier_utils import track_template_usage
            track_template_usage(request.user, obj.template_id, obj.name, action='modified')
        except:
            pass  # Don't fail if tracking fails

    def applicable_expert_prompts(self, obj):  # pragma: no cover - presentation logic
        qs = ExpertPrompt.objects.filter(active=True, domain=obj.domain).order_by('weight')
        if not qs.exists():
            return "(none for domain)"
        return ", ".join(f"{e.slug}" for e in qs)
    applicable_expert_prompts.short_description = _("Expert Prompts (by domain)")


@admin.register(ExpertPrompt)
class ExpertPromptAdmin(admin.ModelAdmin):
    list_display = ("slug", "colored_role", "colored_domain", "active_badge", "weight", "updated_at", "preview_fragment")
    list_filter = ("role", "domain", "active")
    search_fields = ("slug", "title", "description", "prompt_intro", "prompt_outro")
    ordering = ("weight", "role")
    list_editable = ("weight",)
    readonly_fields = ("created_at", "updated_at")
    actions = ["activate_prompts", "deactivate_prompts", "reseed_prompts"]
    fieldsets = (
        (None, {"fields": ("slug", "title", "role", "domain", "active", "weight")}),
        ("Content", {"fields": ("description", "prompt_intro", "prompt_outro"), "classes": ("collapse",)}),
        ("Timestamps", {"fields": ("created_at", "updated_at"), "classes": ("collapse",)}),
        ("Preview", {"description": "Rendered composition order: intro → base prompt (external) → outro.", "fields": tuple()}),
    )

    # ------------- Custom list column helpers -------------
    def colored_role(self, obj):  # pragma: no cover (presentation)
        palette = {
            'business_strategist': '#2563eb',
            'general_analyst': '#7c3aed',
            'policy_analyst': '#d97706',
            'medical_researcher': '#dc2626',
            'technology_architect': '#0891b2',
            'academic_scholar': '#065f46',
            'investor': '#334155',
        }
        color = palette.get(obj.role, '#374151')
        return format_html('<span style="padding:2px 6px;border-radius:4px;background:{};color:#fff;font-size:12px;">{}</span>', color, obj.get_role_display())
    colored_role.short_description = _("Role")

    def colored_domain(self, obj):  # pragma: no cover
        colors = {
            'general': '#6b7280',
            'business': '#1d4ed8',
            'medical': '#dc2626',
            'technology': '#0ea5e9',
            'academic': '#15803d',
        }
        color = colors.get(obj.domain, '#6b7280')
        return format_html('<strong style="color:{};">{}</strong>', color, obj.get_domain_display())
    colored_domain.short_description = _("Domain")

    def active_badge(self, obj):  # pragma: no cover
        if obj.active:
            return format_html('<span style="color:#065f46;font-weight:600;">● Active</span>')
        return format_html('<span style="color:#b91c1c;font-weight:600;">● Inactive</span>')
    active_badge.short_description = _("Status")

    def preview_fragment(self, obj):  # pragma: no cover
        frag = (obj.prompt_intro or '')[:40].strip()
        if frag:
            return format_html('<span title="Intro fragment">{}…</span>', frag)
        return "—"
    preview_fragment.short_description = _("Intro")

    # ------------- Bulk actions -------------
    def activate_prompts(self, request, queryset):
        updated = queryset.update(active=True)
        self.message_user(request, f"Activated {updated} prompts")
    activate_prompts.short_description = _("✅ Activate selected")

    def deactivate_prompts(self, request, queryset):
        updated = queryset.update(active=False)
        self.message_user(request, f"Deactivated {updated} prompts")
    deactivate_prompts.short_description = _("🚫 Deactivate selected")

    def reseed_prompts(self, request, queryset):
        from django.core.management import call_command
        call_command('seed_templates_and_prompts', '--force')
        # After reseed, we may have new prompts beyond current queryset
        self.message_user(request, "Reseed complete. Refresh to see full canonical set (should be 9).")
    reseed_prompts.short_description = _("♻️ Reseed canonical (force)")

    # ------------- Empty-state guidance -------------
    def changelist_view(self, request, extra_context=None):
        from django.core.management import call_command
        qs_count = ExpertPrompt.objects.count()
        missing = 9 - qs_count
        banner = None
        if qs_count < 9:
            banner = {
                'type': 'warning',
                'message': f"Only {qs_count} expert prompts present. {missing} missing. Use action '♻️ Reseed canonical (force)' to restore full set.",
            }
        extra_context = extra_context or {}
        extra_context['tw_expert_banner'] = banner
        return super().changelist_view(request, extra_context=extra_context)


@admin.register(CLIExecution)
class CLIExecutionAdmin(admin.ModelAdmin):
    list_display = ("id", "command", "status", "template_id", "expert_slug", "returncode", "duration_ms", "created_at")
    list_filter = ("command", "status", "created_at")
    search_fields = ("command", "template_id", "expert_slug", "stdout", "stderr")
    readonly_fields = (
        "command", "args", "template_id", "expert_slug", "prompt_final", "stdout", "stderr",
        "status", "returncode", "duration_ms", "meta", "created_at", "updated_at"
    )
    ordering = ("-created_at",)
    fieldsets = (
        (None, {"fields": ("command", "status", "returncode", "duration_ms")}),
        ("Context", {"fields": ("template_id", "expert_slug", "args", "meta"), "classes": ("collapse",)}),
        ("Prompt", {"fields": ("prompt_final",), "classes": ("collapse",)}),
        ("Output", {"fields": ("stdout", "stderr"), "classes": ("collapse",)}),
        ("Timestamps", {"fields": ("created_at", "updated_at"), "classes": ("collapse",)}),
    )
    def has_add_permission(self, request):
        return False
    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "command", "status", "template_id", "expert_slug", "created_at", "finished_at")
    list_filter = ("status", "command")
    search_fields = ("command", "template_id", "expert_slug", "error")
    readonly_fields = ("celery_task_id", "execution_id", "created_at", "updated_at", "finished_at", "error")

@admin.register(SystemHotspotEvent)
class SystemHotspotEventAdmin(admin.ModelAdmin):
    list_display = ("resource", "level", "peak_percent", "started_at", "ended_at", "resolved")
    list_filter = ("resource", "level", "resolved", "started_at")
    search_fields = ("resource", "level")
    readonly_fields = ("started_at", "ended_at", "duration_sec", "resolved", "peak_percent", "meta")
    ordering = ("-started_at",)
    def has_add_permission(self, request):
        return False
    def has_change_permission(self, request, obj=None):
        return False
