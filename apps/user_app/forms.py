"""
User App Forms
Registration and authentication forms with invitation code validation
"""

from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError
from .models import User, InvitationCode
from datetime import datetime


class InvitationCodeRegistrationForm(UserCreationForm):
    """
    Registration form with invitation code validation
    """
    
    invitation_code = forms.CharField(
        max_length=20,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('Enter invitation code'),
            'autocomplete': 'off'
        }),
        label=_('Invitation Code'),
        help_text=_('You need a valid invitation code to register')
    )
    
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': _('Enter your email')
        }),
        label=_('Email Address')
    )
    
    first_name = forms.CharField(
        max_length=30,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('First name')
        }),
        label=_('First Name')
    )
    
    last_name = forms.CharField(
        max_length=30,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('Last name')
        }),
        label=_('Last Name')
    )
    
    phone_number = forms.CharField(
        max_length=20,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('Phone number (optional)')
        }),
        label=_('Phone Number')
    )
    
    class Meta:
        model = User
        fields = ('username', 'first_name', 'last_name', 'email', 'phone_number', 
                 'invitation_code', 'password1', 'password2')
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Add CSS classes to all fields
        for field_name, field in self.fields.items():
            if field_name not in ['invitation_code', 'email', 'first_name', 'last_name', 'phone_number']:
                field.widget.attrs['class'] = 'form-control'
    
    def clean_invitation_code(self):
        """Validate invitation code"""
        from django.utils import timezone
        
        code = self.cleaned_data.get('invitation_code')
        
        if not code:
            raise ValidationError(_('Invitation code is required'))
        
        try:
            invitation = InvitationCode.objects.get(code=code.upper())
        except InvitationCode.DoesNotExist:
            raise ValidationError(_('Invalid invitation code'))
        
        # Check if code is valid
        if not invitation.is_valid():
            if invitation.current_uses >= invitation.max_uses:
                raise ValidationError(_('This invitation code has been used up'))
            elif timezone.now() > invitation.expires_at:
                raise ValidationError(_('This invitation code has expired'))
            else:
                raise ValidationError(_('This invitation code is not valid'))
        
        # Store the invitation object for later use
        self._invitation_code_obj = invitation
        return code.upper()
    
    def clean_email(self):
        """Validate email and check domain restrictions"""
        email = self.cleaned_data.get('email')
        
        if not email:
            raise ValidationError(_('Email is required'))
        
        # Check if email is already registered
        if User.objects.filter(email=email).exists():
            raise ValidationError(_('A user with this email already exists'))
        
        # Check invitation code domain restrictions
        if hasattr(self, '_invitation_code_obj'):
            if not self._invitation_code_obj.can_be_used_by_email(email):
                raise ValidationError(_('Your email domain is not allowed for this invitation code'))
        
        return email
    
    def save(self, commit=True):
        """Save user and mark invitation code as used"""
        user = super().save(commit=False)
        user.email = self.cleaned_data['email']
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        user.phone_number = self.cleaned_data.get('phone_number', '')
        user.is_verified = True  # Auto-verify users with invitation codes
        
        if commit:
            user.save()
            # Mark invitation code as used
            if hasattr(self, '_invitation_code_obj'):
                self._invitation_code_obj.use_code(user)
        
        return user


class InvitationCodeCreationForm(forms.ModelForm):
    """
    Form for admins to create invitation codes
    """
    
    expires_days = forms.IntegerField(
        initial=30,
        min_value=1,
        max_value=365,
        widget=forms.NumberInput(attrs={'class': 'form-control'}),
        label=_('Expires in (days)'),
        help_text=_('Number of days until this code expires')
    )
    
    class Meta:
        model = InvitationCode
        fields = ('max_uses', 'notes', 'allowed_domains')
        widgets = {
            'max_uses': forms.NumberInput(attrs={'class': 'form-control', 'min': 1}),
            'notes': forms.Textarea(attrs={'class': 'form-control', 'rows': 3}),
            'allowed_domains': forms.Textarea(attrs={
                'class': 'form-control', 
                'rows': 2,
                'placeholder': 'example.com, company.org'
            }),
        }
    
    def save(self, created_by, commit=True):
        """Create invitation code with calculated expiry"""
        expires_days = self.cleaned_data['expires_days']
        max_uses = self.cleaned_data['max_uses']
        notes = self.cleaned_data['notes']
        allowed_domains = self.cleaned_data['allowed_domains']
        
        return InvitationCode.create_invitation_code(
            created_by=created_by,
            expires_days=expires_days,
            max_uses=max_uses,
            notes=notes,
            allowed_domains=allowed_domains
        )
