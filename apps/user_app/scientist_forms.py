"""
Scientist Authentication Forms
User-friendly forms for researcher registration and login
"""

from django import forms
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth import get_user_model
from django.utils.translation import gettext_lazy as _

User = get_user_model()


class ScientistRegistrationForm(UserCreationForm):
    """Registration form for scientist users"""
    
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': _('Enter your email address')
        }),
        help_text=_('We\'ll use this to send you research updates and notifications.')
    )
    
    first_name = forms.CharField(
        max_length=30,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('First name')
        })
    )
    
    last_name = forms.CharField(
        max_length=30,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('Last name')
        })
    )
    
    research_field = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('e.g., Media Studies, Communication, Psychology')
        }),
        help_text=_('Your research field or academic discipline (optional)')
    )
    
    institution = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('University or research institution')
        }),
        help_text=_('Your affiliated institution (optional)')
    )
    
    class Meta:
        model = User
        fields = ('username', 'email', 'first_name', 'last_name', 'password1', 'password2')
        
        widgets = {
            'username': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': _('Choose a username')
            }),
        }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Update password field styling
        self.fields['password1'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': _('Create a secure password')
        })
        self.fields['password2'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': _('Confirm your password')
        })
        
        # User-friendly help texts
        self.fields['username'].help_text = _('Letters, digits and @/./+/-/_ only.')
        self.fields['password1'].help_text = _('Your password must contain at least 8 characters.')
    
    def save(self, commit=True):
        user = super().save(commit=False)
        user.email = self.cleaned_data['email']
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        
        # Set default tier for new scientist users
        user.tier = 'basic'  # Start with basic tier
        
        if commit:
            user.save()
            
            # Add research info to user profile if provided
            research_field = self.cleaned_data.get('research_field')
            institution = self.cleaned_data.get('institution')
            
            # You can extend this to save additional profile data
            # For now, we'll just log it or save it to user profile
            
        return user


class ScientistLoginForm(AuthenticationForm):
    """Login form for scientist users"""
    
    username = forms.CharField(
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': _('Username or email'),
            'autofocus': True
        })
    )
    
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': _('Password')
        })
    )
    
    remember_me = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input'
        }),
        label=_('Remember me for 30 days')
    )


class ProfileUpdateForm(forms.ModelForm):
    """Form for updating scientist profile"""
    
    class Meta:
        model = User
        fields = ('first_name', 'last_name', 'email')
        
        widgets = {
            'first_name': forms.TextInput(attrs={'class': 'form-control'}),
            'last_name': forms.TextInput(attrs={'class': 'form-control'}),
            'email': forms.EmailInput(attrs={'class': 'form-control'}),
        }