"""
Management command to create invitation codes
"""

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from apps.user_app.models import InvitationCode


User = get_user_model()


class Command(BaseCommand):
    help = 'Create invitation codes for beta users'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--count',
            type=int,
            default=1,
            help='Number of invitation codes to create (default: 1)'
        )
        parser.add_argument(
            '--expires-days',
            type=int,
            default=30,
            help='Number of days until codes expire (default: 30)'
        )
        parser.add_argument(
            '--max-uses',
            type=int,
            default=1,
            help='Maximum uses per code (default: 1)'
        )
        parser.add_argument(
            '--created-by',
            type=str,
            help='Username of admin who creates the codes'
        )
        parser.add_argument(
            '--notes',
            type=str,
            default='',
            help='Notes for the invitation codes'
        )
        parser.add_argument(
            '--allowed-domains',
            type=str,
            default='',
            help='Comma-separated list of allowed email domains'
        )
    
    def handle(self, *args, **options):
        count = options['count']
        expires_days = options['expires_days']
        max_uses = options['max_uses']
        created_by_username = options['created_by']
        notes = options['notes']
        allowed_domains = options['allowed_domains']
        
        # Get or create the admin user
        if created_by_username:
            try:
                created_by = User.objects.get(username=created_by_username)
            except User.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f'User "{created_by_username}" does not exist')
                )
                return
        else:
            # Try to get the first superuser
            created_by = User.objects.filter(is_superuser=True).first()
            if not created_by:
                self.stdout.write(
                    self.style.ERROR('No superuser found. Please create one first or specify --created-by')
                )
                return
        
        # Create invitation codes
        created_codes = []
        for i in range(count):
            code = InvitationCode.create_invitation_code(
                created_by=created_by,
                expires_days=expires_days,
                max_uses=max_uses,
                notes=notes,
                allowed_domains=allowed_domains
            )
            created_codes.append(code)
        
        # Display results
        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {count} invitation code(s):')
        )
        
        for code in created_codes:
            self.stdout.write(f'  Code: {code.code}')
            self.stdout.write(f'    Expires: {code.expires_at}')
            self.stdout.write(f'    Max uses: {code.max_uses}')
            if code.allowed_domains:
                self.stdout.write(f'    Allowed domains: {code.allowed_domains}')
            self.stdout.write('')
