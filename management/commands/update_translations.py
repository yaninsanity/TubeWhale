"""
Django管理命令：自动化翻译更新
Auto Translation Update Management Command

使用方法:
python manage.py update_translations
python manage.py update_translations --languages zh-hans,en
python manage.py update_translations --compile-only
"""

import os
import re
import subprocess
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command
from django.conf import settings


class Command(BaseCommand):
    help = '自动化翻译文件更新和编译 / Automated translation file update and compilation'

    def add_arguments(self, parser):
        parser.add_argument(
            '--languages',
            type=str,
            default=','.join([code for code, _ in getattr(settings, 'LANGUAGES', [('en','English')])]),
            help='要处理的语言列表，用逗号分隔 (默认: settings.LANGUAGES)'
        )
        parser.add_argument(
            '--compile-only',
            action='store_true',
            help='仅编译现有翻译文件，不生成新的'
        )
        parser.add_argument(
            '--update-chinese',
            action='store_true',
            default=True,
            help='自动更新中文翻译内容'
        )
        parser.add_argument(
            '--no-collectstatic',
            action='store_true',
            help='跳过静态文件收集'
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('🎯 开始自动化翻译更新...\n')
        )

        try:
            languages = [lang.strip() for lang in options['languages'].split(',')]
            
            # 1. 生成翻译文件 (除非仅编译)
            if not options['compile_only']:
                self.generate_translation_files(languages)
            
            # 2. 更新中文翻译内容
            if options['update_chinese'] and ('zh-hans' in languages or 'zh-cn' in languages):
                self.update_chinese_translations()
            
            # 3. 编译翻译文件
            self.compile_translation_files()
            
            # 4. 收集静态文件
            if not options['no_collectstatic']:
                self.collect_static_files()
            
            self.stdout.write(
                self.style.SUCCESS('\n🎉 翻译更新完成！')
            )
            self.stdout.write(
                'ℹ️  请重启Django服务器以应用更改: python manage.py runserver'
            )
            
        except Exception as e:
            raise CommandError(f'翻译更新失败: {e}')

    def generate_translation_files(self, languages):
        """生成翻译文件"""
        self.stdout.write('📝 生成翻译文件...')
        
        for lang in languages:
            self.stdout.write(f'  🔄 处理语言: {lang}')
            try:
                call_command(
                    'makemessages',
                    locale=[lang],
                    no_location=True,
                    no_obsolete=True,
                    verbosity=0
                )
                self.stdout.write(
                    self.style.SUCCESS(f'    ✅ {lang} 翻译文件生成成功')
                )
            except Exception as e:
                self.stdout.write(
                    self.style.WARNING(f'    ⚠️  {lang} 翻译文件生成失败: {e}')
                )

    def update_chinese_translations(self):
        """自动更新中文翻译内容"""
        self.stdout.write('🔄 更新中文翻译内容...')
        
        # 翻译映射
        translations = {
            "User Management": "用户管理",
            "User": "用户",
            "API Key": "API密钥", 
            "API Key Management": "API密钥管理",
            "API Usage Log": "API使用日志",
            "API Usage Log Management": "API使用日志管理",
            "Invitation Code": "邀请码",
            "Invitation Code Management": "邀请码管理",
            "Expert Domain": "专家领域",
            "Expert Domain Management": "专家领域管理",
            "Analysis Report": "分析报告", 
            "Analysis Report Management": "分析报告管理",
            "Analysis Metric": "分析指标",
            "Analysis Metric Management": "分析指标管理", 
            "Analysis Insight": "分析洞察",
            "Analysis Insight Management": "分析洞察管理",
            "Video": "视频",
            "Video Management": "视频管理",
            "Analysis Task": "分析任务",
            "Analysis Task Management": "分析任务管理",
            "Video Comment": "视频评论",
            "Video Comment Management": "视频评论管理",
            "Video Tag": "视频标签",
            "Video Tag Management": "视频标签管理"
        }
        
        # 查找中文翻译文件
        locale_root = Path(settings.BASE_DIR) / 'locale'
        po_files = []
        
        # 支持多种中文locale命名
        for chinese_locale in ['zh_Hans', 'zh_CN', 'zh-hans', 'zh-cn']:
            po_path = locale_root / chinese_locale / 'LC_MESSAGES' / 'django.po'
            if po_path.exists():
                po_files.append(po_path)
        
        for po_file in po_files:
            try:
                with open(po_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                updated_count = 0
                # 更新翻译
                for english, chinese in translations.items():
                    # 查找并替换空的翻译
                    pattern = f'msgid "{re.escape(english)}"\nmsgstr ""'
                    replacement = f'msgid "{english}"\nmsgstr "{chinese}"'
                    
                    if re.search(pattern, content):
                        content = re.sub(pattern, replacement, content)
                        updated_count += 1
                
                # 写回文件
                with open(po_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                    
                self.stdout.write(
                    self.style.SUCCESS(f'  ✅ 更新 {po_file.name}: {updated_count} 条翻译')
                )
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'  ❌ 更新翻译文件失败 {po_file}: {e}')
                )

    def compile_translation_files(self):
        """编译翻译文件"""
        self.stdout.write('🔨 编译翻译文件...')
        
        try:
            # 尝试使用Django的compilemessages命令
            call_command('compilemessages', verbosity=0)
            self.stdout.write(
                self.style.SUCCESS('  ✅ Django compilemessages 成功')
            )
        except Exception as django_error:
            self.stdout.write(
                self.style.WARNING(f'  ⚠️  Django compilemessages 失败: {django_error}')
            )
            self.stdout.write('  🔄 尝试手动编译...')
            
            # 手动编译每个.po文件
            locale_root = Path(settings.BASE_DIR) / 'locale'
            compiled_count = 0
            
            for po_file in locale_root.rglob('*.po'):
                mo_file = po_file.with_suffix('.mo')
                try:
                    subprocess.run(
                        ['msgfmt', '-o', str(mo_file), str(po_file)],
                        check=True,
                        capture_output=True
                    )
                    compiled_count += 1
                    self.stdout.write(f'    ✅ 编译: {po_file.name}')
                except (subprocess.CalledProcessError, FileNotFoundError) as e:
                    self.stdout.write(
                        self.style.WARNING(f'    ⚠️  编译失败 {po_file.name}: {e}')
                    )
            
            if compiled_count > 0:
                self.stdout.write(
                    self.style.SUCCESS(f'  ✅ 手动编译完成: {compiled_count} 个文件')
                )

    def collect_static_files(self):
        """收集静态文件"""
        self.stdout.write('📦 收集静态文件...')
        
        try:
            call_command('collectstatic', interactive=False, verbosity=0)
            self.stdout.write(
                self.style.SUCCESS('  ✅ 静态文件收集成功')
            )
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f'  ⚠️  静态文件收集失败: {e}')
            )
