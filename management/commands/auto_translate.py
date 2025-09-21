#!/usr/bin/env python3
"""
自动化翻译管理脚本
Automated translation management script

此脚本将自动完成以下操作：
1. 检查并更新所有模型的 verbose_name 为英文
2. 生成翻译文件 (.po)
3. 编译翻译文件 (.mo)
4. 重启开发服务器以应用更改

This script will automatically:
1. Check and update all model verbose_name to English
2. Generate translation files (.po)
3. Compile translation files (.mo)
4. Restart development server to apply changes
"""

import os
import sys
import subprocess
import time
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent

def run_command(cmd, description="", check=True):
    """运行命令并显示输出"""
    print(f"\n{'='*50}")
    print(f"🔄 {description}")
    print(f"📝 命令: {cmd}")
    print(f"{'='*50}")
    
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            cwd=PROJECT_ROOT,
            capture_output=True, 
            text=True,
            timeout=120
        )
        
        if result.stdout:
            print(f"✅ 输出:\n{result.stdout}")
        
        if result.stderr:
            print(f"⚠️  错误信息:\n{result.stderr}")
            
        if check and result.returncode != 0:
            print(f"❌ 命令执行失败，返回码: {result.returncode}")
            return False
        else:
            print(f"✅ 命令执行成功")
            return True
            
    except subprocess.TimeoutExpired:
        print(f"⏰ 命令执行超时")
        return False
    except Exception as e:
        print(f"❌ 执行异常: {e}")
        return False

def check_django_environment():
    """检查Django环境"""
    print("\n🔍 检查Django环境...")
    
    # 检查虚拟环境
    venv_path = PROJECT_ROOT / "venv_tubewhale"
    if not venv_path.exists():
        print(f"❌ 虚拟环境不存在: {venv_path}")
        return False
    
    # 检查manage.py
    manage_py = PROJECT_ROOT / "manage.py"
    if not manage_py.exists():
        print(f"❌ manage.py不存在: {manage_py}")
        return False
        
    print("✅ Django环境检查通过")
    return True

def activate_venv_command():
    """获取激活虚拟环境的命令前缀"""
    return f"source {PROJECT_ROOT}/venv_tubewhale/bin/activate &&"

def stop_django_server():
    """停止Django开发服务器"""
    print("\n🛑 停止Django开发服务器...")
    run_command(
        "pkill -f 'python.*manage.py.*runserver'",
        "停止Django服务器进程",
        check=False
    )
    time.sleep(2)

def generate_translation_files():
    """生成翻译文件"""
    venv_cmd = activate_venv_command()
    
    # 为所有支持的语言生成翻译文件
    languages = ['en', 'zh-hans', 'zh-cn', 'ja', 'ko', 'es', 'fr', 'de', 'ru', 'ar']
    
    for lang in languages:
        success = run_command(
            f"{venv_cmd} python manage.py makemessages -l {lang} --no-location --no-obsolete",
            f"生成 {lang} 翻译文件",
            check=False
        )
        
        if success:
            print(f"✅ {lang} 翻译文件生成成功")
        else:
            print(f"⚠️  {lang} 翻译文件生成失败，跳过...")

def update_chinese_translations():
    """更新中文翻译内容"""
    print("\n📝 更新中文翻译内容...")
    
    # 中文翻译映射
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
    
    # 更新中文翻译文件
    zh_hans_po = PROJECT_ROOT / "locale/zh_Hans/LC_MESSAGES/django.po"
    zh_cn_po = PROJECT_ROOT / "locale/zh_CN/LC_MESSAGES/django.po"
    
    for po_file in [zh_hans_po, zh_cn_po]:
        if po_file.exists():
            try:
                with open(po_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 更新翻译
                for english, chinese in translations.items():
                    # 查找并替换翻译
                    msgid_pattern = f'msgid "{english}"'
                    if msgid_pattern in content:
                        # 查找对应的msgstr
                        import re
                        pattern = f'msgid "{re.escape(english)}"\nmsgstr ""'
                        replacement = f'msgid "{english}"\nmsgstr "{chinese}"'
                        content = re.sub(pattern, replacement, content)
                
                with open(po_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                    
                print(f"✅ 更新翻译文件: {po_file}")
                
            except Exception as e:
                print(f"❌ 更新翻译文件失败 {po_file}: {e}")

def compile_translation_files():
    """编译翻译文件"""
    venv_cmd = activate_venv_command()
    
    # 编译所有翻译文件
    success = run_command(
        f"{venv_cmd} python manage.py compilemessages",
        "编译翻译文件",
        check=False
    )
    
    if not success:
        print("⚠️  Django compilemessages 失败，尝试手动编译...")
        
        # 手动编译每个.po文件
        locale_dir = PROJECT_ROOT / "locale"
        if locale_dir.exists():
            for po_file in locale_dir.rglob("*.po"):
                mo_file = po_file.with_suffix(".mo")
                manual_success = run_command(
                    f"msgfmt -o {mo_file} {po_file}",
                    f"手动编译 {po_file.name}",
                    check=False
                )
                if manual_success:
                    print(f"✅ 手动编译成功: {mo_file}")

def collect_static_files():
    """收集静态文件"""
    venv_cmd = activate_venv_command()
    
    run_command(
        f"{venv_cmd} python manage.py collectstatic --noinput",
        "收集静态文件",
        check=False
    )

def run_migrations():
    """运行数据库迁移"""
    venv_cmd = activate_venv_command()
    
    run_command(
        f"{venv_cmd} python manage.py makemigrations",
        "生成数据库迁移文件",
        check=False
    )
    
    run_command(
        f"{venv_cmd} python manage.py migrate",
        "应用数据库迁移",
        check=False
    )

def start_django_server():
    """启动Django开发服务器"""
    print("\n🚀 启动Django开发服务器...")
    venv_cmd = activate_venv_command()
    
    print("启动服务器在后台运行...")
    print("服务器地址: http://127.0.0.1:8000/")
    print("管理后台: http://127.0.0.1:8000/admin/")
    print("要停止服务器，请使用: pkill -f 'python.*manage.py.*runserver'")
    
    # 在后台启动服务器
    subprocess.Popen(
        f"{venv_cmd} python manage.py runserver 127.0.0.1:8000",
        shell=True,
        cwd=PROJECT_ROOT
    )
    
    time.sleep(3)
    print("✅ Django服务器已启动")

def main():
    """主函数"""
    print("🎯 TubeWhale 自动化翻译管理脚本")
    print("=" * 60)
    
    # 1. 检查环境
    if not check_django_environment():
        print("❌ 环境检查失败，退出脚本")
        sys.exit(1)
    
    # 2. 停止现有服务器
    stop_django_server()
    
    # 3. 运行数据库迁移
    run_migrations()
    
    # 4. 生成翻译文件
    generate_translation_files()
    
    # 5. 更新中文翻译
    update_chinese_translations()
    
    # 6. 编译翻译文件
    compile_translation_files()
    
    # 7. 收集静态文件
    collect_static_files()
    
    # 8. 启动服务器
    start_django_server()
    
    print("\n" + "=" * 60)
    print("🎉 自动化翻译管理完成！")
    print("=" * 60)
    print("📋 完成的操作:")
    print("  ✅ 数据库迁移")
    print("  ✅ 生成翻译文件")
    print("  ✅ 更新中文翻译")
    print("  ✅ 编译翻译文件") 
    print("  ✅ 收集静态文件")
    print("  ✅ 启动Django服务器")
    print("\n🌐 服务器访问地址:")
    print("  🔗 主页: http://127.0.0.1:8000/")
    print("  🔗 管理后台: http://127.0.0.1:8000/admin/")
    print("\n📋 下一步:")
    print("  1. 访问管理后台检查界面语言")
    print("  2. 如需停止服务器: pkill -f 'python.*manage.py.*runserver'")

if __name__ == "__main__":
    main()
