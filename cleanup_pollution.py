#!/usr/bin/env python3
"""
TubeWhale 项目清理脚本
清理开发过程中产生的临时文件和污染文件
"""

import os
import shutil
import glob
from pathlib import Path

def main():
    print("🧹 TubeWhale 项目清理脚本")
    print("=" * 50)
    
    project_root = Path.cwd()
    
    # 要删除的文件模式
    file_patterns = [
        # 开发临时文件
        "*_enhanced.py",
        "*_clean.py", 
        "*_new.py",
        "*_temp.py",
        "*_test.py",
        "*_debug.py",
        "whale_*.py",
        "*_academic.py",
        "test_*.py",
        "debug_*.py",
        "temp*.py",
        "*_temporary.py",
        
        # 开发文档
        "*_REPORT.md",
        "*_ARCHITECTURE.md", 
        "*_PLAN.md",
        "README_*.md",
        "CLEANUP_*.md",
        "RECOVERY_*.md",
        "TRANSLATION_*.md",
        "*_SPEC.md",
        
        # 开发脚本
        "*.sh",
        "auto_*.py",
        "complete_*.py",
        "final_*.py",
        "fix_*.py",
        "force_*.py",
        "manage_*.py",
        "performance_*.py",
        "quick_*.py",
        "simple_*.py",
        "update_*.py",
        "clean_*.py",
        
        # 配置文件
        "Dockerfile",
        "docker-compose.yml",
        "docker-entrypoint.sh",
        "*.yaml",
        "pyproject.toml",
        "nginx.conf",
        
        # 日志文件
        "*.log",
        "cookies.txt",
        "init_db.sql",
        
        # Flask remnants
        "flask_requirements.txt",
        "admin_panel.py",
    ]
    
    # 要删除的目录
    dir_patterns = [
        "core",
        "docs", 
        "gradio_interface",
        "management",
        "requirements",
        "scripts",
        "services",
        "static",
        "tests",
        "tubewhale_platform",
        ".github",
        "docker",
        "service/examples",
        "service/templates",
    ]
    
    files_to_delete = []
    dirs_to_delete = []
    
    # 收集要删除的文件
    for pattern in file_patterns:
        for file_path in glob.glob(str(project_root / pattern)):
            if os.path.isfile(file_path):
                files_to_delete.append(file_path)
    
    # 收集要删除的目录
    for dir_pattern in dir_patterns:
        dir_path = project_root / dir_pattern
        if dir_path.exists() and dir_path.is_dir():
            dirs_to_delete.append(str(dir_path))
    
    print(f"🔍 发现 {len(files_to_delete)} 个文件和 {len(dirs_to_delete)} 个目录需要清理")
    
    if files_to_delete:
        print("\n📄 将删除的文件:")
        for file_path in files_to_delete[:10]:  # 只显示前10个
            print(f"   ❌ {os.path.relpath(file_path)}")
        if len(files_to_delete) > 10:
            print(f"   ... 还有 {len(files_to_delete) - 10} 个文件")
    
    if dirs_to_delete:
        print("\n📁 将删除的目录:")
        for dir_path in dirs_to_delete:
            print(f"   ❌ {os.path.relpath(dir_path)}/")
    
    print("\n" + "=" * 50)
    response = input("❓ 确认要清理这些文件和目录吗？(y/N): ")
    
    if response.lower() != 'y':
        print("🚫 取消清理操作")
        return
    
    print("\n🧹 开始清理...")
    
    # 删除文件
    deleted_files = 0
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            deleted_files += 1
        except Exception as e:
            print(f"❌ 删除文件失败: {file_path} - {e}")
    
    # 删除目录
    deleted_dirs = 0
    for dir_path in dirs_to_delete:
        try:
            shutil.rmtree(dir_path)
            deleted_dirs += 1
            print(f"✅ 删除目录: {os.path.relpath(dir_path)}/")
        except Exception as e:
            print(f"❌ 删除目录失败: {dir_path} - {e}")
    
    print(f"\n🎉 清理完成！删除了 {deleted_files} 个文件和 {deleted_dirs} 个目录")
    
    print("\n📋 保留的核心文件:")
    print("   ✅ Django 项目文件 (apps/, tubewhale_project/)")
    print("   ✅ 原有核心功能 (agents/, utils/, main.py)")
    print("   ✅ 配置文件 (requirements.txt, .env.example)")
    print("   ✅ 文档文件 (README.md, LICENSE)")
    print("   ✅ 数据文件 (*.db, downloads/, logs/)")
    
    print("\n🚀 推荐接下来:")
    print("   1. git add . && git commit -m 'Clean up development pollution'")
    print("   2. 检查 .gitignore 确保未来不会再次污染")
    print("   3. 继续使用干净的 Django 项目结构")

if __name__ == "__main__":
    main()
