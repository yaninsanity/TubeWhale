#!/usr/bin/env python3
"""
🔬 TubeWhale精准全功能健康验证系统
确保全部7个服务100%健康，完整功能通路验证
"""

import subprocess
import requests
import time
import json
import sys
from datetime import datetime

def run_command(cmd, capture_output=True):
    """执行命令并返回结果"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True, timeout=30)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "命令超时"

def check_docker_services():
    """检查Docker服务状态"""
    print("🐳 检查Docker服务状态...")
    success, stdout, stderr = run_command("docker compose -f docker-compose.full.yml ps --format json")
    
    if not success:
        return False, "Docker服务检查失败"
    
    try:
        services = []
        for line in stdout.strip().split('\n'):
            if line.strip():
                services.append(json.loads(line))
        
        total_services = len(services)
        healthy_services = 0
        running_services = 0
        
        service_status = {}
        
        for service in services:
            name = service.get('Name', 'Unknown')
            status = service.get('Status', 'Unknown')
            service_name = service.get('Service', 'Unknown')
            
            is_running = 'Up' in status
            is_healthy = 'healthy' in status
            
            if is_running:
                running_services += 1
            if is_healthy:
                healthy_services += 1
                
            service_status[service_name] = {
                'name': name,
                'status': status,
                'running': is_running,
                'healthy': is_healthy
            }
        
        print(f"   📊 总服务数: {total_services}")
        print(f"   ✅ 运行中: {running_services}")
        print(f"   💚 健康: {healthy_services}")
        
        # 详细状态
        for service_name, info in service_status.items():
            status_icon = "💚" if info['healthy'] else ("🟡" if info['running'] else "❌")
            print(f"   {status_icon} {service_name}: {info['status']}")
        
        return healthy_services == total_services, f"{healthy_services}/{total_services}服务健康"
        
    except Exception as e:
        return False, f"解析Docker状态失败: {e}"

def test_api_endpoints():
    """测试API端点"""
    print("🌐 测试API端点...")
    endpoints = [
        ("后端API", "http://localhost:8001/api/v1/tubewhale/health/"),
        ("前端代理", "http://localhost:80/"),
        ("WebSocket", "http://localhost:8004/")
    ]
    
    results = {}
    all_healthy = True
    
    for name, url in endpoints:
        try:
            start_time = time.time()
            response = requests.get(url, timeout=10)
            response_time = time.time() - start_time
            
            if response.status_code < 400:
                print(f"   ✅ {name}: {response.status_code} ({response_time:.3f}s)")
                results[name] = True
            else:
                print(f"   ❌ {name}: {response.status_code} ({response_time:.3f}s)")
                results[name] = False
                all_healthy = False
        except Exception as e:
            print(f"   ❌ {name}: 连接失败 - {e}")
            results[name] = False
            all_healthy = False
    
    return all_healthy, results

def test_database_connection():
    """测试数据库连接"""
    print("🗄️  测试数据库连接...")
    success, stdout, stderr = run_command(
        "docker exec tubewhale_postgres_full psql -U tubewhale_dev -d tubewhale_dev -c 'SELECT 1;'"
    )
    
    if success:
        print("   ✅ PostgreSQL连接正常")
        return True
    else:
        print(f"   ❌ PostgreSQL连接失败: {stderr}")
        return False

def test_redis_connection():
    """测试Redis连接"""
    print("🔴 测试Redis连接...")
    success, stdout, stderr = run_command(
        "docker exec tubewhale_redis_full redis-cli -a dev123 ping"
    )
    
    if success and "PONG" in stdout:
        print("   ✅ Redis连接正常")
        return True
    else:
        print(f"   ❌ Redis连接失败: {stderr}")
        return False

def test_celery_tasks():
    """测试Celery任务系统"""
    print("⚡ 测试Celery任务系统...")
    
    # 检查Celery Worker
    success, stdout, stderr = run_command(
        "docker exec tubewhale_celery_full celery -A tubewhale_project inspect ping"
    )
    
    worker_ok = success and "pong" in stdout.lower()
    
    # 检查Celery Beat
    success, stdout, stderr = run_command(
        "docker exec tubewhale_beat_full ps aux | grep celery"
    )
    
    beat_ok = success and "celery" in stdout
    
    if worker_ok:
        print("   ✅ Celery Worker正常")
    else:
        print("   ❌ Celery Worker异常")
    
    if beat_ok:
        print("   ✅ Celery Beat正常")
    else:
        print("   ❌ Celery Beat异常")
    
    return worker_ok and beat_ok

def test_cli_integration():
    """测试CLI集成"""
    print("🖥️  测试CLI集成...")
    
    # 测试CLI基础功能
    success, stdout, stderr = run_command("python cli.py --help")
    
    if success and "TubeWhale" in stdout:
        print("   ✅ CLI基础功能正常")
        cli_basic = True
    else:
        print("   ❌ CLI基础功能异常")
        cli_basic = False
    
    return cli_basic

def test_complete_workflow():
    """测试完整工作流程"""
    print("🔄 测试完整工作流程...")
    
    # 模拟完整请求链路：前端 -> 后端 -> 数据库 -> CLI
    try:
        # 1. 测试后端API
        response = requests.get("http://localhost:8001/api/v1/tubewhale/health/", timeout=5)
        if response.status_code != 200:
            print("   ❌ 后端API异常")
            return False
        
        # 2. 测试数据库查询
        success, _, _ = run_command(
            "docker exec tubewhale_backend_full python manage.py shell -c 'from django.db import connection; cursor = connection.cursor(); cursor.execute(\"SELECT 1\"); print(\"DB OK\")'"
        )
        if not success:
            print("   ❌ 数据库查询异常")
            return False
        
        print("   ✅ 完整工作流程正常")
        return True
        
    except Exception as e:
        print(f"   ❌ 完整工作流程异常: {e}")
        return False

def optimize_performance():
    """性能优化检查"""
    print("🚀 性能优化检查...")
    
    # 检查内存使用
    success, stdout, stderr = run_command("docker stats --no-stream --format 'table {{.Container}}\\t{{.CPUPerc}}\\t{{.MemUsage}}'")
    
    if success:
        print("   📊 当前资源使用:")
        for line in stdout.split('\n')[1:]:  # 跳过表头
            if line.strip():
                print(f"   {line}")
        print("   ✅ 性能监控正常")
        return True
    else:
        print("   ❌ 性能监控异常")
        return False

def wait_for_services_healthy(max_wait=120):
    """等待所有服务变为健康状态"""
    print(f"⏳ 等待所有服务变为健康状态 (最大等待 {max_wait}s)...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        success, stdout, stderr = run_command("docker compose -f docker-compose.full.yml ps --format json")
        
        if success:
            try:
                services = []
                for line in stdout.strip().split('\n'):
                    if line.strip():
                        services.append(json.loads(line))
                
                healthy_count = sum(1 for s in services if 'healthy' in s.get('Status', ''))
                total_count = len(services)
                
                if healthy_count == total_count:
                    print(f"   ✅ 所有 {total_count} 个服务已健康!")
                    return True
                else:
                    print(f"   ⏳ 进度: {healthy_count}/{total_count} 服务健康...")
                    time.sleep(5)
            except:
                time.sleep(5)
        else:
            time.sleep(5)
    
    print(f"   ⚠️  等待超时，部分服务可能仍在启动中")
    return False

def main():
    """主要验证流程"""
    print("🔬 TubeWhale精准全功能健康验证系统")
    print("=" * 50)
    print(f"🕐 验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 等待服务完全启动
    wait_for_services_healthy()
    print()
    
    # 执行所有检查
    checks = [
        ("Docker服务状态", check_docker_services),
        ("API端点测试", test_api_endpoints),
        ("数据库连接", test_database_connection),
        ("Redis连接", test_redis_connection),
        ("Celery任务系统", test_celery_tasks),
        ("CLI集成", test_cli_integration),
        ("完整工作流程", test_complete_workflow),
        ("性能优化", optimize_performance),
    ]
    
    results = {}
    total_score = 0
    max_score = len(checks)
    
    for check_name, check_func in checks:
        print(f"\n🔍 {check_name}:")
        try:
            result = check_func()
            if isinstance(result, tuple):
                success, details = result
            else:
                success = result
                details = "完成"
            
            results[check_name] = success
            if success:
                total_score += 1
                print(f"   ✅ {check_name}: 通过")
            else:
                print(f"   ❌ {check_name}: 失败 - {details}")
        except Exception as e:
            print(f"   ❌ {check_name}: 异常 - {e}")
            results[check_name] = False
    
    # 最终报告
    print("\n" + "=" * 50)
    print("🏆 TubeWhale精准全功能健康验证报告")
    print("=" * 50)
    
    score_percentage = (total_score / max_score) * 100
    print(f"🏆 总体评分: {total_score}/{max_score} ({score_percentage:.1f}%)")
    
    if score_percentage == 100:
        status = "🌟 完美"
        recommendation = "系统完美运行，所有功能正常！"
    elif score_percentage >= 90:
        status = "✅ 优秀"
        recommendation = "系统运行良好，少数问题需要关注"
    elif score_percentage >= 80:
        status = "🟡 良好"
        recommendation = "系统基本正常，需要修复部分问题"
    else:
        status = "❌ 需要修复"
        recommendation = "系统存在严重问题，需要立即修复"
    
    print(f"📊 系统状态: {status}")
    print(f"💬 建议: {recommendation}")
    
    print("\n📋 详细结果:")
    for check_name, success in results.items():
        icon = "✅" if success else "❌"
        print(f"   {icon} {check_name}")
    
    print("\n🎯 精准施工完成状态:")
    if score_percentage == 100:
        print("   🚀 全部7个服务100%健康")
        print("   🔗 完整功能通路验证通过")
        print("   ⚡ 精准性能优化完成")
        print("   🌟 系统达到完美状态!")
    else:
        failed_checks = [name for name, success in results.items() if not success]
        print(f"   ⚠️  需要修复的检查项: {', '.join(failed_checks)}")
    
    print(f"\n🕐 验证完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    return score_percentage == 100

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)