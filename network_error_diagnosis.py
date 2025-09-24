#!/usr/bin/env python3
"""
🔍 TubeWhale网络错误诊断工具
Network Error Diagnostic Tool - 精准定位网络问题
"""

import requests
import json
import subprocess
import time
from datetime import datetime

def test_json_endpoints():
    """测试可能出现JSON序列化错误的端点"""
    print("🔍 测试JSON端点...")
    
    test_urls = [
        # API端点
        "http://localhost:8001/api/v1/tubewhale/health/",
        "http://localhost:8001/api/v1/tubewhale/scenarios/",
        
        # 前端端点
        "http://localhost:80/api/v1/tubewhale/health/",
        "http://localhost:80/dashboard/",
        "http://localhost:80/dashboard/templates/wizard/?step=input&tool=video&template_id=general-analyst&analysis_type=comprehensive",
    ]
    
    results = {}
    
    for url in test_urls:
        print(f"   测试: {url}")
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                try:
                    # 尝试解析JSON
                    if 'json' in response.headers.get('content-type', ''):
                        json_data = response.json()
                        print(f"   ✅ {url}: JSON解析成功")
                        results[url] = {'status': 'ok', 'type': 'json'}
                    else:
                        print(f"   ✅ {url}: HTML响应正常")
                        results[url] = {'status': 'ok', 'type': 'html'}
                except json.JSONDecodeError as e:
                    print(f"   ❌ {url}: JSON解析失败 - {e}")
                    results[url] = {'status': 'json_error', 'error': str(e)}
            else:
                print(f"   ⚠️  {url}: HTTP {response.status_code}")
                results[url] = {'status': 'http_error', 'code': response.status_code}
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ {url}: 连接失败 - {e}")
            results[url] = {'status': 'connection_error', 'error': str(e)}
    
    return results

def test_problematic_request():
    """模拟可能导致问题的请求"""
    print("🎯 模拟问题请求...")
    
    # 基于日志中的错误请求
    problem_url = "http://localhost:80/dashboard/templates/wizard/?step=input&tool=video&template_id=academic-scholar&analysis_type=educational"
    
    try:
        print(f"   POST请求: {problem_url}")
        response = requests.post(problem_url, 
                               data={'test': 'data'}, 
                               headers={'Content-Type': 'application/x-www-form-urlencoded'},
                               timeout=10)
        
        print(f"   响应状态: {response.status_code}")
        print(f"   响应头: {dict(response.headers)}")
        
        if response.status_code == 500:
            print("   ❌ 发现500错误 - 这就是网络错误的原因!")
            return False
        else:
            print("   ✅ 请求正常")
            return True
            
    except Exception as e:
        print(f"   ❌ 请求异常: {e}")
        return False

def check_django_logs():
    """检查Django错误日志"""
    print("📋 检查Django错误日志...")
    
    try:
        result = subprocess.run([
            'docker', 'compose', '-f', 'docker-compose.full.yml', 
            'logs', '--tail=30', 'backend'
        ], capture_output=True, text=True, timeout=30)
        
        if "JSON serializable" in result.stdout:
            print("   ❌ 发现JSON序列化错误!")
            # 提取错误行
            lines = result.stdout.split('\n')
            for line in lines:
                if "JSON serializable" in line or "TypeError" in line:
                    print(f"   📝 错误: {line}")
            return False
        else:
            print("   ✅ 未发现JSON序列化错误")
            return True
            
    except Exception as e:
        print(f"   ❌ 检查日志失败: {e}")
        return False

def test_specific_api_calls():
    """测试特定的API调用"""
    print("🎯 测试特定API调用...")
    
    # 测试scenario API
    try:
        print("   测试场景API...")
        response = requests.get("http://localhost:8001/api/v1/tubewhale/scenarios/", 
                               headers={'Authorization': 'Bearer test'})
        
        if response.status_code == 401:
            print("   ⚠️  需要认证 - 这是正常的")
            return True
        elif response.status_code == 200:
            print("   ✅ 场景API正常")
            return True
        else:
            print(f"   ❌ 场景API异常: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ 场景API测试失败: {e}")
        return False

def run_comprehensive_diagnosis():
    """运行综合诊断"""
    print("🔬 TubeWhale网络错误综合诊断")
    print("=" * 50)
    print(f"🕐 诊断时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 运行所有测试
    tests = [
        ("JSON端点测试", test_json_endpoints),
        ("问题请求模拟", test_problematic_request),
        ("Django日志检查", check_django_logs),
        ("特定API测试", test_specific_api_calls),
    ]
    
    results = {}
    issues_found = []
    
    for test_name, test_func in tests:
        print(f"\n🔄 执行: {test_name}")
        try:
            result = test_func()
            results[test_name] = result
            if not result:
                issues_found.append(test_name)
        except Exception as e:
            print(f"   ❌ {test_name} 执行失败: {e}")
            results[test_name] = False
            issues_found.append(test_name)
    
    # 总结报告
    print("\n" + "=" * 50)
    print("🎯 网络错误诊断报告")
    print("=" * 50)
    
    if issues_found:
        print("❌ 发现以下问题:")
        for issue in issues_found:
            print(f"   • {issue}")
        
        print("\n💡 解决建议:")
        if "Django日志检查" in issues_found:
            print("   1. 存在JSON序列化错误，需要修复gettext lazy对象")
            print("   2. 重新构建后端容器应用修复")
        
        print("   3. 检查是否有未包装的翻译函数")
        print("   4. 确保所有_()调用都用str()包装")
        
    else:
        print("✅ 未发现网络错误 - 系统正常!")
    
    return len(issues_found) == 0

if __name__ == "__main__":
    success = run_comprehensive_diagnosis()
    exit(0 if success else 1)