#!/usr/bin/env python3
"""
TubeWhale网络问题诊断和修复脚本
Network Issue Diagnosis and Fix Script
"""

import subprocess
import requests
import json
import time
import sys
from typing import Dict, List, Any

class NetworkDiagnostic:
    """网络诊断和修复工具"""
    
    def __init__(self):
        self.results = {}
        self.issues_found = []
        self.fixes_applied = []
        
    def check_docker_services(self) -> Dict[str, Any]:
        """检查Docker服务状态"""
        print("🐳 检查Docker服务状态...")
        
        try:
            result = subprocess.run(
                ["docker", "compose", "-f", "docker-compose.full.yml", "ps"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                # 分析服务状态
                output = result.stdout
                running_services = output.count("Up")
                healthy_services = output.count("(healthy)")
                unhealthy_services = output.count("(unhealthy)")
                starting_services = output.count("(health: starting)")
                
                status = {
                    'total_services': running_services,
                    'healthy': healthy_services,
                    'unhealthy': unhealthy_services,
                    'starting': starting_services,
                    'raw_output': output
                }
                
                if unhealthy_services > 0:
                    self.issues_found.append(f"发现 {unhealthy_services} 个不健康的服务")
                
                print(f"   📊 运行中: {running_services}, 健康: {healthy_services}, 启动中: {starting_services}")
                
                return status
            else:
                self.issues_found.append("无法获取Docker服务状态")
                return {'error': result.stderr}
                
        except Exception as e:
            self.issues_found.append(f"Docker检查失败: {str(e)}")
            return {'error': str(e)}
    
    def test_api_endpoints(self) -> Dict[str, Dict]:
        """测试API端点"""
        print("\n📡 测试API端点...")
        
        endpoints = {
            'backend': 'http://localhost:8001/api/v1/tubewhale/health/',
            'frontend': 'http://localhost/api/v1/tubewhale/health/',
            'websocket': 'http://localhost:8004/'
        }
        
        results = {}
        for name, url in endpoints.items():
            try:
                start_time = time.time()
                response = requests.get(url, timeout=5)
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    print(f"   ✅ {name}: 正常 ({response_time:.3f}s)")
                    results[name] = {
                        'status': 'healthy',
                        'response_time': response_time,
                        'status_code': response.status_code
                    }
                else:
                    print(f"   ⚠️  {name}: 状态码 {response.status_code}")
                    results[name] = {
                        'status': 'warning',
                        'status_code': response.status_code,
                        'response_time': response_time
                    }
                    self.issues_found.append(f"{name} 返回状态码 {response.status_code}")
                    
            except requests.exceptions.ConnectionError:
                print(f"   ❌ {name}: 连接被拒绝")
                results[name] = {'status': 'connection_refused'}
                self.issues_found.append(f"{name} 连接被拒绝")
            except requests.exceptions.Timeout:
                print(f"   ⏰ {name}: 连接超时")
                results[name] = {'status': 'timeout'}
                self.issues_found.append(f"{name} 连接超时")
            except Exception as e:
                print(f"   ❌ {name}: {str(e)}")
                results[name] = {'status': 'error', 'error': str(e)}
                self.issues_found.append(f"{name} 错误: {str(e)}")
        
        return results
    
    def test_cli_integration(self) -> Dict[str, Any]:
        """测试CLI集成"""
        print("\n🖥️  测试CLI集成...")
        
        try:
            # 测试CLI基础功能
            result = subprocess.run(
                ["python", "cli.py", "--help"],
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode == 0 and "analyze" in result.stdout:
                print("   ✅ CLI基础功能正常")
                
                # 测试模板集成
                template_result = subprocess.run([
                    "python", "cli.py", "analyze", "dQw4w9WgXcQ", 
                    "--template-name", "Business Strategy Analyst",
                    "--template-id", "business_analyst",
                    "--job-id", "diagnostic_test"
                ], capture_output=True, text=True, timeout=30)
                
                if template_result.returncode == 0:
                    print("   ✅ CLI模板集成正常")
                    return {
                        'basic_cli': True,
                        'template_integration': True,
                        'output': template_result.stdout
                    }
                else:
                    print("   ⚠️  CLI模板集成需要关注")
                    self.issues_found.append("CLI模板集成异常")
                    return {
                        'basic_cli': True,
                        'template_integration': False,
                        'error': template_result.stderr
                    }
            else:
                print("   ❌ CLI基础功能异常")
                self.issues_found.append("CLI基础功能异常")
                return {
                    'basic_cli': False,
                    'template_integration': False,
                    'error': result.stderr
                }
                
        except Exception as e:
            print(f"   ❌ CLI测试失败: {str(e)}")
            self.issues_found.append(f"CLI测试失败: {str(e)}")
            return {'error': str(e)}
    
    def check_container_logs(self):
        """检查容器日志中的错误"""
        print("\n📋 检查容器日志...")
        
        containers = [
            "tubewhale_backend_full",
            "tubewhale_nginx_full", 
            "tubewhale_websocket_full"
        ]
        
        for container in containers:
            try:
                result = subprocess.run(
                    ["docker", "logs", container, "--tail", "10"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0:
                    logs = result.stdout + result.stderr
                    
                    # 检查常见错误模式
                    error_patterns = [
                        "ERROR", "CRITICAL", "TypeError", "ConnectionError", 
                        "JSON serializable", "500 Internal Server Error"
                    ]
                    
                    for pattern in error_patterns:
                        if pattern in logs:
                            print(f"   ⚠️  {container}: 发现 {pattern}")
                            self.issues_found.append(f"{container} 日志中发现 {pattern}")
                else:
                    print(f"   ❌ 无法获取 {container} 日志")
                    
            except Exception as e:
                print(f"   ❌ 检查 {container} 日志失败: {str(e)}")
    
    def apply_fixes(self):
        """应用修复"""
        print("\n🔧 应用修复...")
        
        if not self.issues_found:
            print("   ℹ️  未发现需要修复的问题")
            return
        
        # 修复JSON序列化问题
        if any("JSON serializable" in issue or "TypeError" in issue for issue in self.issues_found):
            print("   🔄 修复JSON序列化问题...")
            try:
                result = subprocess.run(
                    ["python", "fix_json_serialization.py"],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    print("   ✅ JSON序列化问题已修复")
                    self.fixes_applied.append("JSON序列化修复")
                    
                    # 重启后端容器
                    print("   🔄 重启后端容器...")
                    subprocess.run(["docker", "restart", "tubewhale_backend_full"], timeout=30)
                    time.sleep(5)
                    self.fixes_applied.append("后端容器重启")
                else:
                    print("   ❌ JSON序列化修复失败")
            except Exception as e:
                print(f"   ❌ 修复失败: {str(e)}")
        
        # 检查服务健康状态并重启不健康的服务
        unhealthy_services = [issue for issue in self.issues_found if "不健康" in issue]
        if unhealthy_services:
            print("   🔄 重启不健康的服务...")
            try:
                subprocess.run([
                    "docker", "compose", "-f", "docker-compose.full.yml", "restart"
                ], timeout=60)
                self.fixes_applied.append("重启Docker服务")
                time.sleep(10)
            except Exception as e:
                print(f"   ❌ 重启服务失败: {str(e)}")
    
    def generate_report(self):
        """生成诊断报告"""
        print("\n" + "="*60)
        print("🎯 TubeWhale网络问题诊断报告")
        print("="*60)
        
        print(f"\n🔍 发现的问题 ({len(self.issues_found)}):")
        if self.issues_found:
            for i, issue in enumerate(self.issues_found, 1):
                print(f"   {i}. {issue}")
        else:
            print("   ✅ 未发现问题")
        
        print(f"\n🔧 应用的修复 ({len(self.fixes_applied)}):")
        if self.fixes_applied:
            for i, fix in enumerate(self.fixes_applied, 1):
                print(f"   {i}. {fix}")
        else:
            print("   ℹ️  无需修复")
        
        # 最终状态检查
        print(f"\n📊 最终状态检查:")
        final_api_check = self.test_api_endpoints()
        
        healthy_endpoints = sum(1 for status in final_api_check.values() 
                              if status.get('status') == 'healthy')
        total_endpoints = len(final_api_check)
        
        print(f"\n🏆 系统健康度: {healthy_endpoints}/{total_endpoints} 端点正常")
        
        if healthy_endpoints == total_endpoints:
            print("🌟 状态: 完美 - 所有问题已解决!")
            return True
        elif healthy_endpoints >= total_endpoints * 0.8:
            print("⚡ 状态: 良好 - 主要功能正常")
            return True
        else:
            print("⚠️  状态: 需要关注 - 仍有问题待解决")
            return False
    
    def run_full_diagnostic(self) -> bool:
        """运行完整诊断"""
        print("🐋 TubeWhale网络问题诊断启动")
        print("解决Network error和CLI功能配合问题")
        print("-" * 50)
        
        # 执行所有检查
        self.check_docker_services()
        self.test_api_endpoints()
        self.test_cli_integration()
        self.check_container_logs()
        
        # 应用修复
        self.apply_fixes()
        
        # 生成报告
        return self.generate_report()

def main():
    """主函数"""
    diagnostic = NetworkDiagnostic()
    success = diagnostic.run_full_diagnostic()
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)