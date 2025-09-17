"""
API App Permissions
REST API权限控制
"""

from rest_framework import permissions
from rest_framework.permissions import BasePermission
from django.utils import timezone
from apps.user_app.models import APIKey


class IsOwnerOrReadOnly(BasePermission):
    """
    只有对象的所有者才能编辑，其他人只能读取
    """
    
    def has_object_permission(self, request, view, obj):
        # 读取权限允许任何请求
        if request.method in permissions.SAFE_METHODS:
            return True
        
        # 写入权限只允许对象的所有者
        return obj.user == request.user


class IsOwner(BasePermission):
    """
    只有对象的所有者才能访问
    """
    
    def has_object_permission(self, request, view, obj):
        return obj.user == request.user


class APIKeyPermission(BasePermission):
    """
    API Key权限验证
    检查API Key是否有效且未超过使用限制
    """
    
    def has_permission(self, request, view):
        # 如果已经通过JWT认证，允许访问
        if request.user.is_authenticated and not hasattr(request, 'api_key'):
            return True
        
        # 检查API Key认证
        if hasattr(request, 'api_key'):
            api_key = request.api_key
            
            # 检查API Key是否有效
            if not api_key.is_valid():
                return False
            
            # 检查速率限制（简单的每日限制）
            today_usage = api_key.usage_logs.filter(
                timestamp__date=timezone.now().date()
            ).count()
            
            if today_usage >= api_key.rate_limit:
                return False
            
            return True
        
        return False


class VideoOwnerPermission(BasePermission):
    """
    视频所有者权限
    """
    
    def has_object_permission(self, request, view, obj):
        # 如果对象有user属性，检查是否为当前用户
        if hasattr(obj, 'user'):
            return obj.user == request.user
        
        # 如果对象有video属性，检查视频是否属于当前用户
        if hasattr(obj, 'video'):
            return obj.video.user == request.user
        
        return False


class AnalysisTaskPermission(BasePermission):
    """
    分析任务权限
    """
    
    def has_permission(self, request, view):
        return request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        # 检查任务是否属于当前用户
        return obj.user == request.user


class ReportPermission(BasePermission):
    """
    报告访问权限
    """
    
    def has_permission(self, request, view):
        return request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        # 检查报告是否属于当前用户
        return obj.user == request.user


class AdminOrReadOnly(BasePermission):
    """
    管理员可以编辑，其他用户只能读取
    """
    
    def has_permission(self, request, view):
        if request.method in permissions.SAFE_METHODS:
            return True
        return request.user.is_staff


class APIKeyOwnerPermission(BasePermission):
    """
    API Key所有者权限
    """
    
    def has_permission(self, request, view):
        return request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        return obj.user == request.user
