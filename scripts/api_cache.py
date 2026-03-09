#!/usr/bin/env python3
"""
V5 Web面板缓存中间件

功能：
- 为API响应添加内存缓存
- 减少重复数据库查询
- 提高响应速度
"""

import json
import time
from functools import wraps
from datetime import datetime

class APICache:
    """API响应缓存"""
    
    def __init__(self, default_ttl=30):
        self.cache = {}
        self.default_ttl = default_ttl  # 默认缓存30秒
    
    def get(self, key):
        """获取缓存"""
        if key in self.cache:
            data, expiry = self.cache[key]
            if time.time() < expiry:
                return data
            else:
                del self.cache[key]
        return None
    
    def set(self, key, data, ttl=None):
        """设置缓存"""
        ttl = ttl or self.default_ttl
        expiry = time.time() + ttl
        self.cache[key] = (data, expiry)
    
    def clear(self, pattern=None):
        """清除缓存"""
        if pattern:
            keys_to_delete = [k for k in self.cache if pattern in k]
            for k in keys_to_delete:
                del self.cache[k]
        else:
            self.cache.clear()
    
    def stats(self):
        """返回缓存统计"""
        now = time.time()
        valid = sum(1 for _, expiry in self.cache.values() if expiry > now)
        expired = len(self.cache) - valid
        return {'total': len(self.cache), 'valid': valid, 'expired': expired}


# 全局缓存实例
_cache = APICache()


def cached_api(ttl=30, key_fn=None):
    """
    API缓存装饰器
    
    用法：
        @cached_api(ttl=60)
        def api_account():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存key
            if key_fn:
                cache_key = key_fn(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # 尝试从缓存获取
            cached = _cache.get(cache_key)
            if cached is not None:
                return cached
            
            # 执行原函数
            result = func(*args, **kwargs)
            
            # 缓存结果
            _cache.set(cache_key, result, ttl)
            
            return result
        return wrapper
    return decorator


def clear_cache_pattern(pattern):
    """清除匹配的缓存"""
    _cache.clear(pattern)


def get_cache_stats():
    """获取缓存统计"""
    return _cache.stats()


# 在web_dashboard.py中使用：
# from api_cache import cached_api, clear_cache_pattern
# 
# @cached_api(ttl=60)  # 缓存60秒
# def api_account():
#     ...
# 
# @cached_api(ttl=30)  # 缓存30秒
# def api_positions():
#     ...
