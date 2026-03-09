#!/usr/bin/env python3
"""
V5 日志轮转工具

功能：
- 自动压缩和归档旧日志
- 限制日志文件大小
- 保留最近N天日志
"""

import gzip
import shutil
from pathlib import Path
from datetime import datetime, timedelta

LOGS_DIR = Path('/home/admin/clawd/v5-trading-bot/logs')
ARCHIVE_DIR = LOGS_DIR / 'archive'

# 保留策略
KEEP_DAYS = 7           # 保持原样的天数
ARCHIVE_DAYS = 30       # 压缩保留的天数
MAX_LOG_SIZE_MB = 100   # 单个日志文件最大大小


class LogRotator:
    """日志轮转器"""
    
    def __init__(self):
        self.stats = {'rotated': 0, 'compressed': 0, 'deleted': 0, 'space_saved_mb': 0}
    
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    
    def get_file_age_days(self, path):
        """获取文件年龄（天）"""
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        return (datetime.now() - mtime).days
    
    def rotate_large_logs(self):
        """轮转过大的日志文件"""
        for log_file in LOGS_DIR.glob('*.log'):
            size_mb = log_file.stat().st_size / (1024 * 1024)
            
            if size_mb > MAX_LOG_SIZE_MB:
                self.log(f"🔄 轮转大文件: {log_file.name} ({size_mb:.1f}MB)")
                
                # 重命名为带日期后缀
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                new_name = f"{log_file.stem}.{timestamp}.log"
                new_path = log_file.parent / new_name
                
                # 移动文件
                shutil.move(log_file, new_path)
                
                # 创建新的空日志文件
                log_file.touch()
                
                self.stats['rotated'] += 1
                self.stats['space_saved_mb'] += size_mb * 0.9  # 压缩后约节省90%
    
    def compress_old_logs(self):
        """压缩旧日志"""
        ARCHIVE_DIR.mkdir(exist_ok=True)
        
        for log_file in LOGS_DIR.glob('*.log'):
            # 跳过当前活跃的日志
            if '.' not in log_file.stem:
                continue
            
            age = self.get_file_age_days(log_file)
            
            if age > KEEP_DAYS and age <= ARCHIVE_DAYS:
                # 需要压缩
                archive_path = ARCHIVE_DIR / f"{log_file.name}.gz"
                
                self.log(f"📦 压缩: {log_file.name}")
                
                with open(log_file, 'rb') as f_in:
                    with gzip.open(archive_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # 删除原文件
                log_file.unlink()
                
                self.stats['compressed'] += 1
                original_size = archive_path.stat().st_size / 0.1  # 估算原大小
                self.stats['space_saved_mb'] += original_size * 0.9 / (1024*1024)
    
    def delete_very_old_logs(self):
        """删除过期日志"""
        # 删除过期的压缩日志
        for archive_file in ARCHIVE_DIR.glob('*.gz'):
            age = self.get_file_age_days(archive_file)
            
            if age > ARCHIVE_DAYS:
                size_mb = archive_file.stat().st_size / (1024 * 1024)
                self.log(f"🗑️  删除过期归档: {archive_file.name}")
                archive_file.unlink()
                self.stats['deleted'] += 1
                self.stats['space_saved_mb'] += size_mb
    
    def clean_application_logs(self):
        """清理应用特定的日志目录"""
        # 清理v5_runtime.log等
        for app_log in LOGS_DIR.glob('v5_*.log'):
            age = self.get_file_age_days(app_log)
            size_mb = app_log.stat().st_size / (1024 * 1024)
            
            if age > KEEP_DAYS:
                self.log(f"🗑️  清理应用日志: {app_log.name}")
                
                # 压缩后删除
                archive_path = ARCHIVE_DIR / f"{app_log.name}.{datetime.now().strftime('%Y%m%d')}.gz"
                with open(app_log, 'rb') as f_in:
                    with gzip.open(archive_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                app_log.unlink()
                self.stats['compressed'] += 1
                self.stats['space_saved_mb'] += size_mb * 0.9
    
    def run(self):
        """运行日志轮转"""
        self.log("=" * 60)
        self.log("📝 V5 日志轮转开始")
        self.log("=" * 60)
        
        if not LOGS_DIR.exists():
            self.log("❌ 日志目录不存在")
            return
        
        # 1. 轮转过大的日志
        self.rotate_large_logs()
        
        # 2. 压缩旧日志
        self.compress_old_logs()
        
        # 3. 删除过期日志
        self.delete_very_old_logs()
        
        # 4. 清理应用日志
        self.clean_application_logs()
        
        # 输出统计
        self.log("")
        self.log("=" * 60)
        self.log("📊 轮转统计")
        self.log("=" * 60)
        self.log(f"已轮转: {self.stats['rotated']} 个")
        self.log(f"已压缩: {self.stats['compressed']} 个")
        self.log(f"已删除: {self.stats['deleted']} 个")
        self.log(f"空间节省: {self.stats['space_saved_mb']:.1f} MB")
        self.log("=" * 60)


def main():
    rotator = LogRotator()
    rotator.run()


if __name__ == '__main__':
    main()
