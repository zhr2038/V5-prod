#!/usr/bin/env python3
"""
V5 自动备份工具

功能：
- 备份关键数据文件
- 支持本地和远程备份
- 保留策略管理
"""

import shutil
import tarfile
from pathlib import Path
from datetime import datetime, timedelta

WORKSPACE = Path('/home/admin/clawd/v5-trading-bot')
BACKUP_DIR = WORKSPACE / 'backups'

# 备份配置
BACKUP_ITEMS = [
    'reports/orders.sqlite',
    'reports/positions.sqlite',
    'reports/fills.sqlite',
    'reports/bills.sqlite',
    'configs/',
    'memory/',
    'MEMORY.md',
    'SOUL.md',
    'IDENTITY.md',
    'USER.md'
]

KEEP_BACKUPS = 7  # 保留最近7个备份


class BackupManager:
    """备份管理器"""
    
    def __init__(self):
        self.stats = {'backed_up': 0, 'errors': 0, 'size_mb': 0}
    
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    
    def create_backup(self, name=None):
        """创建备份"""
        BACKUP_DIR.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = name or f"v5_backup_{timestamp}"
        backup_path = BACKUP_DIR / f"{backup_name}.tar.gz"
        
        self.log("=" * 60)
        self.log(f"🗄️  创建备份: {backup_name}")
        self.log("=" * 60)
        
        with tarfile.open(backup_path, 'w:gz') as tar:
            for item in BACKUP_ITEMS:
                src_path = WORKSPACE / item
                if src_path.exists():
                    try:
                        if src_path.is_dir():
                            tar.add(src_path, arcname=item)
                            self.log(f"📁 备份目录: {item}")
                        else:
                            tar.add(src_path, arcname=item)
                            self.log(f"📄 备份文件: {item}")
                        self.stats['backed_up'] += 1
                    except Exception as e:
                        self.log(f"❌ 备份失败 {item}: {e}")
                        self.stats['errors'] += 1
                else:
                    self.log(f"⚠️  跳过不存在: {item}")
        
        # 计算备份大小
        size_mb = backup_path.stat().st_size / (1024 * 1024)
        self.stats['size_mb'] = size_mb
        
        self.log(f"✅ 备份完成: {backup_path}")
        self.log(f"📦 大小: {size_mb:.1f} MB")
        
        return backup_path
    
    def cleanup_old_backups(self):
        """清理旧备份"""
        if not BACKUP_DIR.exists():
            return
        
        backups = sorted(BACKUP_DIR.glob('*.tar.gz'), key=lambda x: x.stat().st_mtime, reverse=True)
        
        if len(backups) > KEEP_BACKUPS:
            to_delete = backups[KEEP_BACKUPS:]
            self.log(f"\n🗑️  清理 {len(to_delete)} 个旧备份...")
            for backup in to_delete:
                try:
                    backup.unlink()
                    self.log(f"  删除: {backup.name}")
                except Exception as e:
                    self.log(f"  删除失败: {backup.name} - {e}")
    
    def list_backups(self):
        """列出所有备份"""
        if not BACKUP_DIR.exists():
            print("没有备份")
            return
        
        backups = sorted(BACKUP_DIR.glob('*.tar.gz'), key=lambda x: x.stat().st_mtime, reverse=True)
        
        print("\n📋 备份列表:")
        print("-" * 60)
        for i, backup in enumerate(backups, 1):
            size_mb = backup.stat().st_size / (1024 * 1024)
            mtime = datetime.fromtimestamp(backup.stat().st_mtime)
            print(f"{i}. {backup.name}")
            print(f"   大小: {size_mb:.1f} MB  时间: {mtime.strftime('%Y-%m-%d %H:%M')}")
        print("-" * 60)
    
    def restore_backup(self, backup_name):
        """恢复备份"""
        backup_path = BACKUP_DIR / backup_name
        if not backup_path.exists():
            self.log(f"❌ 备份不存在: {backup_name}")
            return False
        
        self.log(f"🔄 恢复备份: {backup_name}")
        
        # 创建恢复目录
        restore_dir = WORKSPACE / f'restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        restore_dir.mkdir(exist_ok=True)
        
        with tarfile.open(backup_path, 'r:gz') as tar:
            tar.extractall(path=restore_dir)
        
        self.log(f"✅ 备份已解压到: {restore_dir}")
        self.log("⚠️  请手动检查并移动到正确位置")
        
        return True
    
    def run(self):
        """运行备份流程"""
        self.log("🚀 开始备份流程")
        
        # 1. 创建备份
        backup_path = self.create_backup()
        
        # 2. 清理旧备份
        self.cleanup_old_backups()
        
        # 3. 输出统计
        self.log("\n" + "=" * 60)
        self.log("📊 备份统计")
        self.log("=" * 60)
        self.log(f"已备份: {self.stats['backed_up']} 项")
        self.log(f"错误: {self.stats['errors']} 项")
        self.log(f"备份大小: {self.stats['size_mb']:.1f} MB")
        self.log("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 自动备份工具')
    parser.add_argument('action', choices=['backup', 'list', 'restore'], default='backup', nargs='?')
    parser.add_argument('--name', help='备份名称')
    parser.add_argument('--restore-file', help='要恢复的备份文件名')
    args = parser.parse_args()
    
    manager = BackupManager()
    
    if args.action == 'backup':
        manager.run()
    elif args.action == 'list':
        manager.list_backups()
    elif args.action == 'restore':
        if args.restore_file:
            manager.restore_backup(args.restore_file)
        else:
            print("❌ 请指定 --restore-file 参数")


if __name__ == '__main__':
    main()
