#!/usr/bin/env python3
"""
V5 数据自动归档脚本

功能：
- 压缩并归档旧的reports/runs/目录
- 清理超过保留期限的数据
- 防止磁盘空间无限增长

保留策略：
- 最近30天：保持原样（快速访问）
- 30-90天：压缩为tar.gz（节省空间）
- 超过90天：删除（释放空间）
"""

import os
import re
import shutil
import tarfile
from pathlib import Path
from datetime import datetime, timedelta
import json

# 自动检测项目根目录
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent

REPORTS_DIR = PROJECT_ROOT / 'reports'
RUNS_DIR = REPORTS_DIR / 'runs'
ARCHIVE_DIR = REPORTS_DIR / 'archive'

# 保留策略（天）
KEEP_DAYS = 30      # 保持原样的天数
ARCHIVE_DAYS = 90   # 压缩保留的天数（总计）


class DataArchiver:
    """数据归档器"""
    
    def __init__(self):
        self.stats = {
            'archived': 0,
            'deleted': 0,
            'errors': 0,
            'space_saved_mb': 0
        }
    
    def log(self, msg):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] {msg}")
    
    def parse_run_date(self, run_name):
        """从 run 名解析时间 (格式: YYYYMMDD_HH，兼容 .tar/.tar.gz 后缀)。"""
        try:
            raw_name = str(run_name or '')
            if raw_name.endswith('.tar.gz'):
                raw_name = raw_name[:-7]
            elif raw_name.endswith('.tar'):
                raw_name = raw_name[:-4]

            match = re.search(r'(?<!\d)(\d{8}_\d{2})(?!\d)', raw_name)
            if match:
                return datetime.strptime(match.group(1), '%Y%m%d_%H')

            date_match = re.search(r'(?<!\d)(\d{8})(?!\d)', raw_name)
            if date_match:
                return datetime.strptime(date_match.group(1), '%Y%m%d')
        except:
            return None
        return None
    
    def get_dir_size(self, path):
        """获取目录大小（MB）"""
        total = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = Path(dirpath) / f
                if fp.exists():
                    total += fp.stat().st_size
        return total / (1024 * 1024)  # MB
    
    def archive_run(self, run_path):
        """归档单个run目录"""
        try:
            run_name = run_path.name
            archive_path = ARCHIVE_DIR / f"{run_name}.tar.gz"
            
            # 创建归档
            self.log(f"📦 归档: {run_name}")
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(run_path, arcname=run_name)
            
            # 验证归档完整性
            if tarfile.is_tarfile(archive_path):
                # 删除原目录
                shutil.rmtree(run_path)
                self.stats['archived'] += 1
                self.log(f"✅ 完成: {run_name}")
                return True
            else:
                self.log(f"❌ 归档验证失败: {run_name}")
                archive_path.unlink(missing_ok=True)
                self.stats['errors'] += 1
                return False
                
        except Exception as e:
            self.log(f"❌ 错误: {run_name} - {e}")
            self.stats['errors'] += 1
            return False
    
    def delete_old_archive(self, archive_path):
        """删除过期的归档文件"""
        try:
            size_mb = archive_path.stat().st_size / (1024 * 1024)
            archive_path.unlink()
            self.stats['deleted'] += 1
            self.stats['space_saved_mb'] += size_mb
            self.log(f"🗑️  删除过期归档: {archive_path.name} ({size_mb:.1f}MB)")
            return True
        except Exception as e:
            self.log(f"❌ 删除失败: {archive_path.name} - {e}")
            self.stats['errors'] += 1
            return False
    
    def run(self, dry_run=False):
        """执行归档流程"""
        self.log("=" * 60)
        self.log("🗄️  V5 数据归档开始")
        self.log("=" * 60)
        
        if dry_run:
            self.log("⚠️  干运行模式 - 不会实际删除或归档")
        
        # 确保归档目录存在
        ARCHIVE_DIR.mkdir(exist_ok=True)
        
        now = datetime.now()
        
        # 处理runs目录
        if RUNS_DIR.exists():
            run_dirs = [d for d in RUNS_DIR.iterdir() if d.is_dir()]
            
            for run_path in run_dirs:
                run_date = self.parse_run_date(run_path.name)
                if not run_date:
                    continue
                
                age = now - run_date
                
                if age <= timedelta(days=KEEP_DAYS):
                    # 保留期内，跳过
                    continue
                
                elif age <= timedelta(days=ARCHIVE_DAYS):
                    # 需要归档
                    size_mb = self.get_dir_size(run_path)
                    
                    if dry_run:
                        self.log(f"[干运行] 将归档: {run_path.name} ({size_mb:.1f}MB)")
                    else:
                        if self.archive_run(run_path):
                            self.stats['space_saved_mb'] += size_mb * 0.7  # 估算压缩后节省70%
                
                else:
                    # 超过归档期，直接删除
                    size_mb = self.get_dir_size(run_path)
                    
                    if dry_run:
                        self.log(f"[干运行] 将删除: {run_path.name} ({size_mb:.1f}MB)")
                    else:
                        shutil.rmtree(run_path)
                        self.stats['deleted'] += 1
                        self.stats['space_saved_mb'] += size_mb
                        self.log(f"🗑️  删除过期数据: {run_path.name} ({size_mb:.1f}MB)")
        
        # 处理已归档文件（超过总保留期）
        if ARCHIVE_DIR.exists():
            archive_files = [f for f in ARCHIVE_DIR.iterdir() if f.suffix == '.gz']
            
            for archive_path in archive_files:
                run_date = self.parse_run_date(archive_path.stem)
                if not run_date:
                    continue
                
                age = now - run_date
                
                if age > timedelta(days=ARCHIVE_DAYS):
                    if dry_run:
                        size_mb = archive_path.stat().st_size / (1024 * 1024)
                        self.log(f"[干运行] 将删除归档: {archive_path.name} ({size_mb:.1f}MB)")
                    else:
                        self.delete_old_archive(archive_path)
        
        # 输出统计
        self.log("")
        self.log("=" * 60)
        self.log("📊 归档统计")
        self.log("=" * 60)
        self.log(f"已归档: {self.stats['archived']} 个目录")
        self.log(f"已删除: {self.stats['deleted']} 个项目")
        self.log(f"空间节省: {self.stats['space_saved_mb']:.1f} MB")
        if self.stats['errors'] > 0:
            self.log(f"错误: {self.stats['errors']} 个")
        self.log("=" * 60)
        
        # 保存统计
        if not dry_run:
            stats_file = REPORTS_DIR / 'archive_stats.json'
            with open(stats_file, 'w') as f:
                json.dump({
                    'last_run': datetime.now().isoformat(),
                    'stats': self.stats
                }, f, indent=2)
        
        return self.stats


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 数据归档工具')
    parser.add_argument('--dry-run', action='store_true', help='干运行模式（不实际执行）')
    args = parser.parse_args()
    
    archiver = DataArchiver()
    archiver.run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
