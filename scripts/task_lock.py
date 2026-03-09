#!/usr/bin/env python3
"""
V5 定时任务互斥锁工具

功能：
- 防止定时任务重叠执行
- 支持超时自动释放
- 支持强制解锁
"""

import os
import time
import fcntl
from pathlib import Path
from datetime import datetime

LOCK_DIR = Path('/tmp/v5_locks')


class TaskLock:
    """任务锁"""
    
    def __init__(self, task_name, timeout=300):
        """
        task_name: 任务名称（如 'v5-live-20u'）
        timeout: 锁超时时间（秒），默认5分钟
        """
        self.task_name = task_name
        self.timeout = timeout
        self.lock_file = LOCK_DIR / f"{task_name}.lock"
        self.fd = None
    
    def acquire(self):
        """获取锁"""
        LOCK_DIR.mkdir(exist_ok=True)
        
        # 检查是否有超时的旧锁
        if self.lock_file.exists():
            try:
                mtime = self.lock_file.stat().st_mtime
                age = time.time() - mtime
                if age > self.timeout:
                    # 锁已超时，强制删除
                    self.lock_file.unlink()
                    print(f"⚠️  发现超时锁（{age:.0f}秒），已强制释放")
            except:
                pass
        
        try:
            self.fd = open(self.lock_file, 'w')
            fcntl.flock(self.fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # 写入锁信息
            self.fd.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
            self.fd.flush()
            
            return True
        except (IOError, OSError):
            if self.fd:
                self.fd.close()
                self.fd = None
            return False
    
    def release(self):
        """释放锁"""
        if self.fd:
            try:
                fcntl.flock(self.fd.fileno(), fcntl.LOCK_UN)
                self.fd.close()
            except:
                pass
            finally:
                self.fd = None
        
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
        except:
            pass
    
    def __enter__(self):
        if not self.acquire():
            raise RuntimeError(f"任务 {self.task_name} 正在运行中，跳过本次执行")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


def force_unlock(task_name):
    """强制解锁"""
    lock_file = LOCK_DIR / f"{task_name}.lock"
    try:
        if lock_file.exists():
            lock_file.unlink()
            print(f"✅ 已强制解锁: {task_name}")
            return True
        else:
            print(f"ℹ️  没有锁文件: {task_name}")
            return False
    except Exception as e:
        print(f"❌ 解锁失败: {e}")
        return False


def list_active_locks():
    """列出所有活跃锁"""
    if not LOCK_DIR.exists():
        return []
    
    locks = []
    for lock_file in LOCK_DIR.glob('*.lock'):
        try:
            with open(lock_file) as f:
                pid = f.readline().strip()
                timestamp = f.readline().strip()
            
            age = time.time() - lock_file.stat().st_mtime
            locks.append({
                'task': lock_file.stem,
                'pid': pid,
                'since': timestamp,
                'age_seconds': int(age)
            })
        except:
            pass
    
    return locks


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 任务锁管理')
    parser.add_argument('action', choices=['list', 'unlock'], help='操作')
    parser.add_argument('--task', help='任务名称（unlock时需要）')
    args = parser.parse_args()
    
    if args.action == 'list':
        locks = list_active_locks()
        if locks:
            print("🔒 活跃任务锁:")
            for lock in locks:
                print(f"  {lock['task']}: PID={lock['pid']}, 已运行{lock['age_seconds']}秒")
        else:
            print("ℹ️  没有活跃的任务锁")
    
    elif args.action == 'unlock':
        if args.task:
            force_unlock(args.task)
        else:
            print("❌ 请指定 --task 参数")


if __name__ == '__main__':
    main()
