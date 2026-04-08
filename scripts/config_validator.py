#!/usr/bin/env python3
"""
V5 配置验证工具

功能：
- 启动前检查配置是否正确
- 验证API密钥
- 检查必要文件和目录
- 验证定时任务配置
"""

import os
import sys
import yaml
from pathlib import Path
from datetime import datetime

CURRENT_PRODUCTION_TIMERS = (
    'v5-prod.user.timer',
    'v5-reconcile.timer',
    'v5-trade-monitor.timer',
)


def resolve_workspace() -> Path:
    raw = os.getenv('V5_WORKSPACE', '').strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path(__file__).resolve().parents[1]


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding='utf-8', errors='ignore').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


WORKSPACE = resolve_workspace()
CONFIG_DIR = WORKSPACE / 'configs'
REPORTS_DIR = WORKSPACE / 'reports'
DATA_DIR = WORKSPACE / 'data'


def _resolve_workspace_relative_path(raw_path, default: str) -> Path:
    value = str(raw_path or default).strip()
    path = Path(value)
    if not path.is_absolute():
        path = WORKSPACE / path
    return path.resolve()


def _derive_fill_store_path(order_store_path: Path) -> Path:
    if order_store_path.name == 'orders.sqlite':
        return order_store_path.with_name('fills.sqlite')
    if 'orders' in order_store_path.stem:
        return order_store_path.with_name(order_store_path.name.replace('orders', 'fills', 1))
    return order_store_path.with_name('fills.sqlite')


def _derive_position_store_path(order_store_path: Path) -> Path:
    if order_store_path.name == 'orders.sqlite':
        return order_store_path.with_name('positions.sqlite')
    if 'orders' in order_store_path.stem:
        return order_store_path.with_name(order_store_path.name.replace('orders', 'positions', 1))
    return order_store_path.with_name('positions.sqlite')


class ConfigValidator:
    """配置验证器"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.checks_passed = 0
        self.active_config = {}
    
    def log(self, msg, level='INFO'):
        prefix = {'INFO': 'ℹ️', 'PASS': '✅', 'WARN': '⚠️', 'FAIL': '❌'}
        print(f"{prefix.get(level, '•')} {msg}")
    
    def check_file_exists(self, path, required=True):
        """检查文件是否存在"""
        if path.exists():
            self.checks_passed += 1
            if required:
                self.log(f"文件存在: {path.name}", 'PASS')
            return True
        else:
            if required:
                self.errors.append(f"缺少必要文件: {path}")
                self.log(f"文件不存在: {path.name}", 'FAIL')
            else:
                self.warnings.append(f"缺少可选文件: {path}")
                self.log(f"文件不存在: {path.name}", 'WARN')
            return False
    
    def check_directory_structure(self):
        """检查目录结构"""
        self.log("\n📁 检查目录结构...")
        
        required_dirs = [
            WORKSPACE,
            CONFIG_DIR,
            REPORTS_DIR,
            DATA_DIR,
            WORKSPACE / 'src',
            WORKSPACE / 'scripts'
        ]
        
        for dir_path in required_dirs:
            if dir_path.exists():
                self.checks_passed += 1
                self.log(f"目录存在: {dir_path.name}", 'PASS')
            else:
                self.errors.append(f"缺少目录: {dir_path}")
                self.log(f"目录不存在: {dir_path.name}", 'FAIL')
    
    def check_yaml_config(self, config_name):
        """检查YAML配置文件"""
        self.log(f"\n📋 检查配置: {config_name}")
        
        config_path = CONFIG_DIR / config_name
        if not config_path.exists():
            self.errors.append(f"配置文件不存在: {config_path}")
            self.log(f"配置文件不存在", 'FAIL')
            return None
        
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            
            self.checks_passed += 1
            self.log(f"YAML格式正确", 'PASS')
            
            # 检查关键配置项
            required_keys = ['execution', 'account']
            for key in required_keys:
                if key in config:
                    self.checks_passed += 1
                    self.log(f"配置项存在: {key}", 'PASS')
                else:
                    self.warnings.append(f"配置项建议存在: {key}")
                    self.log(f"配置项缺失: {key}", 'WARN')
            
            self.active_config = config if isinstance(config, dict) else {}
            return config
            
        except yaml.YAMLError as e:
            self.errors.append(f"YAML解析错误: {e}")
            self.log(f"YAML格式错误: {e}", 'FAIL')
            return None
    
    def check_env_variables(self):
        """检查环境变量"""
        self.log("\n🔐 检查环境变量...")

        # 加载.env文件
        env_file = WORKSPACE / '.env'
        if env_file.exists():
            load_env_file(env_file)
            self.checks_passed += 1
            self.log("环境变量文件存在", 'PASS')
        else:
            self.warnings.append("缺少.env文件，将使用系统环境变量")
            self.log("环境变量文件不存在", 'WARN')

        # 检查必要的环境变量
        required_vars = ['EXCHANGE_API_KEY', 'EXCHANGE_API_SECRET', 'EXCHANGE_PASSPHRASE']
        for var in required_vars:
            value = os.getenv(var)
            if value:
                self.checks_passed += 1
                self.log(f"环境变量存在: {var}", 'PASS')
            else:
                self.errors.append(f"缺少环境变量: {var}")
                self.log(f"环境变量缺失: {var}", 'FAIL')
    
    def check_timers(self):
        """检查定时任务"""
        self.log("\n⏰ 检查定时任务...")
        
        import subprocess
        
        result = subprocess.run(
            ['systemctl', '--user', 'list-timers', '--all', '--no-pager'],
            capture_output=True, text=True
        )
        
        for timer in CURRENT_PRODUCTION_TIMERS:
            if timer in result.stdout:
                self.checks_passed += 1
                self.log(f"定时任务存在: {timer}", 'PASS')
            else:
                self.warnings.append(f"定时任务可能未启用: {timer}")
                self.log(f"定时任务未找到: {timer}", 'WARN')
    
    def check_disk_space(self):
        """检查磁盘空间"""
        self.log("\n💾 检查磁盘空间...")
        
        import shutil
        
        total, used, free = shutil.disk_usage(WORKSPACE)
        free_gb = free / (1024**3)
        used_percent = used / total * 100
        
        if free_gb < 1:
            self.errors.append(f"磁盘空间不足: 仅剩 {free_gb:.1f}GB")
            self.log(f"磁盘空间严重不足: {free_gb:.1f}GB 可用", 'FAIL')
        elif used_percent > 90:
            self.warnings.append(f"磁盘使用率过高: {used_percent:.1f}%")
            self.log(f"磁盘空间紧张: {free_gb:.1f}GB 可用 ({used_percent:.1f}% 已用)", 'WARN')
        else:
            self.checks_passed += 1
            self.log(f"磁盘空间充足: {free_gb:.1f}GB 可用", 'PASS')
    
    def check_database(self):
        """检查数据库"""
        self.log("\n🗄️  检查数据库...")
        
        import sqlite3
        
        execution_cfg = self.active_config.get('execution', {}) if isinstance(self.active_config, dict) else {}
        orders_db = _resolve_workspace_relative_path(
            execution_cfg.get('order_store_path') if isinstance(execution_cfg, dict) else None,
            'reports/orders.sqlite',
        )
        db_files = [
            orders_db,
            _derive_position_store_path(orders_db),
            _derive_fill_store_path(orders_db),
        ]
        
        for db_path in db_files:
            if db_path.exists():
                try:
                    conn = sqlite3.connect(str(db_path))
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
                    conn.close()
                    
                    size_mb = db_path.stat().st_size / (1024 * 1024)
                    self.checks_passed += 1
                    self.log(f"数据库正常: {db_path.name} ({size_mb:.1f}MB)", 'PASS')
                except Exception as e:
                    self.errors.append(f"数据库错误 {db_path.name}: {e}")
                    self.log(f"数据库错误: {db_path.name}", 'FAIL')
            else:
                self.warnings.append(f"数据库不存在（将自动创建）: {db_path.name}")
                self.log(f"数据库不存在: {db_path.name}", 'WARN')
    
    def run_all_checks(self, config_name='live_prod.yaml'):
        """运行所有检查"""
        print("=" * 60)
        print("🔍 V5 配置验证")
        print("=" * 60)
        print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"工作目录: {WORKSPACE}")
        print(f"配置文件: {config_name}")
        print()
        
        # 执行检查
        self.check_directory_structure()
        self.check_yaml_config(config_name)
        self.check_env_variables()
        self.check_timers()
        self.check_disk_space()
        self.check_database()
        
        # 输出结果
        print("\n" + "=" * 60)
        print("📊 验证结果")
        print("=" * 60)
        print(f"通过: {self.checks_passed} 项")
        print(f"警告: {len(self.warnings)} 项")
        print(f"错误: {len(self.errors)} 项")
        
        if self.warnings:
            print("\n⚠️  警告详情:")
            for w in self.warnings:
                print(f"  - {w}")
        
        if self.errors:
            print("\n❌ 错误详情:")
            for e in self.errors:
                print(f"  - {e}")
            print("\n❌ 验证失败，请修复错误后再启动")
            return False
        else:
            print("\n✅ 验证通过！可以启动系统")
            return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 配置验证工具')
    parser.add_argument('--config', default='live_prod.yaml', help='配置文件名')
    args = parser.parse_args()
    
    validator = ConfigValidator()
    success = validator.run_all_checks(args.config)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
