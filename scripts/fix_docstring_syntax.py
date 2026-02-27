#!/usr/bin/env python3
"""
修复auto-docstring脚本造成的语法错误
将方法签名中的docstring移动到方法体内
"""

import re
from pathlib import Path

def fix_file(filepath: Path):
    """修复单个文件中的docstring位置错误"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 匹配模式: def method(
    #             """docstring"""
    #             self,
    # 替换为: def method(
    #             self,
    #         ):
    #             """docstring"""
    
    # 使用正则表达式查找并修复
    pattern = r'(def\s+\w+\()\s*("""[^"]*""")\s*\n(\s+)(self,)'
    
    def replace_match(m):
        prefix = m.group(1)
        docstring = m.group(2)
        indent = m.group(3)
        self_param = m.group(4)
        return f'{prefix}\n{indent}{self_param}'
    
    new_content = re.sub(pattern, replace_match, content)
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f'Fixed: {filepath}')
        return True
    return False

# 修复所有文件
files_to_fix = [
    'src/utils/auto_blacklist.py',
    'src/execution/reconcile_engine.py',
    'src/execution/bootstrap_patch.py',
    'src/execution/okx_private_client.py',
    'src/reporting/reporting.py',
    'src/reporting/budget_state.py',
    'src/reporting/summary_writer.py',
]

root = Path('/home/admin/clawd/v5-trading-bot')
fixed_count = 0

for f in files_to_fix:
    filepath = root / f
    if filepath.exists():
        if fix_file(filepath):
            fixed_count += 1
    else:
        print(f'Not found: {f}')

print(f'\nTotal files fixed: {fixed_count}')
