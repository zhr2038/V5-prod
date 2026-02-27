#!/usr/bin/env python3
"""
批量添加文档字符串脚本
自动为缺少文档字符串的函数和类添加基本文档
"""

import ast
import re
from pathlib import Path

def add_docstrings_to_file(filepath: Path):
    """为文件添加文档字符串"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.split('\n')
    
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return False
    
    # 收集需要添加文档字符串的位置
    to_add = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            if node.name == '__init__':
                continue
            if node.name.startswith('_'):
                continue
            if ast.get_docstring(node):
                continue
            
            # 确定插入位置（冒号后的下一行）
            insert_line = node.lineno
            # 查找def/class行的缩进
            def_line = lines[insert_line - 1]
            base_indent = len(def_line) - len(def_line.lstrip())
            docstring_indent = ' ' * (base_indent + 4)
            
            # 生成简单的文档字符串
            if isinstance(node, ast.ClassDef):
                docstring = f'\"\"\"{node.name}类\"\"\"'
            else:
                # 函数 - 生成基于函数名的简单描述
                func_name = node.name
                # 转换下划线为空格，首字母大写
                desc = func_name.replace('_', ' ').capitalize()
                docstring = f'\"\"\"{desc}\"\"\"'
            
            to_add.append((insert_line, docstring_indent + docstring))
    
    if not to_add:
        return False
    
    # 从后往前插入，避免行号变化
    for line_no, text in sorted(to_add, reverse=True):
        lines.insert(line_no, text)
    
    # 写回文件
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    return True

# 修复所有文件
src_dir = Path('/home/admin/clawd/v5-trading-bot/src')
fixed = 0
for py_file in src_dir.rglob('*.py'):
    if '.venv' in str(py_file):
        continue
    if add_docstrings_to_file(py_file):
        fixed += 1
        print(f'Fixed: {py_file}')

print(f'\nTotal files fixed: {fixed}')
