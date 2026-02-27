#!/usr/bin/env python3
"""
代码风格检查工具

检查内容：
- 行长度超过100字符
- 函数长度超过50行
- 缺少文档字符串

用法：
    python scripts/code_style_check.py
"""

import ast
import sys
from pathlib import Path
from typing import List, Tuple


def check_line_length(filepath: Path, max_length: int = 100) -> List[Tuple[int, int, str]]:
    """检查行长度"""
    issues = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            # 忽略注释和字符串
            stripped = line.split('#')[0].strip()
            if len(line.rstrip()) > max_length and not stripped.startswith(('"""', "'''")):
                issues.append((i, len(line.rstrip()), line.strip()[:50]))
    return issues


def check_function_length(filepath: Path, max_lines: int = 50) -> List[Tuple[str, int, int]]:
    """检查函数长度"""
    issues = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # 计算函数行数
                start_line = node.lineno
                end_line = node.end_lineno
                length = end_line - start_line + 1
                if length > max_lines:
                    issues.append((node.name, length, start_line))
    except SyntaxError:
        pass  # 忽略语法错误文件
    return issues


def check_docstrings(filepath: Path) -> List[Tuple[str, int, str]]:
    """检查文档字符串"""
    issues = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                # 检查是否有文档字符串
                docstring = ast.get_docstring(node)
                if not docstring:
                    issues.append((node.name, node.lineno, type(node).__name__))
    except SyntaxError:
        pass
    return issues


def main():
    project_root = Path(__file__).parent.parent
    src_dir = project_root / 'src'
    scripts_dir = project_root / 'scripts'
    
    all_issues = {
        'line_length': [],
        'function_length': [],
        'missing_docstring': []
    }
    
    # 检查所有Python文件
    for py_file in list(src_dir.rglob('*.py')) + list(scripts_dir.rglob('*.py')):
        if '.venv' in str(py_file):
            continue
        
        # 行长度检查
        line_issues = check_line_length(py_file)
        for line, length, content in line_issues:
            all_issues['line_length'].append((py_file, line, length, content))
        
        # 函数长度检查
        func_issues = check_function_length(py_file)
        for func, length, line in func_issues:
            all_issues['function_length'].append((py_file, func, length, line))
        
        # 文档字符串检查
        doc_issues = check_docstrings(py_file)
        for name, line, node_type in doc_issues:
            all_issues['missing_docstring'].append((py_file, name, line, node_type))
    
    # 输出报告
    print("="*60)
    print("代码风格检查报告")
    print("="*60)
    
    print(f"\n📏 行长度问题 ({len(all_issues['line_length'])}):")
    if all_issues['line_length']:
        for filepath, line, length, content in all_issues['line_length'][:10]:
            print(f"  {filepath}:{line} ({length} chars): {content}...")
        if len(all_issues['line_length']) > 10:
            print(f"  ... and {len(all_issues['line_length']) - 10} more")
    else:
        print("  ✅ 无问题")
    
    print(f"\n📄 函数长度问题 ({len(all_issues['function_length'])}):")
    if all_issues['function_length']:
        for filepath, func, length, line in sorted(
            all_issues['function_length'], 
            key=lambda x: x[2], 
            reverse=True
        )[:10]:
            print(f"  {filepath}:{line} {func}() ({length} lines)")
    else:
        print("  ✅ 无问题")
    
    print(f"\n📝 缺少文档字符串 ({len(all_issues['missing_docstring'])}):")
    if all_issues['missing_docstring']:
        for filepath, name, line, node_type in all_issues['missing_docstring'][:10]:
            print(f"  {filepath}:{line} {node_type} {name}")
        if len(all_issues['missing_docstring']) > 10:
            print(f"  ... and {len(all_issues['missing_docstring']) - 10} more")
    else:
        print("  ✅ 无问题")
    
    print("\n" + "="*60)
    
    # 返回非零退出码如果有问题
    total_issues = sum(len(v) for v in all_issues.values())
    if total_issues > 0:
        print(f"发现 {total_issues} 个问题")
        return 1
    else:
        print("✅ 所有检查通过！")
        return 0


if __name__ == '__main__':
    sys.exit(main())
