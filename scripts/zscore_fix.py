def robust_zscore_cross_section_fixed(values: Dict[str, float], winsorize_pct: float = 0.05) -> Dict[str, float]:
    """修复版稳健截面z-score：处理单币种情况"""
    if not values:
        return {}
    
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    
    # 1. 缩尾处理
    if winsorize_pct > 0 and len(xs) > 1:
        lower = np.percentile(xs, winsorize_pct * 100)
        upper = np.percentile(xs, (1 - winsorize_pct) * 100)
        xs = np.clip(xs, lower, upper)
    
    # 2. 处理单币种情况
    if len(xs) == 1:
        # 单币种时，返回标准化值（例如除以绝对值或固定值）
        # 方案A: 返回原值（不标准化）
        # 方案B: 返回符号值（-1, 0, 1）
        # 方案C: 返回缩放值
        return {keys[0]: float(np.sign(xs[0]) if abs(xs[0]) > 1e-12 else 0.0)}
    
    # 3. 处理所有值相同的情况
    if len(set(xs)) == 1:
        # 所有值相同，返回0
        return {k: 0.0 for k in keys}
    
    # 4. 使用median和MAD（Median Absolute Deviation）
    med = np.median(xs)
    mad = np.median(np.abs(xs - med))
    
    # 5. 标准化：MAD -> 标准差近似 (MAD * 1.4826 ≈ std for normal)
    if mad < 1e-12:
        return {k: 0.0 for k in keys}
    
    zs = (xs - med) / (mad * 1.4826)
    return {k: float(z) for k, z in zip(keys, zs)}


def standard_zscore_cross_section_fixed(values: Dict[str, float]) -> Dict[str, float]:
    """修复版标准z-score：处理单币种情况"""
    if not values:
        return {}
    
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    
    # 处理单币种情况
    if len(xs) == 1:
        return {keys[0]: float(np.sign(xs[0]) if abs(xs[0]) > 1e-12 else 0.0)}
    
    # 处理所有值相同的情况
    if len(set(xs)) == 1:
        return {k: 0.0 for k in keys}
    
    # 标准z-score计算
    mu = float(np.mean(xs))
    sd = float(np.std(xs))
    if sd < 1e-12:
        return {k: 0.0 for k in keys}
    
    zs = (xs - mu) / sd
    return {k: float(z) for k, z in zip(keys, zs)}
