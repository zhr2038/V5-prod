#!/usr/bin/env python3
"""
V5 HMM (隐马尔可夫模型) 市场状态检测器

简化版实现，基于NumPy，无需hmmlearn

参考: 使用高斯HMM识别隐藏的 market regime
"""

import hashlib
import json
import os
import pickle
from pathlib import Path
from typing import Tuple
from dataclasses import dataclass

import numpy as np


LEGACY_HMM_PICKLE_ENV = "V5_ALLOW_LEGACY_HMM_PICKLE_LOAD"


def sha256_file(path: Path) -> str:
    """Return the sha256 digest for a local model artifact."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hmm_model_info_path(model_path: Path) -> Path:
    return model_path.with_name(f"{model_path.stem}_info.json")


def _legacy_hmm_pickle_allowed() -> bool:
    return str(os.getenv(LEGACY_HMM_PICKLE_ENV, "")).strip().upper() in {"1", "TRUE", "YES", "Y"}


def _expected_sha256_from_info(info_path: Path) -> str | None:
    if not info_path.is_file():
        return None
    try:
        info = json.loads(info_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"HMM model info is unreadable: {info_path}") from exc
    value = info.get("model_sha256") or info.get("sha256") or info.get("artifact_sha256")
    return str(value).strip() if value else None


def verified_hmm_pickle_artifact_path(
    path: Path,
    *,
    expected_sha256: str | None = None,
    require_hash: bool = False,
) -> Path:
    raw_path = Path(path)
    resolved = raw_path.expanduser().resolve(strict=True)
    if resolved.suffix.lower() != ".pkl":
        raise RuntimeError(f"Refusing to load HMM pickle with unexpected suffix: {resolved}")
    if raw_path.is_symlink() or resolved.is_symlink():
        raise RuntimeError(f"Refusing to load symlinked HMM pickle: {resolved}")

    info_path = hmm_model_info_path(resolved)
    expected = expected_sha256 or _expected_sha256_from_info(info_path)
    info_requires_hash = info_path.is_file() or require_hash
    if expected:
        actual = sha256_file(resolved)
        if actual.lower() != expected.lower():
            raise RuntimeError(f"HMM pickle sha256 mismatch: {resolved}")
    elif info_requires_hash and not _legacy_hmm_pickle_allowed():
        raise RuntimeError(
            f"HMM pickle sha256 missing for {resolved}; "
            f"set {LEGACY_HMM_PICKLE_ENV}=YES only for trusted legacy production artifacts"
        )
    return resolved


@dataclass
class HMMState:
    """HMM状态定义"""
    name: str
    mean: np.ndarray
    cov: np.ndarray
    
    
class SimpleGaussianHMM:
    """
    简化版高斯HMM实现
    
    特性:
    - 支持多特征输入
    - EM算法训练
    - 维特比解码找最优状态序列
    """
    
    def __init__(self, n_components: int = 3, n_iter: int = 100, tol: float = 1e-2):
        self.n_components = n_components
        self.n_iter = n_iter
        self.tol = tol
        
        # 模型参数
        self.startprob_ = None  # 初始状态概率
        self.transmat_ = None   # 状态转移矩阵
        self.means_ = None      # 各状态均值
        self.covs_ = None       # 各状态协方差
        
        self.n_features = None
        self.converged = False
        
    def _gaussian_pdf(self, x: np.ndarray, mean: np.ndarray, cov: np.ndarray) -> float:
        """计算多元高斯概率密度"""
        d = x - mean
        det = np.linalg.det(cov)
        if det == 0:
            det = 1e-10
        inv = np.linalg.inv(cov + np.eye(cov.shape[0]) * 1e-6)
        exp_arg = -0.5 * d.T @ inv @ d
        return np.exp(exp_arg) / np.sqrt((2 * np.pi) ** len(x) * det)
    
    def _forward(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """前向算法"""
        n_samples = len(X)
        alpha = np.zeros((n_samples, self.n_components))
        scale = np.zeros(n_samples)
        
        # 初始化
        for j in range(self.n_components):
            alpha[0, j] = self.startprob_[j] * self._gaussian_pdf(X[0], self.means_[j], self.covs_[j])
        scale[0] = alpha[0].sum()
        if scale[0] > 0:
            alpha[0] /= scale[0]
        
        # 递推
        for t in range(1, n_samples):
            for j in range(self.n_components):
                alpha[t, j] = sum(alpha[t-1, i] * self.transmat_[i, j] 
                                  for i in range(self.n_components))
                alpha[t, j] *= self._gaussian_pdf(X[t], self.means_[j], self.covs_[j])
            scale[t] = alpha[t].sum()
            if scale[t] > 0:
                alpha[t] /= scale[t]
        
        return alpha, scale
    
    def _backward(self, X: np.ndarray, scale: np.ndarray) -> np.ndarray:
        """后向算法"""
        n_samples = len(X)
        beta = np.zeros((n_samples, self.n_components))
        beta[-1] = 1.0
        
        for t in range(n_samples - 2, -1, -1):
            for i in range(self.n_components):
                beta[t, i] = sum(
                    self.transmat_[i, j] * self._gaussian_pdf(X[t+1], self.means_[j], self.covs_[j]) * beta[t+1, j]
                    for j in range(self.n_components)
                )
            if scale[t+1] > 0:
                beta[t] /= scale[t+1]
        
        return beta
    
    def fit(self, X: np.ndarray):
        """训练HMM (EM算法)"""
        n_samples, self.n_features = X.shape
        
        # 初始化参数
        kmeans_idx = np.linspace(0, n_samples, self.n_components + 1, dtype=int)
        self.means_ = np.array([X[kmeans_idx[i]:kmeans_idx[i+1]].mean(axis=0) 
                                for i in range(self.n_components)])
        self.covs_ = np.array([np.cov(X.T) + np.eye(self.n_features) * 0.1 
                               for _ in range(self.n_components)])
        self.startprob_ = np.ones(self.n_components) / self.n_components
        self.transmat_ = np.ones((self.n_components, self.n_components)) / self.n_components
        
        log_likelihood_old = -np.inf
        
        for _iteration in range(self.n_iter):
            # E-step
            alpha, scale = self._forward(X)
            beta = self._backward(X, scale)
            
            # 计算gamma (状态概率)
            gamma = alpha * beta
            gamma_sum = gamma.sum(axis=1, keepdims=True)
            gamma_sum[gamma_sum == 0] = 1
            gamma /= gamma_sum
            
            # 计算xi (转移概率)
            xi = np.zeros((n_samples - 1, self.n_components, self.n_components))
            for t in range(n_samples - 1):
                for i in range(self.n_components):
                    for j in range(self.n_components):
                        xi[t, i, j] = (alpha[t, i] * self.transmat_[i, j] * 
                                       self._gaussian_pdf(X[t+1], self.means_[j], self.covs_[j]) * 
                                       beta[t+1, j])
                xi[t] /= xi[t].sum() if xi[t].sum() > 0 else 1
            
            # M-step: 更新参数
            # 更新转移矩阵
            self.transmat_ = xi.sum(axis=0) / xi.sum(axis=0).sum(axis=1, keepdims=True)
            self.transmat_ = np.maximum(self.transmat_, 1e-10)
            self.transmat_ /= self.transmat_.sum(axis=1, keepdims=True)
            
            # 更新均值和协方差
            for j in range(self.n_components):
                gamma_j = gamma[:, j]
                self.means_[j] = (X * gamma_j[:, np.newaxis]).sum(axis=0) / gamma_j.sum()
                diff = X - self.means_[j]
                self.covs_[j] = (diff.T @ (diff * gamma_j[:, np.newaxis])) / gamma_j.sum()
                self.covs_[j] += np.eye(self.n_features) * 1e-6  # 正则化
            
            # 检查收敛
            log_likelihood = np.log(scale[scale > 0]).sum()
            if abs(log_likelihood - log_likelihood_old) < self.tol:
                self.converged = True
                break
            log_likelihood_old = log_likelihood
        
        return self
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """维特比解码，返回最可能的状态序列"""
        n_samples = len(X)
        delta = np.zeros((n_samples, self.n_components))
        psi = np.zeros((n_samples, self.n_components), dtype=int)
        
        # 初始化
        for j in range(self.n_components):
            delta[0, j] = np.log(self.startprob_[j] + 1e-10) + \
                          np.log(self._gaussian_pdf(X[0], self.means_[j], self.covs_[j]) + 1e-10)
        
        # 递推
        for t in range(1, n_samples):
            for j in range(self.n_components):
                trans_probs = delta[t-1] + np.log(self.transmat_[:, j] + 1e-10)
                psi[t, j] = np.argmax(trans_probs)
                delta[t, j] = trans_probs[psi[t, j]] + \
                              np.log(self._gaussian_pdf(X[t], self.means_[j], self.covs_[j]) + 1e-10)
        
        # 回溯
        states = np.zeros(n_samples, dtype=int)
        states[-1] = np.argmax(delta[-1])
        for t in range(n_samples - 2, -1, -1):
            states[t] = psi[t+1, states[t+1]]
        
        return states
    
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return the smoothed posterior state probabilities."""
        alpha, scale = self._forward(X)
        beta = self._backward(X, scale)
        gamma = alpha * beta
        gamma_sum = gamma.sum(axis=1, keepdims=True)
        gamma_sum[gamma_sum == 0] = 1
        return gamma / gamma_sum
    
    def score(self, X: np.ndarray) -> float:
        """计算对数似然"""
        _, scale = self._forward(X)
        return np.log(scale[scale > 0]).sum()
    
    def save(self, path: Path) -> str:
        """保存模型"""
        with Path(path).open('wb') as f:
            pickle.dump({
                'n_components': self.n_components,
                'startprob_': self.startprob_,
                'transmat_': self.transmat_,
                'means_': self.means_,
                'covs_': self.covs_,
                'n_features': self.n_features,
                'converged': self.converged
            }, f)
        return sha256_file(Path(path))

    def load(self, path: Path, *, expected_sha256: str | None = None, require_hash: bool = False):
        """加载模型"""
        artifact_path = verified_hmm_pickle_artifact_path(
            Path(path),
            expected_sha256=expected_sha256,
            require_hash=require_hash,
        )
        with artifact_path.open('rb') as f:
            data = pickle.load(f)  # noqa: S301 - verified local HMM artifact
            self.n_components = data['n_components']
            self.startprob_ = data['startprob_']
            self.transmat_ = data['transmat_']
            self.means_ = data['means_']
            self.covs_ = data['covs_']
            self.n_features = data['n_features']
            self.converged = data['converged']
        return self
