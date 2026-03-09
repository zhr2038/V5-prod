#!/usr/bin/env python3
"""HMM market regime detector built on BTC close-price features."""

import csv
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from src.regime.hmm_model import SimpleGaussianHMM


class HMMRegimeDetector:
    """Detect market regimes with a Gaussian HMM."""

    def __init__(self, n_components: int = 3, model_path: Path = None):
        self.n_components = n_components
        self.model = SimpleGaussianHMM(n_components=n_components)
        self.model_path = model_path or (PROJECT_ROOT / 'models' / 'hmm_regime.pkl')
        self.info_path = self.model_path.parent / 'hmm_regime_info.json'
        self.state_names = self._load_state_labels()

    @staticmethod
    def build_features_from_closes(closes: Sequence[float], min_periods: int = 14) -> np.ndarray:
        """Build the live and training features from the same close-price series."""
        values = [float(v) for v in closes if v is not None]
        features = []
        for i in range(len(values)):
            if i < min_periods:
                continue

            prev_close = values[i - 1]
            close_now = values[i]
            ret_1h = (close_now - prev_close) / prev_close if prev_close > 0 else 0.0

            anchor_idx = max(0, i - 6)
            anchor_close = values[anchor_idx]
            ret_6h = (close_now - anchor_close) / anchor_close if anchor_close > 0 else 0.0

            window = np.asarray(values[max(0, i - min_periods): i + 1], dtype=float)
            vol = float(np.std(np.diff(window) / window[:-1])) if len(window) > 1 else 0.0

            gains = [
                values[j] - values[j - 1]
                for j in range(max(1, i - min_periods), i + 1)
                if values[j] > values[j - 1]
            ]
            losses = [
                values[j - 1] - values[j]
                for j in range(max(1, i - min_periods), i + 1)
                if values[j] < values[j - 1]
            ]
            avg_gain = float(np.mean(gains)) if gains else 0.0
            avg_loss = float(np.mean(losses)) if losses else 0.001
            rsi = 100 - (100 / (1 + avg_gain / avg_loss))

            features.append([ret_1h, ret_6h, vol, rsi])

        return np.asarray(features, dtype=float)

    def _load_state_labels(self) -> dict:
        try:
            if self.info_path.exists():
                info = json.loads(self.info_path.read_text(encoding='utf-8'))
                labels = info.get('state_labels', {})
                parsed = {int(k): v for k, v in labels.items()}
                if len(set(parsed.values())) == min(len(parsed), 3):
                    return parsed
        except Exception as exc:
            print(f'[HMM] load state labels failed: {exc}')
        return {0: 'TrendingUp', 1: 'Sideways', 2: 'TrendingDown'}

    def _load_training_data_from_cache(
        self,
        cache_dir: Path,
        symbol: str,
        lookback_days: int,
    ) -> Optional[np.ndarray]:
        symbol_key = symbol.replace('/', '_').replace('-', '_').upper()
        files = sorted(cache_dir.glob(f'{symbol_key}_1H_*.csv'))
        if not files:
            print(f'[HMM] missing price cache for {symbol_key} in {cache_dir}')
            return None

        rows = {}
        for file in files:
            try:
                with file.open('r', encoding='utf-8', newline='') as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        ts = row.get('timestamp')
                        close = row.get('close')
                        if not ts or close in (None, ''):
                            continue
                        try:
                            rows[datetime.fromisoformat(ts)] = float(close)
                        except Exception:
                            continue
            except Exception as exc:
                print(f'[HMM] load price cache failed: {file} {exc}')

        if not rows:
            return None

        ordered = sorted(rows.items(), key=lambda item: item[0])
        cutoff = ordered[-1][0] - timedelta(days=lookback_days)
        closes = [close for ts, close in ordered if ts >= cutoff]
        if len(closes) < 50:
            print(f'[HMM] price cache insufficient: {len(closes)} closes')
            return None
        return self.build_features_from_closes(closes)

    def _load_training_data_from_alpha_db(
        self,
        db_path: Path,
        symbol: str,
        lookback_days: int,
    ) -> Optional[np.ndarray]:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        start_ts = int((datetime.now() - timedelta(days=lookback_days)).timestamp())
        cursor.execute(
            '''
            SELECT ts, f1_mom_5d, f2_mom_20d, f3_vol_adj_ret_20d
            FROM alpha_snapshots
            WHERE symbol = ? AND ts > ?
            ORDER BY ts
            ''',
            (symbol, start_ts),
        )
        rows = cursor.fetchall()
        conn.close()

        if len(rows) < 50:
            print(f'[HMM] alpha snapshot data insufficient: {len(rows)}')
            return None

        features = []
        for i, row in enumerate(rows):
            if i < 14:
                continue
            f1_mom = row[1] or 0
            f2_mom = row[2] or 0
            vol_adj_ret = row[3] or 0

            window = rows[max(0, i - 14): i + 1]
            moms = [r[1] for r in window if r[1] is not None]
            gains = [m for m in moms if m > 0]
            losses = [abs(m) for m in moms if m < 0]
            avg_gain = float(np.mean(gains)) if gains else 0.0
            avg_loss = float(np.mean(losses)) if losses else 0.001
            rsi = 100 - (100 / (1 + avg_gain / avg_loss))
            features.append([
                float(f1_mom) * 0.01,
                float(f2_mom) * 0.01,
                abs(float(vol_adj_ret)) * 0.01,
                rsi,
            ])

        return np.asarray(features, dtype=float)

    def _infer_state_labels(self) -> dict:
        if self.model.means_ is None:
            return dict(self.state_names)

        trend_scores = {idx: float(mean[0] + mean[1]) for idx, mean in enumerate(self.model.means_)}
        ordered = sorted(trend_scores, key=trend_scores.get)
        labels = {idx: 'Sideways' for idx in trend_scores}
        if ordered:
            labels[ordered[0]] = 'TrendingDown'
            labels[ordered[-1]] = 'TrendingUp'
        for idx in ordered[1:-1]:
            labels[idx] = 'Sideways'
        return labels

    def _write_model_info(self, n_samples: int) -> None:
        if self.model.means_ is None:
            return
        labels = self._infer_state_labels()
        payload = {
            'trained_at': datetime.now().isoformat(),
            'model_class': type(self.model).__name__,
            'model_payload_type': 'dict',
            'n_components': int(self.n_components),
            'n_samples': int(n_samples),
            'n_features': int(self.model.n_features or 0),
            'converged': bool(self.model.converged),
            'state_labels': {str(k): v for k, v in labels.items()},
            'state_means': {
                f'State_{idx}': [float(v) for v in self.model.means_[idx]]
                for idx in range(len(self.model.means_))
            },
            'transition_matrix': self.model.transmat_.tolist(),
        }
        self.info_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    def load_training_data(
        self,
        db_path: Path = None,
        symbol: str = 'BTC/USDT',
        lookback_days: int = 120,
    ) -> Optional[np.ndarray]:
        source_path = Path(db_path) if db_path else (PROJECT_ROOT / 'data' / 'cache')
        if source_path.is_dir():
            features = self._load_training_data_from_cache(source_path, symbol, lookback_days)
            if features is not None and len(features) > 0:
                return features

        if source_path.suffix.lower() == '.csv':
            features = self._load_training_data_from_cache(source_path.parent, symbol, lookback_days)
            if features is not None and len(features) > 0:
                return features

        legacy_db = source_path if source_path.suffix.lower() == '.db' else (PROJECT_ROOT / 'reports' / 'alpha_history.db')
        if legacy_db.exists():
            return self._load_training_data_from_alpha_db(legacy_db, symbol, lookback_days)
        return None

    def train(self, X: np.ndarray = None):
        if X is None:
            X = self.load_training_data()

        if X is None or len(X) < 100:
            print('[HMM] training data insufficient')
            return False

        print(f'[HMM] start training, samples={len(X)}, features={X.shape[1]}')
        self.model.fit(X)
        self.state_names = self._infer_state_labels()

        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save(self.model_path)
        self._write_model_info(len(X))

        print(f'[HMM] training done, converged={self.model.converged}')
        print(f'[HMM] transition matrix:\n{self.model.transmat_}')
        for i in range(self.n_components):
            print(f'[HMM] state_{i} ({self.state_names.get(i, "Unknown")}): {self.model.means_[i]}')
        return True

    def predict(self, features: np.ndarray) -> dict:
        if self.model.means_ is None:
            if self.model_path.exists():
                self.model.load(self.model_path)
            else:
                return {'state': 'Unknown', 'state_id': -1, 'probability': 0.0, 'probs': [0.33, 0.33, 0.34]}

        probs = self.model.predict_proba(features)
        current_probs = probs[-1]
        states = self.model.predict(features)
        current_state = int(states[-1])

        labels = self._infer_state_labels()
        state_characteristics = []
        true_state_probs = {'TrendingUp': 0.0, 'TrendingDown': 0.0, 'Sideways': 0.0}
        for i in range(self.n_components):
            mean = self.model.means_[i]
            true_state = labels.get(i, 'Sideways')
            state_characteristics.append({
                'id': i,
                'true_state': true_state,
                'mom_5d': float(mean[0]),
                'mom_20d': float(mean[1]),
                'volatility': float(mean[2]),
                'trend_score': float(mean[0] + mean[1]),
            })
            true_state_probs[true_state] += float(current_probs[i])

        semantic_state = max(true_state_probs, key=true_state_probs.get)
        return {
            'state': semantic_state,
            'state_id': current_state,
            'probability': float(true_state_probs[semantic_state]),
            'latent_probability': float(current_probs[current_state]),
            'probs': current_probs.tolist(),
            'all_states': {k: float(v) for k, v in true_state_probs.items()},
            'state_details': state_characteristics,
        }

    def detect_regime(self, features_list: list) -> dict:
        features_arr = np.asarray(features_list, dtype=float)
        result = self.predict(features_arr)
        result['timestamp'] = datetime.now().isoformat()
        result['features'] = {
            'mom_5d': float(features_arr[-1][0]),
            'mom_20d': float(features_arr[-1][1]),
            'volatility': float(features_arr[-1][2]),
            'rsi': float(features_arr[-1][3]),
        }
        return result


if __name__ == '__main__':
    detector = HMMRegimeDetector()
    X = detector.load_training_data()
    if X is not None:
        detector.train(X)
