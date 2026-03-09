import csv
from datetime import datetime, timedelta

import numpy as np

from src.regime.hmm_regime_detector import HMMRegimeDetector


class DummyModel:
    def __init__(self):
        self.means_ = np.array([
            [-0.03, -0.02, 0.01, 40.0],
            [0.04, 0.05, 0.01, 65.0],
            [0.001, -0.001, 0.05, 50.0],
        ])
        self.transmat_ = np.eye(3)
        self.n_features = 4
        self.converged = True

    def predict(self, X):
        return np.array([0] * len(X))

    def predict_proba(self, X):
        probs = np.zeros((len(X), 3))
        probs[-1] = [0.2, 0.7, 0.1]
        return probs


def test_predict_prefers_semantic_state_probability():
    detector = HMMRegimeDetector()
    detector.model = DummyModel()

    result = detector.predict(np.ones((20, 4), dtype=float))

    assert result['state'] == 'TrendingUp'
    assert result['probability'] == 0.7
    assert result['latent_probability'] == 0.2


def test_load_training_data_uses_price_cache_features(tmp_path):
    cache_file = tmp_path / 'BTC_USDT_1H_2026-01-01_2026-01-03.csv'
    start = datetime(2026, 1, 1, 0, 0, 0)
    closes = [100.0 + i for i in range(80)]
    with cache_file.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vol_ccy'])
        for i, close in enumerate(closes):
            ts = start + timedelta(hours=i)
            writer.writerow([ts.isoformat(sep=' '), close, close, close, close, 1.0, 1.0])

    detector = HMMRegimeDetector()
    features = detector.load_training_data(tmp_path, symbol='BTC/USDT', lookback_days=120)
    expected = HMMRegimeDetector.build_features_from_closes(closes)

    assert features is not None
    assert np.allclose(features, expected)
