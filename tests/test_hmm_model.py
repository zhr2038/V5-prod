import numpy as np

from src.regime.hmm_model import SimpleGaussianHMM


def test_predict_proba_uses_smoothed_posterior():
    model = SimpleGaussianHMM(n_components=2)
    model.startprob_ = np.array([0.5, 0.5])
    model.transmat_ = np.array([[0.98, 0.02], [0.10, 0.90]])
    model.means_ = np.array([[0.0], [3.0]])
    model.covs_ = np.array([[[0.25]], [[0.25]]])
    model.n_features = 1

    X = np.array([[0.0], [0.0], [3.0]])

    alpha, scale = model._forward(X)
    beta = model._backward(X, scale)
    expected = alpha * beta
    expected /= expected.sum(axis=1, keepdims=True)

    probs = model.predict_proba(X)
    forward_only = alpha / alpha.sum(axis=1, keepdims=True)

    assert np.allclose(probs, expected)
    assert not np.allclose(probs[-2], forward_only[-2])
