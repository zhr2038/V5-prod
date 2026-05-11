from __future__ import annotations


class QuantLabError(Exception):
    """Base quant-lab integration error."""


class QuantLabUnavailable(QuantLabError):
    """quant-lab API could not be reached or returned an invalid response."""


class QuantLabTimeout(QuantLabUnavailable):
    """quant-lab API request timed out."""


class QuantLabHTTPError(QuantLabUnavailable):
    """quant-lab API returned a non-2xx response."""


class QuantLabValidationError(QuantLabError):
    """quant-lab response or local quant-lab config failed validation."""


class QuantLabPermissionError(QuantLabError):
    """quant-lab permission value is not supported by V5."""
