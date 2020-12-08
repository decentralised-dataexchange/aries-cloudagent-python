from marshmallow.validate import OneOf, Range, Regexp

class IGrantIOAPIKeyJWS(Regexp):
    """Validate iGrant.io API key JWS"""

    EXAMPLE = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyaWQiOiI1ZjUyNGZhYmM2NzAwMTAwMDEwMGY5ZTUiLCJleHAiOjE2MzAzMzQ1Nzl9.mE0WH81Y40xImEcEVwhHa5KA8uaxPF4SwrZPKW-SiYc"
    PATTERN = rf"^[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*$"

    def __init__(self):
        """Initializer."""

        super().__init__(
            IGrantIOAPIKeyJWS.PATTERN,
            error="Value {input} is not iGrant.io API key JWS",
        )