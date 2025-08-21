class PlaywrightException(Exception):
    """Generic exception that all other TikTok errors are children of."""

    def __init__(self, raw_response, message, error_code=None):
        self.error_code = error_code
        self.raw_response = raw_response
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.error_code} -> {self.message}"


class CaptchaException(PlaywrightException):
    """Browser is showing captcha"""


class NotFoundException(PlaywrightException):
    """Browser indicated that this object does not exist."""


class EmptyResponseException(PlaywrightException):
    """Browser sent back an empty response."""


class SoundRemovedException(PlaywrightException):
    """This sound has no id from being removed by browser."""


class InvalidJSONException(PlaywrightException):
    """Browser returned invalid JSON."""


class InvalidResponseException(PlaywrightException):
    """The response from browser was invalid."""