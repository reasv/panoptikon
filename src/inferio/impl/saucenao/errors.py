class SauceNaoApiError(Exception):
    pass


class UnknownApiError(SauceNaoApiError):
    pass


class UnknownServerError(UnknownApiError):
    pass


class UnknownClientError(UnknownApiError):
    pass


class BadKeyError(SauceNaoApiError):
    pass


class BadFileSizeError(SauceNaoApiError):
    pass


class LimitReachedError(SauceNaoApiError):
    pass


class ShortLimitReachedError(LimitReachedError):
    pass


class LongLimitReachedError(LimitReachedError):
    pass
