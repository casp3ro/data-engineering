class DomainException(Exception):
    pass


class InvalidPriceError(DomainException):
    pass


class InvalidMileageError(DomainException):
    pass


class InvalidListingError(DomainException):
    pass
