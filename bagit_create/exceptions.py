class APIException(Exception):
    # This exception handles API errors (wrong API key or wrong url)
    pass


class GitlabException(Exception):
    # This exception handles internal gitlab errors when connection is successful
    pass


class RecidException(Exception):
    # This exception handles recid errors (incorrect recid or page not accessible)
    pass


class ServerException(Exception):
    # This exception handles server connection errors
    pass


class WrongInputException(Exception):
    # This exception handles wrong cli commands
    pass
