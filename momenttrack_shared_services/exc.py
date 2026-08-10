class DatabaseConnectionError(Exception):
    pass


class DuplicateMadeTransactionError(Exception):
    """docstring for DuplicateMadeTransactionError"""
    def __init__(self, message):
        super(DuplicateMadeTransactionError, self).__init__(message)
        self.message = message

    def __reduce__(self):
        return (DuplicateMadeTransactionError, (self.message))
