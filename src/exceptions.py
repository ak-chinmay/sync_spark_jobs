class SparkConnectionException(IOError):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = arg

