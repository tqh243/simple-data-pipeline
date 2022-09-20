class MongoETLInvalidMaxType(Exception):
    def __init__(self, max_type):
        exc_msg = f'max_type ({max_type}) not supported.'
        super().__init__(exc_msg)
