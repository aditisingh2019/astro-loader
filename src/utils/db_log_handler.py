import logging

class DatabaseLogHandler(logging.Handler):

    def __init__(self, engine):
        super().__init__()
        self.engine = engine

    def emit(self, record):
        pass
        #with self.engine.connection() as connection:
            
        
