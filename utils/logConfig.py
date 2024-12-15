import logging
import sys

def setupLogger():
    logger = logging.getLogger('DataProcessor')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    fileHandler = logging.FileHandler('data_processing.log')
    fileHandler.setFormatter(formatter)
    
    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setFormatter(formatter)
    
    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)
    
    return logger
