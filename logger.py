from loguru import logger

logger.add(f'./{__file__}.log', level="INFO", rotation="5 MB")
