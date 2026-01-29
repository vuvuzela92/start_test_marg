import logging, os
# from .env_loader import *
from dotenv import load_dotenv

load_dotenv()


LOGS_PATH = os.getenv("LOGS_PATH")

# def setup_logger(filename="app.log"):
#     path = os.getenv("LOGS_PATH", "./logs")
#     os.makedirs(path, exist_ok=True)
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         handlers=[
#             logging.FileHandler(f"{path}/{filename}", encoding="utf-8"),
#             logging.StreamHandler()
#         ]
#     )
#     return logging.getLogger(filename)

def setup_logger(filename="app.log"):
    path = os.getenv("LOGS_PATH", "./logs")
    os.makedirs(path, exist_ok=True)

    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        fh = logging.FileHandler(f"{path}/{filename}", encoding="utf-8")
        sh = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(fmt)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger
