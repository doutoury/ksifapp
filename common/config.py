from dataclasses import dataclass
from os import path, environ

base_dir = base_dir = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))


@dataclass
class Config: 
    """
    기본 Configuration 
    """
    BASE_DIR = base_dir

    DB_POOL_RECYCLE: int = 900
    DB_ECHO: bool = True

@dataclass
class LocalConfig(Config): 
    PROJ_RELOAD: bool = True

@dataclass
class ProdConfig(Config): 
    PROJ_RELOAD: bool = False

def conf(): 
    """
    환경 불러오기
    :return:
    """
    config = dict(prod=ProdConfig(), local=LocalConfig())   # 파이썬에서의 SwitchCase 사용법
    return config.get(environ.get("API_ENV", "local"))      # 파이썬에서의 SwitchCase 사용법