import os
from functools import lru_cache
from dotenv import load_dotenv, find_dotenv



@lru_cache
def setup_env() -> None:
    """
    Reads current active environment,
    then loads corresponding environment variables.
    `@lru_cache` annotation makes sure `.env` files are only loaded once.
    Subsequent calls will return the same value as its first call.
    """
    try:
        env_file = find_dotenv(
            filename=".env",
            raise_error_if_not_found=False,
            usecwd=False
        )
        if env_file:
            load_dotenv(env_file, verbose=True)

        active_env = str(os.environ["ENVIRONMENT"])

        if active_env == 'DEVELOPMENT':
            load_dotenv(find_dotenv('.env.dev'))
        elif active_env == 'TESTING':
            load_dotenv(find_dotenv('.env.test'))
    except:
        error_msg = 'No .env files were found.'
        raise Exception(error_msg)

        
def get_env_value(env_variable: str) -> str | int | bool | None:
    """
    Gets environment variables depending on active environment.
    """
    try:
        value = parse_env_value(os.environ[env_variable])
        return value
    except KeyError:
        error_msg = f'{env_variable} environment variable not set.'
        raise Exception(error_msg)


def parse_env_value(value: str) -> str | bool | int | None:
    """
    Parses environment variable into either bool, strings, ints, or None type.
    """ 
    if value == "none": return None             
    if value in ["0", "false"]: return False   
    if value in ["1", "true"]: return True
    if value.isnumeric(): return int(value)   
    return value