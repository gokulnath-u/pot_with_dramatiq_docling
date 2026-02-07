import logging
import sys

def setup_logging():
    if logging.getLogger().hasHandlers():
        return

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logging.getLogger("pypdf").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)