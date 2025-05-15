from shared_architecture.utils.logging_utils import log_info, log_warning, log_exception
import os
from pathlib import Path

def configure_logging(service_name: str = "microservice", log_level: str = "INFO"):
    """
    Configures logging for the microservice.

    Args:
        service_name: The name of the microservice (used in log messages).
        log_level: The desired logging level (e.g., "DEBUG", "INFO", "WARNING", "ERROR").
    """

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)  # Create the 'logs' directory if it doesn't exist
    log_file = log_dir / f"{service_name}.log"



    # Example of more advanced configuration using logging.config.dictConfig
    # if not os.path.exists("logging.conf"):
    #     print("logging.conf not found")
    # else:
    #     logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

    log_info(f"Logging configured for {service_name} at level {log_level}")


if __name__ == "__main__":
    configure_logging("test_service", "DEBUG")
    logging.debug("This is a debug message.")
    log_info("This is an info message.")
    log_warning("This is a warning message.")
    log_exception("This is an error message.")
    logging.critical("This is a critical message.")