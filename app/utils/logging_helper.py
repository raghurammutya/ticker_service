import logging
import logging.config
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

    # Basic configuration (can be extended)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),  # Default to INFO
        format=f"%(asctime)s - %(levelname)s - {service_name} - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),  # Output to console
            logging.FileHandler(log_file),  # Output to file
        ],
    )

    # Example of more advanced configuration using logging.config.dictConfig
    # if not os.path.exists("logging.conf"):
    #     print("logging.conf not found")
    # else:
    #     logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

    logging.info(f"Logging configured for {service_name} at level {log_level}")


if __name__ == "__main__":
    configure_logging("test_service", "DEBUG")
    logging.debug("This is a debug message.")
    logging.info("This is an info message.")
    logging.warning("This is a warning message.")
    logging.error("This is an error message.")
    logging.critical("This is a critical message.")