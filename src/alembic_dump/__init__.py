import logging

# Get the root logger
logger = logging.getLogger()

# Set the logging level
logger.setLevel(logging.INFO)

# Create a stream handler for console output
handler = logging.StreamHandler()

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set the formatter on the handler
handler.setFormatter(formatter)

# Add the handler to the root logger
logger.addHandler(handler)
