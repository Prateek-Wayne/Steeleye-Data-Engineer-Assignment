"""
@author: Prateek Verma

"""
import os
import logging

# Creating directory to store log
if not os.path.exists("logs"):
    os.mkdir("logs")

# Format for logging
formatter = "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(message)s"

# Setting up logger
logging.basicConfig(
    format=formatter,
    level=logging.INFO,
    filename=os.path.join(os.getcwd(), "logs/my_app.log"),
    filemode="a",
)

log = logging.getLogger(__name__)
