import sys
import os
import threading
import time
import random
import signal

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Database")
