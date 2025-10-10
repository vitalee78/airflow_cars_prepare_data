import logging
import re
from datetime import datetime
from random import uniform
from time import sleep
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.cars.lots.loader import LotsLoader

logger = logging.getLogger(__name__)

class ParserAuctions:
    pass