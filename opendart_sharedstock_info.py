# -*- coding: utf-8 -*-

import io, requests, os
import json
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta

try:
    from pandas import json_normalize
except ImportError:
    from pandas.io.json import json_normalize

# 대량보유 상황보고
def get_majorstock_json(api_key, corp_code):
    url = 'https://opendart.fss.or.kr/api/majorstock.json'
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
    }

    res = requests.get(url, params=params)
    list_data = json.loads(res.content)
    return list_data

# 임원ㆍ주요주주 소유보고
def get_elestock_json(api_key, corp_code):
    url = 'https://opendart.fss.or.kr/api/elestock.json'
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
    }

    res = requests.get(url, params=params)
    list_data = json.loads(res.content)
    return list_data
