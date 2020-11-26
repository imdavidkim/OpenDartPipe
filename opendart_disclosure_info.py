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


def getConfig():
    import configparser
    global doc_path
    config = configparser.ConfigParser()
    config.read('config.ini')
    doc_path = config['COMMON']['DOCUMENT_PATH']


def get_corpcode_dict(crtfc_key):
    print("[OPENDART]기업고유번호 수집...")
    params = {'crtfc_key': crtfc_key}
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    res = requests.get(url, params=params)
    zfile = zipfile.ZipFile(io.BytesIO(res.content))
    fin = zfile.open(zfile.namelist()[0])
    # print(fin.read().decode('utf-8'))
    root = ET.fromstring(fin.read().decode('utf-8'))
    data = {}
    for child in root:
        if len(child.find('corp_code').text.strip()) > 1: # 종목고유코드가 있는 경우
            data[child.find('corp_code').text.strip()] = {"stock_code": child.find('stock_code').text.strip(), "corp_name": child.find('corp_name').text.strip()}
    print("{} CorpCode returned".format(len(data)))
    return data


def get_list_json(crtfc_key, corp_code=None, bgn_de=None, end_de=None, last_reprt_at=None, pblntf_ty=None, pblntf_detail_ty=None, corp_cls=None, sort=None, sort_mth=None, page_no=None, page_count=None):
    print("[OPENDART]공시List 수집...")
    params = {'crtfc_key': crtfc_key
              , 'corp_code': corp_code
              , 'bgn_de': bgn_de
              , 'end_de': end_de
              , 'last_reprt_at': last_reprt_at
              , 'pblntf_ty': pblntf_ty
              , 'pblntf_detail_ty': pblntf_detail_ty
              , 'corp_cls': corp_cls
              , 'sort': sort
              , 'sort_mth': sort_mth
              , 'page_no': page_no
              , 'page_count': page_count
    }
    url = "https://opendart.fss.or.kr/api/list.json"
    res = requests.get(url, params=params)
    list_data = json.loads(res.content)
    return list_data


def get_company_info_json(crtfc_key, corp_code=None):
    print("[OPENDART]기업개황 수집...")
    params = {'crtfc_key': crtfc_key
              , 'corp_code': corp_code
    }
    url = "https://opendart.fss.or.kr/api/company.json"
    res = requests.get(url, params=params)
    list_data = json.loads(res.content)
    return list_data


def get_document_xhml(api_key, rcp_no, cache=True):
    # create cache directory if not exists
    print("[OPENDART]공시서류원본파일 수집...")
    getConfig()
    # docs_cache_dir = 'docs_cache'
    if not os.path.exists(doc_path):
        os.makedirs(doc_path)

    # read and return document if exists
    fn = os.path.join(doc_path, 'dart-{}.xhml'.format(rcp_no))
    if os.path.isfile(fn) and os.path.getsize(fn) > 0:
        with open(fn, 'rt') as f:
            return f.read()

    url = 'https://opendart.fss.or.kr/api/document.xml'
    params = {
        'crtfc_key': api_key,
        'rcept_no': rcp_no,
    }

    r = requests.get(url, params=params)
    try:
        tree = ET.XML(r.content)
        status = tree.find('status').text
        message = tree.find('message').text
        if status != '000':
            raise ValueError({'status': status, 'message': message})
    except ET.ParseError as e:
        pass
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    info_list = zf.infolist()
    xml_data = zf.read(info_list[0].filename)
    xml_text = xml_data.decode('euc-kr')

    # save document to cache
    with open(fn, 'wt') as f:
        f.write(xml_text)
    return xml_text
