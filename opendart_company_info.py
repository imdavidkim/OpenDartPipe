# -*- coding: utf-8 -*-

import io, requests
import json
import zipfile
import xml.etree.ElementTree as et


def get_fnlttSinglAcnt_json(crtfc_key, corp_code, bsns_year, reprt_code="11014"):
    print("[OPENDART]단일회사 주요계정정보 수집...")
    params = {'crtfc_key': crtfc_key
              , 'corp_code': corp_code
              , 'bsns_year': bsns_year
              , 'reprt_code': reprt_code
    }
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    res = requests.get(url, params=params)
    acnt_data = json.loads(res.content)
    return acnt_data


def get_fnlttSinglAcntAll_json(crtfc_key, corp_code, bsns_year, reprt_code="11014", fs_div="CFS"):
    print("[OPENDART]단일회사 전체재무제표정보 수집...")
    params = {'crtfc_key': crtfc_key
              , 'corp_code': corp_code
              , 'bsns_year': bsns_year
              , 'reprt_code': reprt_code
              , 'fs_div': fs_div
    }
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    res = requests.get(url, params=params)
    acnt_all_data = json.loads(res.content)
    return acnt_all_data
