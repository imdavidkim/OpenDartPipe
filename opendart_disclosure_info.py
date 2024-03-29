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
    global path, doc_path, apikey, yyyymmdd
    config = configparser.ConfigParser()
    config.read(r'C:/Users/Kim/Documents/Projects/Waver/OpenDartPipe/config.ini')
    apikey = config['DART']['SEARCH-API-KEY']
    path = config['COMMON']['REPORT_PATH']
    doc_path = config['COMMON']['DOCUMENT_PATH']
    yyyymmdd = str(datetime.now())[:10]


def getKey():
    return apikey


def get_corpcode_dict(crtfc_key):
    print("[OPENDART]기업고유번호 수집...")
    getConfig()
    data = None
    yyyymmdd = str(datetime.now())[:10]
    workDir = r'{}\{}\{}'.format(path, "DartCorpCode", yyyymmdd)
    print(workDir)
    if not os.path.exists(workDir):
        os.makedirs(workDir)
    if os.path.exists(r'{}\DartCorpCode.{}.json'.format(workDir, yyyymmdd)):
        with open(r'{}\DartCorpCode.{}.json'.format(workDir, yyyymmdd), "r") as f:
            print(r"Read from {}\DartCorpCode.{}.json".format(workDir, yyyymmdd))
            corpcodes = f.read()
            data = json.loads(corpcodes)
    if data is None:
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
                data[child.find('corp_code').text.strip()] = {"corp_code": child.find('corp_code').text.strip(), "stock_code": child.find('stock_code').text.strip(), "corp_name": child.find('corp_name').text.strip()}
        print("{} CorpCode returned".format(len(data)))
        with open(r'{}\DartCorpCode.{}.json'.format(workDir, yyyymmdd), 'w') as fp:
            json.dump(data, fp)
    return data


def get_list_json(crtfc_key, corp_code=None, bgn_de=None, end_de=None, last_reprt_at="Y", pblntf_ty=None, pblntf_detail_ty=None, corp_cls=None, req_type=None, sort="date", sort_mth="desc", page_no=None, page_count=100):
    print("[OPENDART]공시List 수집...")
    retJson = None
    start = pd.to_datetime(bgn_de).strftime('%Y%m%d') if bgn_de else ''
    end = pd.to_datetime(end_de).strftime('%Y%m%d') if end_de else datetime.today().strftime('%Y%m%d')
    params = {'crtfc_key': crtfc_key
              , 'corp_code': corp_code
              , 'bgn_de': start
              , 'end_de': end
              , 'last_reprt_at': last_reprt_at
              , 'pblntf_ty': pblntf_ty
              , 'pblntf_detail_ty': pblntf_detail_ty
              , 'corp_cls': corp_cls
              , 'sort': sort
              , 'sort_mth': sort_mth
              , 'page_no': 1 if page_no is None else page_no
              , 'page_count': page_count
    }
    url = "https://opendart.fss.or.kr/api/list.json"
    res = requests.get(url, params=params)
    list_data = json.loads(res.content)
    retJson = list_data
    if list_data['status'] == '013':
        retJson["list"] = []
        return retJson
    else:
        if req_type is not None:
            tmp_list = []
            for l in list_data["list"]:
                tmp = l
                if "[기재정정]" in l["report_nm"]:
                    tmp["report_nm"] = l["report_nm"].replace("[기재정정]", "")
                tmp["report_nm"] = "{} {}".format(tmp["report_nm"].split(" ")[1], tmp["report_nm"].split(" ")[0])
                tmp_list.append(tmp)
        if list_data["total_page"] != list_data["page_no"]:
            cur_page = list_data["page_no"] + 1
            tot_page = list_data["total_page"]
            retJson = list_data
            for page in range(cur_page, tot_page+1):
                params["page_no"] = page
                res = requests.get(url, params=params)
                list_data = json.loads(res.content)
                if req_type is not None:
                    for ll in list_data["list"]:
                        tmp = ll
                        if "[기재정정]" in ll["report_nm"]:
                            tmp["report_nm"] = ll["report_nm"].replace("[기재정정]", "")
                        tmp["report_nm"] = "{} {}".format(tmp["report_nm"].split(" ")[1], tmp["report_nm"].split(" ")[0])
                        tmp_list.append(tmp)
                else:
                    retJson["list"].extend(list_data["list"])
            # print(retJson["total_count"], len(retJson["list"]))
        if "page_no" in retJson.keys(): retJson.pop("page_no", None)
        if "page_count" in retJson.keys(): retJson.pop("page_count", None)
        if "total_page" in retJson.keys(): retJson.pop("total_page", None)
        if req_type is not None: retJson["list"] = sorted(tmp_list, key=lambda k:k["report_nm"], reverse=True)
        return retJson


def get_company_info_json(crtfc_key, corp_code=None):
    print("[OPENDART]기업개황 수집...")
    params = {'crtfc_key': crtfc_key
              , 'corp_code': corp_code
    }
    url = "https://opendart.fss.or.kr/api/company.json"
    res = requests.get(url, params=params)
    list_data = json.loads(res.content)
    return list_data


def get_document_xhml(api_key, rcp_no, stock_code, corp_code, corp_name, dir_name, cache=True):
    # create cache directory if not exists
    # print("[OPENDART]공시서류원본파일 수집...")
    getConfig()
    # docs_cache_dir = 'docs_cache'
    work_path = r'{}\{}\{}'.format(doc_path, dir_name, yyyymmdd)
    if not os.path.exists(work_path):
        os.makedirs(work_path)

    # read and return document if exists
    fn = os.path.join(work_path, '[{}][{}]{}-{}.xml'.format(stock_code, corp_code, corp_name, rcp_no))
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
            if status == '014':
                print(rcp_no, stock_code, corp_code, corp_name, {'status': status, 'message': message})
                return None
            else:
                raise ValueError({'status': status, 'message': message})
    except ET.ParseError as e:
        pass
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    info_list = zf.infolist()
    print(info_list)
    for i in range(len(info_list)):
        xml_data = zf.read(info_list[i].filename)
        try:
            xml_text = xml_data.replace(b'encoding="utf-8"', b'encoding="euc-kr"').decode('euc-kr')
            # xml_text = xml_data.decode('x-windows-949')
        except Exception as e:
            # print(rcp_no, stock_code, corp_code, corp_name, xml_data)
            xml_text = xml_data.replace(b'encoding="utf-8"', b'encoding="euc-kr"').decode('cp949')

        # save document to cache
        fn = os.path.join(work_path, '[{}][{}]{}-{}_{}.xml'.format(stock_code, corp_code, corp_name, rcp_no, info_list[i].filename))
        with open(fn, 'wt') as f:
            f.write(xml_text)

    return info_list
