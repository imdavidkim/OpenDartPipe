# -*- coding: utf-8 -*-
import OpenDartPipe.opendart_disclosure_info as di
import OpenDartPipe.opendart_company_info as ci
import OpenDartPipe.opendart_bizreport_info as bi
import OpenDartPipe.opendart_sharedstock_info as si
import watson.db_factory as db
import detective.fnguide_collector as fc
import detective.messenger as messenger
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json


class Pipe:
    api_key = ""
    corp_codes = {}
    reprt_ty_codes = {
        "1분기보고서": "11013",
        "반기보고서": "11012",
        "3분기보고서": "11014",
        "사업보고서": "11011"
    }
    pblntf_ty_codes = {
        "정기공시": "A",
        "주요사항보고": "B",
        "발행공시": "C",
        "지분공시": "D",
        "기타공시": "E",
        "외부감사관련": "F",
        "펀드공시": "G",
        "자산유동화": "H",
        "거래소공시": "I",
        "공정위공시": "J"
    }
    pblntf_detail_ty_codes = {
        "사업보고서": "A001",
        "반기보고서": "A002",
        "분기보고서": "A003",
        "등록법인결산서류(자본시장법이전)": "A004",
        "소액공모법인결산서류": "A005",
        "주요사항보고서": "B001",
        "주요경영사항신고(자본시장법 이전)": "B002",
        "최대주주등과의거래신고(자본시장법 이전)": "B003",
        "증권신고(지분증권)": "C001",
        "증권신고(채무증권)": "C002",
        "증권신고(파생결합증권)": "C003",
        "증권신고(합병등)": "C004",
        "증권신고(기타)": "C005",
        "소액공모(지분증권)": "C006",
        "소액공모(채무증권)": "C007",
        "소액공모(파생결합증권)": "C008",
        "소액공모(합병등)": "C009",
        "소액공모(기타)": "C010",
        "호가중개시스템을통한소액매출": "C011",
        "주식등의대량보유상황보고서": "D001",
        "임원ㆍ주요주주특정증권등소유상황보고서": "D002",
        "의결권대리행사권유": "D003",
        "공개매수": "D004",
        "자기주식취득/처분": "E001",
        "신탁계약체결/해지": "E002",
        "합병등종료보고서": "E003",
        "주식매수선택권부여에관한신고": "E004",
        "사외이사에관한신고": "E005",
        "주주총회소집공고": "E006",
        "시장조성/안정조작": "E007",
        "합병등신고서(자본시장법 이전)": "E008",
        "금융위등록/취소(자본시장법 이전)": "E009",
        "감사보고서": "F001",
        "연결감사보고서": "F002",
        "결합감사보고서": "F003",
        "회계법인사업보고서": "F004",
        "감사전재무제표미제출신고서": "F005",
        "증권신고(집합투자증권-신탁형)": "G001",
        "증권신고(집합투자증권-회사형)": "G002",
        "증권신고(집합투자증권-합병)": "G003",
        "자산유동화계획/양도등록": "H001",
        "사업/반기/분기보고서": "H002",
        "증권신고(유동화증권등)": "H003",
        "채권유동화계획/양도등록": "H004",
        "수시보고": "H005",
        "주요사항보고서(거래소)": "H006",
        "수시공시": "I001",
        "공정공시": "I002",
        "시장조치/안내": "I003",
        "지분공시": "I004",
        "증권투자회사": "I005",
        "채권공시": "I006",
        "대규모내부거래관련": "J001",
        "대규모내부거래관련(구)": "J002",
        "기업집단현황공시": "J004",
        "비상장회사중요사항공시": "J005",
        "기타공정위공시": "J006",

    }
    corp_cls_codes = {
        "유가": "Y",
        "코스닥": "K",
        "코넥스": "N",
        "기타": "E"
    }

    # 2020 스케쥴표
    reporting_schedules = {
        "01": {"1분기보고서": "04", "반기보고서": "07", "3분기보고서": "10", "사업보고서": "01"},
        "02": {"1분기보고서": "05", "반기보고서": "08", "3분기보고서": "11", "사업보고서": "02"},
        "03": {"1분기보고서": "06", "반기보고서": "09", "3분기보고서": "12", "사업보고서": "03"},
        "04": {"1분기보고서": "07", "반기보고서": "10", "3분기보고서": "01", "사업보고서": "04"},
        "05": {"1분기보고서": "08", "반기보고서": "11", "3분기보고서": "02", "사업보고서": "05"},
        "06": {"1분기보고서": "09", "반기보고서": "12", "3분기보고서": "03", "사업보고서": "06"},
        "07": {"1분기보고서": "10", "반기보고서": "01", "3분기보고서": "04", "사업보고서": "07"},
        "08": {"1분기보고서": "11", "반기보고서": "02", "3분기보고서": "05", "사업보고서": "08"},
        "09": {"1분기보고서": "12", "반기보고서": "03", "3분기보고서": "06", "사업보고서": "09"},
        "10": {"1분기보고서": "01", "반기보고서": "04", "3분기보고서": "07", "사업보고서": "10"},
        "11": {"1분기보고서": "02", "반기보고서": "05", "3분기보고서": "08", "사업보고서": "11"},
        "12": {"1분기보고서": "03", "반기보고서": "06", "3분기보고서": "09", "사업보고서": "12"}

    }

    def create(self):
        di.getConfig()
        self.api_key = di.getKey()
        if self.corp_codes == {}:
            self.corp_codes = di.get_corpcode_dict(self.api_key)

    # -- Opendart : 공시정보
    def get_corp_code(self, info):
        if self.corp_codes == {}:
            self.corp_codes = di.get_corpcode_dict(self.api_key)

        if self.corp_codes != {} and info.isdigit():
            if len(info) == 6:
                res = list(filter(lambda corp: corp['stock_code'] == info, self.corp_codes.values()))
            else:
                res = list(filter(lambda corp: corp['corp_code'] == info, self.corp_codes.values()))
            if len(res) > 0:
                return True, res[0]["corp_code"]
            else:
                return False, None
        elif self.corp_codes != {} and info.isdigit() is not True:
            res = list(filter(lambda corp: corp['corp_name'] == info, self.corp_codes.values()))
            if len(res) > 0:
                return True, res[0]["corp_code"]
            else:
                return False, None

    def get_list(self, corp_code=None, bgn_de=None, end_de=None, last_reprt_at="Y", pblntf_ty=None,
                 pblntf_detail_ty=None, corp_cls=None):
        retObj = None
        if corp_code:
            ret, code = self.get_corp_code(corp_code)
        else:
            ret = True
            code = None
        if ret:
            return di.get_list_json(self.api_key, code, bgn_de, end_de, last_reprt_at, pblntf_ty, pblntf_detail_ty,
                                    corp_cls)

    def get_company_info(self, corp_code):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return di.get_company_info_json(self.api_key, code)
    # -- Opendart : 공시정보 끝

    # -- Opendart : 상장기업 재무정보
    def get_fnlttSinglAcntAll(self, corp_code, bsns_year, reprt_code="11014"):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return ci.get_fnlttSinglAcntAll_json(self.api_key, code, bsns_year, reprt_code)

    def get_fnlttSinglAcnt(self, corp_code, bsns_year, reprt_code="11014"):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return ci.get_fnlttSinglAcnt_json(self.api_key, code, bsns_year, reprt_code)

    # -- Opendart : 상장기업 재무정보 끝

    # -- Opendart : 지분공시 종합정보
    def get_majorstock(self, corp_code):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return si.get_majorstock_json(self.api_key, code)

    def get_elestock(self, corp_code):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return si.get_elestock_json(self.api_key, code)

    def get_shared_reporting(self, base_date, target_date=None):
        if target_date is None:
            db.ResultListDataStore(self.get_list(bgn_de=base_date, pblntf_ty=self.pblntf_ty_codes["지분공시"]))
        else:
            db.ResultListDataStore(
                self.get_list(bgn_de=base_date, end_de=target_date, pblntf_ty=self.pblntf_ty_codes["지분공시"]))

    def get_majorevent_reporting(self, base_date, target_date=None):
        if target_date is None:
            db.ResultListDataStore(self.get_list(bgn_de=base_date, pblntf_ty=self.pblntf_ty_codes["주요사항보고"],
                                                 pblntf_detail_ty=self.pblntf_detail_ty_codes["주요사항보고서"]))
        else:
            db.ResultListDataStore(
                self.get_list(bgn_de=base_date, end_de=target_date, pblntf_ty=self.pblntf_ty_codes["주요사항보고"],
                              pblntf_detail_ty=self.pblntf_detail_ty_codes["주요사항보고서"]))

    def get_krx_reporting(self, base_date, target_date=None):
        if target_date is None:
            db.ResultListDataStore(self.get_list(bgn_de=base_date, pblntf_ty=self.pblntf_ty_codes["거래소공시"],
                                                 pblntf_detail_ty=self.pblntf_detail_ty_codes["공정공시"]))
        else:
            db.ResultListDataStore(
                self.get_list(bgn_de=base_date, end_de=target_date, pblntf_ty=self.pblntf_ty_codes["거래소공시"],
                              pblntf_detail_ty=self.pblntf_detail_ty_codes["공정공시"]))

    def get_majorshareholder_reporting(self, base_date):
        corp_code_list = db.getMajorShareholderReportingInfo(base_date)
        msg = {}
        for corp in corp_code_list:
            # self.get_document_xhml(corp["rcept_no"], corp["corp_code"], corp["corp_name"], "MajorShareHolder")
            result = dart.get_majorstock(corp["corp_code"])
            currData = None
            try:

                for jsonData in result["list"]:
                    currData = jsonData
                    if jsonData["rcept_dt"].replace("-", "") >= base_date \
                            and float(jsonData["stkrt"]) > 5.0 \
                            and float(jsonData["stkrt_irds"]) > 0 \
                            and ("1%" in jsonData["report_resn"]
                                 or "5%" in jsonData["report_resn"]
                                 or "신규" in jsonData["report_resn"]
                                 or "인수" in jsonData["report_resn"]
                                 or "매수" in jsonData["report_resn"]
                    ):
                        msg[corp["corp_name"]] = {"corp_code": corp["corp_code"], "stock_code": corp["stock_code"],
                                                  "url": "http://dart.fss.or.kr/dsaf001/main.do?rcpNo={}".format(
                                                      corp["rcept_no"])}
                        soup = BeautifulSoup(
                            self.get_document_xhml(jsonData["rcept_no"], corp["stock_code"], jsonData["corp_code"],
                                                   jsonData["corp_name"], "MajorShareHolder"), 'lxml')
                        call = json.loads(requests.get(
                            "https://api.finance.naver.com/service/itemSummary.nhn?itemcode={}".format(
                                corp["stock_code"])).content.decode("utf-8"))

                        msg[corp["corp_name"]]["시가총액"] = f'{call["marketSum"] * 1000000:,}'
                        msg[corp["corp_name"]]["PER"] = call["per"]
                        msg[corp["corp_name"]]["EPS"] = call["eps"]
                        msg[corp["corp_name"]]["PBR"] = call["pbr"]
                        msg[corp["corp_name"]]["현재가"] = f'{call["now"]:,}'
                        db.ResultMajorShareholderDataStore(jsonData)
                    elif jsonData["rcept_dt"].replace("-", "") >= base_date \
                            and float(jsonData["stkrt"]) > 5.0 \
                            and float(jsonData["stkrt_irds"]) > 0:
                        pass
                        # print(jsonData)
            except Exception as e:
                print(e)
                print(currData)
        for m in msg:
            print(m, msg[m])

    def get_document_xhml(self, rcp_no, stock_code, corp_code, corp_name, dir_name, cache=True):
        return di.get_document_xhml(self.api_key, rcp_no, stock_code, corp_code, corp_name, dir_name, cache)

    def get_freecapital_increasing_corp_info(self, base_date):
        corp_code_list = db.getFreeCapitalIncreaseEventReportingInfo(base_date)
        print(corp_code_list)
        msg = {}
        for corp in corp_code_list:
            if corp["stock_code"] == "": continue
            print(corp["rcept_no"], corp["corp_code"], corp["corp_name"])
            msg[corp["corp_name"]] = {"rcept_no": corp["rcept_no"], "corp_code": corp["corp_code"],
                                      "stock_code": corp["stock_code"], "보고서명": corp["report_nm"],
                                      "url": "http://dart.fss.or.kr/dsaf001/main.do?rcpNo={}".format(corp["rcept_no"])}
            soup = BeautifulSoup(
                self.get_document_xhml(corp["rcept_no"], corp["stock_code"], corp["corp_code"], corp["corp_name"],
                                       "FreeCapitalIncreasing"), 'lxml')

            # 신주배정기준일
            ALL_BS_DT = soup.find_all('tu', {'aunit': "ALL_BS_DT"})
            if isinstance(ALL_BS_DT, list) and len(ALL_BS_DT) > 0:
                msg[corp["corp_name"]]["신주배정기준일"] = ALL_BS_DT[0].text
            else:
                pass
            # 신주의 배당기산일
            DV_ST_DT = soup.find_all('tu', {'aunit': "DV_ST_DT"})
            if isinstance(DV_ST_DT, list) and len(DV_ST_DT) > 0:
                msg[corp["corp_name"]]["신주의 배당기산일"] = DV_ST_DT[0].text
            else:
                pass
            # 신주의 상장 예정일
            LST_PLN_DT = soup.find_all('tu', {'aunit': "LST_PLN_DT"})
            if isinstance(LST_PLN_DT, list) and len(LST_PLN_DT) > 0:
                msg[corp["corp_name"]]["신주의 상장 예정일"] = LST_PLN_DT[0].text
            else:
                pass
            # 1주당 신주배정 주식수
            NEW_ASN_CST = soup.find_all('te', {'acode': "NEW_ASN_CST"})
            if isinstance(NEW_ASN_CST, list) and len(NEW_ASN_CST) > 0:
                msg[corp["corp_name"]]["1주당 신주배정 주식수"] = NEW_ASN_CST[0].text
            else:
                pass
            # 재원
            paragraph = soup.find_all('p')
            if isinstance(paragraph, list) and len(paragraph) > 0:
                for RESOURCE in paragraph:
                    if "재원" in RESOURCE.text:
                        msg[corp["corp_name"]]["신주정보"] = RESOURCE.text.replace("&cr;", "\n").replace("\n\n", "\n")
            else:
                pass
            call = json.loads(requests.get("https://api.finance.naver.com/service/itemSummary.nhn?itemcode={}".format(
                corp["stock_code"])).content.decode("utf-8"))
            msg[corp["corp_name"]]["시가총액"] = f'{call["marketSum"] * 1000000:,}'
            msg[corp["corp_name"]]["PER"] = call["per"]
            msg[corp["corp_name"]]["EPS"] = call["eps"]
            msg[corp["corp_name"]]["PBR"] = call["pbr"]
            msg[corp["corp_name"]]["현재가"] = f'{call["now"]:,}'

        txt = ""
        for idx, m in enumerate(msg):
            # print(m, msg[m])
            txt += "{} 무상증자공시정보\n".format(msg[m]["rcept_no"][:8])
            txt += "<b>{}</b> [<a href='{}'>공시문서열기</a>]\n".format(m, msg[m]["url"])
            for key in msg[m].keys():
                if key in ['corp_code', 'stock_code', 'url']:
                    continue
                elif key in ['신주정보']:
                    txt += "- {}\n{}\n".format(key, msg[m][key])
                else:
                    txt += "- {} : {}\n".format(key, msg[m][key])
            messenger.free_cap_inc_message_to_telegram(txt)
            txt = ""

    def get_provisional_performance_reporting_corp_info(self, base_date, target_date=None):
        from html_table_parser import parser_functions as parser
        corp_code_list = None
        if target_date is None:
            corp_code_list = db.getProvisionalPerformanceReportingInfo(base_date)
        else:
            corp_code_list = db.getProvisionalPerformanceReportingInfo(base_date, target_date)
        print(corp_code_list)
        data = {}
        unit = None

        for corp in corp_code_list:
            if corp["stock_code"] == "": continue
            print(corp["rcept_no"], corp["corp_code"], corp["corp_name"])
            data[corp["stock_code"]] = {"corp_code": corp["corp_code"],
                                        "corp_name": corp["corp_name"],
                                        "PL": {"Y": {}, "Q": {}}}
            doc = self.get_document_xhml(corp["rcept_no"], corp["stock_code"], corp["corp_code"], corp["corp_name"],
                                         "ProvisionalPerformance")
            if doc:
                unit = None
                soup = BeautifulSoup(doc, 'html.parser')

                # html_table = parser.make2d(soup)
                # print(html_table)
                table = soup.find("table", id=lambda value: value and value.startswith("XFormD1"))
                # if table["id"] == "XFormD1_Form0_RepeatTable0": # 코스피
                #     pass
                # elif table["id"] == "XFormD1_Form0_RepeatTable1": # 코스닥
                #     pass

                html_table_list = parser.make2d(table)
                for h in html_table_list:
                    for c in h:
                        # print(c)
                        if "단위" in c:
                            tmp = c.split(":")[1].strip()
                            if ',' in tmp:
                                tmp2 = tmp.split(",")[0].strip()
                            else:
                                tmp2 = tmp
                            # print("[{}]".format(tmp2))
                            if tmp2 == "백만원":
                                unit = 1000000
                            elif tmp2 == "억원":
                                unit = 100000000
                            elif tmp2 == "십억원":
                                unit = 1000000000
                            elif tmp2 == "백억원":
                                unit = 10000000000
                            elif tmp2 == "천억원":
                                unit = 100000000000
                            elif tmp2 == "조원":
                                unit = 1000000000000
                            break
                    if unit is not None: break
                for h in html_table_list:
                    if h[0] in ['매출액', '영업이익', '법인세비용차감전계속사업이익', '당기순이익', '지배기업 소유주지분 순이익']:
                        if h[1] == "당해실적":
                            if h[2] != "-":
                                data[corp["stock_code"]]["PL"]["Q"][h[0]] = float(h[2].replace(",", "")) * unit
                            else:
                                data[corp["stock_code"]]["PL"]["Q"][h[0]] = 0
                        if h[1] == "누계실적":
                            if h[2] != "-":
                                data[corp["stock_code"]]["PL"]["Y"][h[0]] = float(h[2].replace(",", "")) * unit
                            else:
                                data[corp["stock_code"]]["PL"]["Y"][h[0]] = 0
        print(data)
        return data

    def get_provisional_performance_reporting_corp_info_with_code(self, code, base_date, target_date=None):
        from html_table_parser import parser_functions as parser

        corp_code_list = None
        if target_date is None:
            corp_code_list = db.getProvisionalPerformanceReportingInfoWithStockCode(code, base_date)
        else:
            corp_code_list = db.getProvisionalPerformanceReportingInfoWithStockCode(code, base_date, target_date)
        print(corp_code_list)
        data = {}
        unit = None

        for corp in corp_code_list:
            if corp["stock_code"] == "": continue
            print(corp["rcept_no"], corp["corp_code"], corp["corp_name"])
            data[corp["stock_code"]] = {"corp_code": corp["corp_code"],
                                        "corp_name": corp["corp_name"],
                                        "PL": {"Y": {}, "Q": {}}}
            doc = self.get_document_xhml(corp["rcept_no"], corp["stock_code"], corp["corp_code"], corp["corp_name"],
                                         "ProvisionalPerformance")
            if doc:
                unit = None
                soup = BeautifulSoup(doc, 'html.parser')

                # html_table = parser.make2d(soup)
                # print(html_table)
                table = soup.find("table", id=lambda value: value and value.startswith("XFormD1"))
                # if table["id"] == "XFormD1_Form0_RepeatTable0": # 코스피
                #     pass
                # elif table["id"] == "XFormD1_Form0_RepeatTable1": # 코스닥
                #     pass

                html_table_list = parser.make2d(table)
                for h in html_table_list:
                    for c in h:
                        # print(c)
                        if "단위" in c:
                            if len(c) > 20: continue
                            tmp = c.split(":")[1].strip()
                            if ',' in tmp:
                                tmp2 = tmp.split(",")[0].strip()
                            else:
                                tmp2 = tmp
                            print("[{}]".format(tmp2))
                            if tmp2 == "백만원":
                                unit = 1000000
                            elif tmp2 == "억원":
                                unit = 100000000
                            elif tmp2 == "십억원":
                                unit = 1000000000
                            elif tmp2 == "백억원":
                                unit = 10000000000
                            elif tmp2 == "천억원":
                                unit = 100000000000
                            elif tmp2 == "조원":
                                unit = 1000000000000
                            break
                    if unit is not None: break
                for h in html_table_list:
                    if h[0] in ['매출액', '영업이익', '법인세비용차감전계속사업이익', '당기순이익', '지배기업 소유주지분 순이익']:
                        if h[1] == "당해실적":
                            if h[2] != "-":
                                data[corp["stock_code"]]["PL"]["Q"][h[0]] = float(h[2].replace(",", "")) * unit
                            else:
                                data[corp["stock_code"]]["PL"]["Q"][h[0]] = 0
                        if h[1] == "누계실적":
                            if h[2] != "-":
                                data[corp["stock_code"]]["PL"]["Y"][h[0]] = float(h[2].replace(",", "")) * unit
                            else:
                                data[corp["stock_code"]]["PL"]["Y"][h[0]] = 0
        # print(data)
        return data

    def get_req_lists(self, lists):
        req_list = []
        req_list2 = []
        base_idx = None
        base_year = None
        if len(lists) > 0:
            comp_info = self.get_company_info(lists[0]["corp_code"])
            print(comp_info)
            res = list(filter(lambda l: "사업" in l['report_nm'], lists))
            if len(res) > 0:
                base_idx = lists.index(res[0])
                base_year = res[0]['report_nm'].split(".")[0][-4:]
                reprt_type = "사업보고서"
                reprt_ty_code = self.reprt_ty_codes[reprt_type]
                req_list.append((base_year, reprt_ty_code))
                if base_idx == 0: req_list2.append((base_year, reprt_ty_code))
                # req_list2.append((base_year, reprt_ty_code))
            res = list(filter(lambda l: "반기" in l['report_nm'], lists))
            if len(res) > 0:
                base_idx = lists.index(res[0])
                base_year = res[0]['report_nm'].split(".")[0][-4:]
                reprt_type = "반기보고서"
                reprt_ty_code = self.reprt_ty_codes[reprt_type]
                req_list.append((base_year, reprt_ty_code))
                if base_idx == 0: req_list2.append((base_year, reprt_ty_code))
            # print(res, lists.index(res[0]) if len(res) > 0 else None, reprt_ty_code)
            res = list(filter(lambda l: "분기" in l['report_nm'], lists))
            if len(res) > 0:
                for r in res:
                    reprt_ty_code = None
                    mm = r['report_nm'].split(".")[1][:2]
                    base_idx = lists.index(r)
                    base_year = r['report_nm'].split(".")[0][-4:]
                    for key in self.reporting_schedules[comp_info["acc_mt"]].keys():
                        if mm == self.reporting_schedules[comp_info["acc_mt"]][key]:
                            reprt_ty_code = self.reprt_ty_codes[key]
                            req_list.append((base_year, reprt_ty_code))
                            break
                    if base_idx == 0: req_list2.append((base_year, reprt_ty_code))
            # first_list = lists[:1]
            # if first_list:
            #     base_year = res[0]['report_nm'].split(".")[0][-4:]
            #     reprt_type = "반기보고서"
            #     reprt_ty_code = self.reprt_ty_codes[reprt_type]
            #     req_list.append([base_year, reprt_ty_code])
        req_list2 = list(set(req_list2))
        return req_list, req_list2

    def get_fnlttSinglAcnt_from_req_list(self, corp_code, req_list, all_or_not=None):
        retDict = {}
        curr = None
        ret = None
        yyyy_report_nm = None
        trm_nm = {}
        for bsns_year, reprt_code in req_list:
            if reprt_code == "11011":
                yyyy_report_nm = "{} 4/4".format(bsns_year)
            elif reprt_code == "11012":
                yyyy_report_nm = "{} 2/4".format(bsns_year)
            elif reprt_code == "11013":
                yyyy_report_nm = "{} 1/4".format(bsns_year)
            elif reprt_code == "11014":
                yyyy_report_nm = "{} 3/4".format(bsns_year)
            try:
                report_nm_split = yyyy_report_nm.split(" ")
                trm_nm["thstrm_nm"] = yyyy_report_nm
                trm_nm["frmtrm_nm"] = "{} {}".format(str(int(report_nm_split[0])-1), report_nm_split[1])
                trm_nm["bfefrmtrm_nm"] = "{} {}".format(str(int(report_nm_split[0])-2), report_nm_split[1])
                if all_or_not:
                    ret = self.get_fnlttSinglAcntAll(corp_code, bsns_year, reprt_code)
                else:
                    ret = self.get_fnlttSinglAcnt(corp_code, bsns_year, reprt_code)
                # print(ret)
                if "list" in ret.keys():
                    for l in ret["list"]:
                        curr = l
                        print(l)
                        # if reprt_code == "11011": print(l)
                        if "fs_nm" not in l.keys():
                            l["fs_nm"] = "연결재무제표"
                            # print(l)
                        if l["fs_nm"] not in retDict.keys():
                            retDict[l["fs_nm"]] = {}
                        # print(retDict)
                        if l["sj_nm"] not in retDict[l["fs_nm"]].keys():
                            retDict[l["fs_nm"]][l["sj_nm"]] = {}
                        # print(retDict)
                        if l["account_nm"] not in retDict[l["fs_nm"]][l["sj_nm"]].keys():
                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]] = {}
                        # print(retDict)
                        if l["sj_nm"] == "재무상태표" or l["sj_nm"] == "현금흐름표" or l["sj_nm"] == "자본변동표":
                            if yyyy_report_nm not in retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]].keys():
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm] = {}
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["{} Rate".format(yyyy_report_nm)] = {}
                            if reprt_code == "11011":  # and l["fs_div"] == "CFS":
                                if "bfefrmtrm_dt" not in l.keys() and "bfefrmtrm_nm" in l.keys():
                                    l["bfefrmtrm_dt"] = l["bfefrmtrm_nm"]
                                if "frmtrm_dt" not in l.keys() and "frmtrm_nm" in l.keys():
                                    l["frmtrm_dt"] = l["frmtrm_nm"]
                                if "thstrm_dt" not in l.keys() and "thstrm_nm" in l.keys():
                                    l["thstrm_dt"] = l["thstrm_nm"]

                                if "bfefrmtrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["bfefrmtrm_nm"]] = l["bfefrmtrm_amount"] if l[
                                                                                               "bfefrmtrm_amount"] != "-" and \
                                                                                           l[
                                                                                               "bfefrmtrm_amount"] != "" else "0"
                                if "frmtrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["frmtrm_nm"]] = \
                                        l["frmtrm_amount"] if l["frmtrm_amount"] != "-" and l[
                                            "frmtrm_amount"] != "" else "0"
                                    if "bfefrmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["bfefrmtrm_nm"]] != "0" and l["frmtrm_amount"] != "-" and l[
                                            "frmtrm_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][
                                                "{} Rate".format(yyyy_report_nm)]["전기"] = round((float(
                                                l["frmtrm_amount"].replace(",", "")) - float(
                                                l["bfefrmtrm_amount"].replace(",", ""))) / float(
                                                l["bfefrmtrm_amount"].replace(",", "")) * 100, 2)
                                if "thstrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["thstrm_nm"]] = \
                                        l["thstrm_amount"] if l["thstrm_amount"] != "-" and l[
                                            "thstrm_amount"] != "" else "0"
                                    if "frmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["frmtrm_nm"]] != "0" and l["thstrm_amount"] != "-" and l[
                                            "thstrm_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][
                                                "{} Rate".format(yyyy_report_nm)]["당기"] = round((float(
                                                l["thstrm_amount"].replace(",", "")) - float(
                                                l["frmtrm_amount"].replace(",", ""))) / float(
                                                l["frmtrm_amount"].replace(",", "")) * 100, 2)
                            else:
                                if "frmtrm_dt" not in l.keys() and "frmtrm_nm" in l.keys():
                                    l["frmtrm_dt"] = l["frmtrm_nm"]
                                if "thstrm_dt" not in l.keys() and "thstrm_nm" in l.keys():
                                    l["thstrm_dt"] = l["thstrm_nm"]

                                # if "frmtrm_dt" in l.keys() and "frmtrm_add_amount" in l.keys():
                                #     retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["frmtrm_nm"]] = \
                                #     l["frmtrm_add_amount"] if l["frmtrm_add_amount"] != "-" else "0"
                                # if "thstrm_dt" in l.keys() and "thstrm_add_amount" in l.keys():
                                #     retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["thstrm_nm"]] = \
                                #     l["thstrm_add_amount"] if l["thstrm_add_amount"] != "-" else "0"
                                if "frmtrm_dt" in l.keys() and "frmtrm_amount" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["frmtrm_nm"]] = \
                                        l["frmtrm_amount"] if l["frmtrm_amount"] != "-" and l[
                                            "frmtrm_amount"] != "" else "0"
                                if "thstrm_dt" in l.keys() and "thstrm_amount" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["thstrm_nm"]] = \
                                        l["thstrm_amount"] if l["thstrm_amount"] != "-" and l[
                                            "thstrm_amount"] != "" else "0"
                                    if "frmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][yyyy_report_nm][trm_nm["frmtrm_nm"]] != "0" and l["thstrm_amount"] != "-" and l[
                                            "thstrm_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]][
                                                "{} Rate".format(yyyy_report_nm)]["당기"] = round((float(
                                                l["thstrm_amount"].replace(",", "")) - float(
                                                l["frmtrm_amount"].replace(",", ""))) / float(
                                                l["frmtrm_amount"].replace(",", "")) * 100, 2)

                        if l["sj_nm"] == "손익계산서" or l["sj_nm"] == "포괄손익계산서":
                            if reprt_code == "11011":
                                if "frmtrm_dt" not in l.keys() and "frmtrm_nm" in l.keys():
                                    l["frmtrm_dt"] = l["frmtrm_nm"]
                                if "thstrm_dt" not in l.keys() and "thstrm_nm" in l.keys():
                                    l["thstrm_dt"] = l["thstrm_nm"]
                            else:
                                if "frmtrm_amount" not in l.keys() and "frmtrm_q_nm" in l.keys():
                                    l["frmtrm_dt"] = l["frmtrm_q_nm"]
                                    l["frmtrm_amount"] = l["frmtrm_q_amount"]
                                if "thstrm_dt" not in l.keys() and "thstrm_nm" in l.keys():
                                    l["thstrm_dt"] = l["thstrm_nm"]
                            if "누계" not in retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]].keys():
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"] = {}
                            if "당기" not in retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]].keys():
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"] = {}
                            if yyyy_report_nm not in retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"].keys():
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm] = {}
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][
                                    "{} Rate".format(yyyy_report_nm)] = {"전기": None, "당기": None}
                            if yyyy_report_nm not in retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"].keys():
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][yyyy_report_nm] = {}
                                retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][
                                    "{} Rate".format(yyyy_report_nm)] = {"전기": None, "당기": None}

                            if reprt_code == "11011":
                                # if reprt_code == "11011" and l["fs_div"] == "CFS":
                                if "bfefrmtrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                        trm_nm["bfefrmtrm_nm"]] = l["bfefrmtrm_amount"] if l[
                                                                                               "bfefrmtrm_amount"] != "-" and \
                                                                                           l[
                                                                                               "bfefrmtrm_amount"] != "" else "0"
                                if "frmtrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                        trm_nm["frmtrm_nm"]] = l["frmtrm_amount"] if l["frmtrm_amount"] != "-" and l[
                                        "frmtrm_amount"] != "" else "0"
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][yyyy_report_nm][
                                        trm_nm["frmtrm_nm"]] = l["frmtrm_amount"] if l["frmtrm_amount"] != "-" and l[
                                        "frmtrm_amount"] != "" else "0"
                                    if "bfefrmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                            trm_nm["bfefrmtrm_nm"]] != "0" and l["frmtrm_amount"] != "-" and l[
                                            "frmtrm_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][
                                                "{} Rate".format(yyyy_report_nm)]["전기"] = round((float(
                                                l["frmtrm_amount"].replace(",", "")) - float(
                                                l["bfefrmtrm_amount"].replace(",", ""))) / float(
                                                l["bfefrmtrm_amount"].replace(",", "")) * 100, 2) if l[
                                                                                                         "frmtrm_amount"] != "" and \
                                                                                                     l[
                                                                                                         "bfefrmtrm_amount"] != "" else 0
                                if "thstrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                        trm_nm["thstrm_nm"]] = l["thstrm_amount"] if l["thstrm_amount"] != "-" and l[
                                        "thstrm_amount"] != "" else "0"
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][yyyy_report_nm][
                                        trm_nm["thstrm_nm"]] = l["thstrm_amount"] if l["thstrm_amount"] != "-" and l[
                                        "thstrm_amount"] != "" else "0"
                                    if "frmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                            trm_nm["frmtrm_nm"]] != "0" and l["thstrm_amount"] != "-" and l[
                                            "thstrm_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][
                                                "{} Rate".format(yyyy_report_nm)]["당기"] = round((float(
                                                l["thstrm_amount"].replace(",", "")) - float(
                                                l["frmtrm_amount"].replace(",", ""))) / float(
                                                l["frmtrm_amount"].replace(",", "")) * 100, 2) if l[
                                                                                                      "thstrm_amount"] != "" and \
                                                                                                  l[
                                                                                                      "frmtrm_amount"] != "" else 0
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][
                                                "{} Rate".format(yyyy_report_nm)]["당기"] = round((float(
                                                l["thstrm_amount"].replace(",", "")) - float(
                                                l["frmtrm_amount"].replace(",", ""))) / float(
                                                l["frmtrm_amount"].replace(",", "")) * 100, 2) if l[
                                                                                                      "thstrm_amount"] != "" and \
                                                                                                  l[
                                                                                                      "frmtrm_amount"] != "" else 0
                            else:
                                # if l["account_nm"] == "영업이익" and corp_code == "00126308":
                                #     print()
                                if "frmtrm_dt" in l.keys() and "frmtrm_add_amount" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                        trm_nm["frmtrm_nm"]] = l["frmtrm_add_amount"] if l[
                                                                                        "frmtrm_add_amount"] != "-" else "0"
                                if "thstrm_dt" in l.keys() and "thstrm_add_amount" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                        trm_nm["thstrm_nm"]] = l["thstrm_add_amount"] if l[
                                                                                        "thstrm_add_amount"] != "-" else "0"
                                    if "frmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][yyyy_report_nm][
                                            trm_nm["frmtrm_nm"]] != "0" and l["thstrm_add_amount"] != "-" and l[
                                            "thstrm_add_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["누계"][
                                                "{} Rate".format(yyyy_report_nm)]["당기"] = round((float(
                                                l["thstrm_add_amount"].replace(",", "")) - float(
                                                l["frmtrm_add_amount"].replace(",", ""))) / float(
                                                l["frmtrm_add_amount"].replace(",", "")) * 100, 2) if l[
                                                                                                          "thstrm_add_amount"] != "" and \
                                                                                                      l[
                                                                                                          "frmtrm_add_amount"] != "" else 0
                                if "frmtrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][yyyy_report_nm][
                                        trm_nm["frmtrm_nm"]] = l["frmtrm_amount"] if l["frmtrm_amount"] != "-" and l[
                                        "frmtrm_amount"] != "" else "0"
                                if "thstrm_dt" in l.keys():
                                    retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][yyyy_report_nm][
                                        trm_nm["thstrm_nm"]] = l["thstrm_amount"] if l["thstrm_amount"] != "-" and l[
                                        "frmtrm_amount"] != "" else "0"
                                    if "frmtrm_dt" in l.keys():
                                        if retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][yyyy_report_nm][
                                            trm_nm["frmtrm_nm"]] != "0" and l["thstrm_amount"] != "-" and l[
                                            "thstrm_amount"] != "":
                                            retDict[l["fs_nm"]][l["sj_nm"]][l["account_nm"]]["당기"][
                                                "{} Rate".format(yyyy_report_nm)]["당기"] = round((float(
                                                l["thstrm_amount"].replace(",", "")) - float(
                                                l["frmtrm_amount"].replace(",", ""))) / float(
                                                l["frmtrm_amount"].replace(",", "")) * 100, 2) if l[
                                                                                                      "thstrm_amount"] != "" and \
                                                                                                  l[
                                                                                                      "frmtrm_amount"] != "" else 0
            except Exception as e:
                print(e)
                print(l)
        # print(retDict)
        return retDict


if __name__ == "__main__":
    dart = Pipe()
    dart.create()
    date = '20210217'
    # dart.get_shared_reporting(date)
    # dart.get_majorshareholder_reporting(date)
    dart.get_majorevent_reporting(date)
    dart.get_freecapital_increasing_corp_info(date)
    # dart.get_krx_reporting(date)
    # dart.get_provisional_performance_reporting_corp_info(date)

    # fromdate = "20190101"
    # todate = "20190228"
    # dart.get_krx_reporting(fromdate, todate)
    # dart.get_provisional_performance_reporting_corp_info(fromdate, todate)

    # ret, code = dart.get_corp_code('005930')
    # # ret, code = dart.get_corp_code('299030')
    # # print(ret, code)
    # # print(dart.get_list(corp_code=code, bgn_de='20180101', pblntf_ty='A'))
    # lists = dart.get_list(corp_code=code, bgn_de='20180101', pblntf_ty='A')["list"][:1]
    # # for l in lists:
    # #     print(l)
    # # print(lists)
    # req_list, req_list2 = dart.get_req_lists(lists)
    # # print(dart.get_fnlttSinglAcnt_from_req_list(code, req_list))
    # print("*" * 150)
    # print(dart.get_fnlttSinglAcnt_from_req_list(code, req_list, "ALL"))
    # # print(dart.get_fnlttSinglAcntAll(code, "2020", "11014"))
