# -*- coding: utf-8 -*-
import OpenDartPipe.opendart_disclosure_info as di
import OpenDartPipe.opendart_company_info as ci
import OpenDartPipe.opendart_bizreport_info as bi
import OpenDartPipe.opendart_sharedstock_info as si
import watson.db_factory as db

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
        "주요사항보고서": "H006",
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

    def create(self):
        di.getConfig()
        self.api_key = di.getKey()
        if self.corp_codes == {}:
            self.corp_codes = di.get_corpcode_dict(self.api_key)

    #-- Opendart : 공시정보
    def get_corp_code(self, info):
        if self.corp_codes =={}:
            self.corp_codes = di.get_corpcode_dict(self.api_key)

        if self.corp_codes != {} and info.isdigit():
            if len(info) == 6:
                res = list(filter(lambda corp: corp['stock_code'] == info, self.corp_codes.values()))
            else:
                res = list(filter(lambda corp: corp['corp_code'] == info, self.corp_codes.values()))
            if len(res) > 0:
                return True, res[0]["corp_code"]
            else: return False, None
        elif self.corp_codes != {} and info.isdigit() is not True:
            res = list(filter(lambda corp: corp['corp_name'] == info, self.corp_codes.values()))
            if len(res) > 0:
                return True, res[0]["corp_code"]
            else:
                return False, None

    def get_list(self, corp_code=None, bgn_de=None, end_de=None, last_reprt_at="Y", pblntf_ty=None, pblntf_detail_ty=None, corp_cls=None):
        if corp_code:
            ret, code = self.get_corp_code(corp_code)
        else:
            ret = True
            code = None
        if ret:
            return di.get_list_json(self.api_key, code, bgn_de, end_de, last_reprt_at, pblntf_ty, pblntf_detail_ty, corp_cls)

    def get_company_info(self, corp_code):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return di.get_company_info_json(self.api_key, code)
    # -- Opendart : 공시정보 끝

    # -- Opendart : 상장기업 재무정보
    def get_fnlttSinglAcntAll(self, corp_code, bsns_year, reprt_code="11014", fs_div="CFS"):
        ret, code = self.get_corp_code(corp_code)
        if ret:
            return ci.get_fnlttSinglAcntAll_json(self.api_key, bsns_year, reprt_code, fs_div)

    def get_fnlttSinglAcnt(self, corp_code, bsns_year, reprt_code="11014", fs_div="CFS"):
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

if __name__ == "__main__":
    dart = Pipe()
    dart.create()
    # sec_code = '005930'
    # print(aa.get_company_info(sec_code))
    # print(aa.get_fnlttSinglAcnt(sec_code, "2020", aa.reprt_ty_codes["3분기보고서"]))
    # db.ResultListDataStore
    date = "20201214"
    db.ResultListDataStore(dart.get_list(bgn_de=date, pblntf_ty='D'))


    corp_code_list = db.getMajorShareholderReportingInfo(date)
    for corp_code in corp_code_list:
        result = dart.get_majorstock(corp_code)
        db.ResultMajorShareholderDataStore(result)
