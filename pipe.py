# -*- coding: utf-8 -*-
import OpenDartPipe.opendart_disclosure_info as di
import OpenDartPipe.opendart_company_info as ci
import OpenDartPipe.opendart_bizreport_info as bi
import OpenDartPipe.opendart_sharedstock_info as si


class Pipe:
    api_key = ""
    corp_codes = {}

    def create(self, crtfc_key):
        self.api_key = crtfc_key
        self.corp_codes = di.get_corpcode_dict(self.api_key)


if __name__ == "__main__":
    key = '006291d72abf22ed214da7b1dd5fc5524319041f'
    aa = Pipe()
    aa.create(key)
    print(aa.corp_codes.values())