import re
from datetime import datetime
import logging
import traceback




class Partition_date(object):

    def __init__(self, source_file,dest_file_path,dest_ctrlfile_f):
        self.source_file = source_file
        self.dest_file_path = dest_file_path
        self.dest_ctrlfile_f=dest_ctrlfile_f

    def nsa(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        rop_f = ''
        fsi_f = ''
        circular_f = ''
        match_rop = re.search('rop', self.source_file)
        match_fsi = re.search('fsi', self.source_file)
        match_circular = re.search('circular', self.source_file)
        if match_rop:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_rop_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nsa_rop_e'
            rop_f = 'Y'
        if match_fsi:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_fsi_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nsa_fsi_e'
            fsi_f = 'Y'
        if match_circular:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_circular_bill_pay_summary_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nsa_circular_bill_pay_summary_e'
            circular_f = 'Y'
        try:
            if rop_f == '' and fsi_f == '' and circular_f == '':
                print_msg = "NSA filename does not have rop or fsi or circular in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "NSA filename does not have rop or fsi or circular in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def nielsen(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        non_local_f = ''
        non_national_f = ''
        local_f = ''
        national_f = ''
        match_non_local = re.search('Nielsen_Comp_Non_TV_Local', self.source_file)
        match_non_national = re.search('Nielsen_Comp_Non_TV_National', self.source_file)
        match_local = re.search('Nielsen_Comp_TV_Local', self.source_file)
        match_national = re.search('Nielsen_Comp_TV_National', self.source_file)
        if match_non_local:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_non_tv_local_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nielsen_comp_non_tv_local_e'
            non_local_f = 'Y'
        if match_non_national:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_non_tv_natl_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nielsen_comp_non_tv_natl_e'
            non_national_f = 'Y'
        if match_local:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_tv_local_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nielsen_comp_tv_local_e'
            local_f = 'Y'
        if match_national:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_tv_natl_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'nielsen_comp_tv_natl_e'
            national_f = 'Y'
        try:
            if non_local_f == '' and non_national_f == '' and local_f == '' and national_f == '':
                print_msg = "nielson filename does not have non local or non national or local or national in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "nielson filename does not have non local or non national or local or national in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def nsa_stage(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        rop_f = ''
        fsi_f = ''
        circular_f = ''
        match_rop = re.search('rop', self.source_file)
        match_fsi = re.search('fsi', self.source_file)
        match_circular = re.search('circular', self.source_file)
        if match_rop:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_rop_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nsa_rop_e'
            rop_f = 'Y'
        if match_fsi:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_fsi_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nsa_fsi_e'
            fsi_f = 'Y'
        if match_circular:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_circular_bill_pay_summary_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nsa_circular_bill_pay_summary_e'
            circular_f = 'Y'
        try:
            if rop_f == '' and fsi_f == '' and circular_f == '':
                print_msg="NSA filename does not have rop or fsi or circular in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "NSA filename does not have rop or fsi or circular in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name


    def nielsen_stage(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        non_local_f = ''
        non_national_f = ''
        local_f = ''
        national_f = ''
        match_non_local = re.search('Nielsen_Comp_Non_TV_Local', self.source_file)
        match_non_national = re.search('Nielsen_Comp_Non_TV_National', self.source_file)
        match_local = re.search('Nielsen_Comp_TV_Local', self.source_file)
        match_national = re.search('Nielsen_Comp_TV_National', self.source_file)
        if match_non_local:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_non_tv_local_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nielsen_comp_non_tv_local_e'
            non_local_f = 'Y'
        if match_non_national:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_non_tv_natl_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nielsen_comp_non_tv_natl_e'
            non_national_f = 'Y'
        if match_local:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_tv_local_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nielsen_comp_tv_local_e'
            local_f = 'Y'
        if match_national:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_tv_natl_e' + '/' + partition_date
            schema_name = 'stg_mdf_lzn'
            landing_table_name = 'nielsen_comp_tv_natl_e'
            national_f = 'Y'
        try:
            if non_local_f == '' and non_national_f == '' and local_f == '' and national_f == '':
                print_msg="nielson filename does not have non local or non national or local or national in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "nielson filename does not have non local or non national or local or national in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def groupm_stage(self):
        schema_name = ''
        landing_table_name = ''
        digital_f = ''
        printpak_f = ''
        spokpak_f = ''
        match_digital = re.search('incampaign_mmo', self.source_file)
        match_printpak = re.search('printpak', self.source_file)
        match_spotpak = re.search('SpotPak', self.source_file)
        if match_digital:
            date_match_digital = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_digital:
                date = date_match_digital.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'groupm_digitalmmo_e' + '/' + partition_date
                schema_name = 'stg_mdf_lzn'
                landing_table_name = 'groupm_digitalmmo_e'
                digital_f = 'Y'
        if match_printpak:
            date_match_printpak = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_printpak:
                date = date_match_printpak.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'groupm_printpak_e' + '/' + partition_date
                schema_name = 'stg_mdf_lzn'
                landing_table_name = 'groupm_printpak_e'
                printpak_f = 'Y'
        if match_spotpak:
            date_match_spotpak = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_spotpak:
                date = date_match_spotpak.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'groupm_spotpak_e' + '/' + partition_date
                schema_name = 'stg_mdf_lzn'
                landing_table_name = 'groupm_spotpak_e'
                spokpak_f = 'Y'
        try:
            if digital_f == '' and printpak_f == '' and spokpak_f == '':
                print_msg="groupm filename does not have incampaign_mmo or printpak or spotpak in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "groupm filename does not have incampaign_mmo or printpak or spotpak in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def nsa_bigred(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        rop_f = ''
        fsi_f = ''
        circular_f = ''
        match_rop = re.search('rop', self.source_file)
        match_fsi = re.search('fsi', self.source_file)
        match_circular = re.search('circular', self.source_file)
        if match_rop:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_rop_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nsa_rop_e'
            rop_f = 'Y'
        if match_fsi:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_fsi_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nsa_fsi_e'
            fsi_f = 'Y'
        if match_circular:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nsa_circular_bill_pay_summary_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nsa_circular_bill_pay_summary_e'
            circular_f = 'Y'
        try:
            if rop_f == '' and fsi_f == '' and circular_f == '':
                print_msg = "NSA filename does not have rop or fsi or circular in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "NSA filename does not have rop or fsi or circular in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def kantar_bigred(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        kantar_f = ''
        match_kantar = re.search('Kantar', self.source_file)
        if match_kantar:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + '/kantar_brand_health_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'kantar_brand_health_e'
            kantar_f = 'Y'
        try:
            if kantar_f == '':
                print_msg = "Kantar filename does not have Kantar in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "Kantar filename does not have Kantar in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def nielsen_bigred(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        non_local_f = ''
        non_national_f = ''
        local_f = ''
        national_f = ''
        match_non_local = re.search('Nielsen_Comp_Non_TV_Local', self.source_file)
        match_non_national = re.search('Nielsen_Comp_Non_TV_National|Nielsen_Comp_Non_TV_Natl', self.source_file)
        match_local = re.search('Nielsen_Comp_TV_Local', self.source_file)
        match_national = re.search('Nielsen_Comp_TV_National|Nielsen_Comp_TV_Natl', self.source_file)
        if match_non_local:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_non_tv_local_e_tab' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nielsen_comp_non_tv_local_e'
            non_local_f = 'Y'
        if match_non_national:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_non_tv_natl_e_tab' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nielsen_comp_non_tv_natl_e'
            non_national_f = 'Y'
        if match_local:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_tv_local_e_tab' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nielsen_comp_tv_local_e'
            local_f = 'Y'
        if match_national:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'nielsen_comp_tv_natl_e_tab' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'nielsen_comp_tv_natl_e'
            national_f = 'Y'
        try:
            if non_local_f == '' and non_national_f == '' and local_f == '' and national_f == '':
                print_msg = "nielson filename does not have non local or non national or local or national in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "nielson filename does not have non local or non national or local or national in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def groupm_bigred(self):
        schema_name = ''
        landing_table_name = ''
        digital_f = ''
        printpak_f = ''
        spokpak_f = ''
        match_digital = re.search('incampaign_mmo', self.source_file)
        match_printpak = re.search('printpak', self.source_file)
        match_spotpak = re.search('SpotPak', self.source_file)
        if match_digital:
            date_match_digital = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_digital:
                date = date_match_digital.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'groupm_digitalmmo_e' + '/' + partition_date
                schema_name = 'prd_mdf_lnd'
                landing_table_name = 'groupm_digitalmmo_e'
                digital_f = 'Y'
        if match_printpak:
            date_match_printpak = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_printpak:
                date = date_match_printpak.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'groupm_printpak_e' + '/' + partition_date
                schema_name = 'prd_mdf_lnd'
                landing_table_name = 'groupm_printpak_e'
                printpak_f = 'Y'
        if match_spotpak:
            date_match_spotpak = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_spotpak:
                date = date_match_spotpak.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'groupm_spotpak_e' + '/' + partition_date
                schema_name = 'prd_mdf_lnd'
                landing_table_name = 'groupm_spotpak_e'
                spokpak_f = 'Y'
        try:
            if digital_f == '' and printpak_f == '' and spokpak_f == '':
                print_msg="groupm filename does not have incampaign_mmo or printpak or spotpak in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "groupm filename does not have incampaign_mmo or printpak or spotpak in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name


    def btch_exec_stat(self):

        import datetime
        now = datetime.datetime.now()
        partition_date = 'load_date=' + now.strftime("%Y-%m-%d")
        dest_file_path = self.dest_file_path + '/' + partition_date
        schema_name = 'prd_mdf_lzn'
        landing_table_name = 'btch_exec_stat_ingestion'

        return dest_file_path,schema_name,landing_table_name

    def btch_exec_stat_stage(self):

        import datetime
        now = datetime.datetime.now()
        partition_date = 'load_date=' + now.strftime("%Y-%m-%d")
        dest_file_path = self.dest_file_path + '/' + partition_date
        schema_name = 'stg_mdf_lzn'
        landing_table_name = 'btch_exec_stat_ingestion'

        return dest_file_path,schema_name,landing_table_name

    def btch_exec_stat_bigred(self):

        import datetime
        now = datetime.datetime.now()
        partition_date = 'load_date=' + now.strftime("%Y-%m-%d")
        dest_file_path = self.dest_file_path + '/' + partition_date
        schema_name = 'prd_mdf_lnd'
        landing_table_name = 'btch_exec_stat_ingestion'

        return dest_file_path,schema_name,landing_table_name


    def method_call(self, method_name):

        logging.info("Executing partition_date.Partition_date.%s" % (method_name))
        return getattr(self, method_name)()

    def criteo_bigred(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        non_local_f = ''
        non_national_f = ''
        local_f = ''
        national_f = ''
        match_criteo_category = re.search('criteo_category_advertiser', self.source_file)
        match_non_criteo_category_daily = re.search('criteo_category_daily_snapshot', self.source_file)
        match_criteo_category_monthly = re.search('criteo_category_monthly_snapshot', self.source_file)
        match_criteo_advertiser_category_dly = re.search('criteo_advertiser_category_dly', self.source_file)
        match_criteo_category_dly = re.search('criteo_category_dly', self.source_file)
        match_criteo_floor_price = re.search('taxonomy_floors', self.source_file)
        if match_criteo_category:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'criteo_category_advertiser_daily_snapshot_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'criteo_category_advertiser_daily_snapshot_e'
            non_local_f = 'Y'
        if match_non_criteo_category_daily:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'criteo_category_daily_snapshot_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'criteo_category_daily_snapshot_e'
            non_national_f = 'Y'
        if match_criteo_category_monthly:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'criteo_category_monthly_snapshot_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'criteo_category_monthly_snapshot_e'
            local_f = 'Y'
        if match_criteo_floor_price:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'criteo_floor_prices_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'criteo_floor_prices_e'
            local_f = 'Y'
        if match_criteo_advertiser_category_dly:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'criteo_category_advertiser_dly_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'criteo_category_advertiser_dly_e'
            local_f = 'Y'          
        if match_criteo_category_dly:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'criteo_category_dly_e' + '/' + partition_date
            schema_name = 'prd_mdf_lnd'
            landing_table_name = 'criteo_category_dly_e'
            local_f = 'Y'
        try:
            if match_criteo_category == '' and match_non_criteo_category_daily == '' and match_criteo_category_monthly == '' and match_criteo_advertiser_category_dly == '' and match_criteo_category_dly == '' and match_criteo_floor_price == '':
                print_msg = "criteo filename does not have category_advertiser or criteo_category_daily_snapshot or criteo_category_monthly_snapshot or match_criteo_advertiser_category_dly or match_criteo_category_dly or taxonomy_floors in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "criteo filename does not have category_advertiser or criteo_category_daily_snapshot or criteo_category_monthly_snapshot or match_criteo_advertiser_category_dly or match_criteo_category_dly or taxonomy_floors in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name


    def twitter_sftp_bigred3(self):
        import datetime
        schema_name = ''
        landing_table_name = ''
        match_twitter_category = re.search('Twitter_mmo', self.source_file)
        if match_twitter_category:
            now = datetime.datetime.now()
            partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
            dest_file_path = self.dest_file_path + '/' + 'twitter_sftp_e' + '/' + partition_date
            schema_name = 'prd_mdf_lzn'
            landing_table_name = 'twitter_sftp_e'
        try:
            if match_twitter_category == '':
                print_msg = "twitter filename does not have Twitter_mmo in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "twitter filename does not have Twitter_mmo in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def btch_exec_stat_bigred3(self):

        import datetime
        now = datetime.datetime.now()
        partition_date = 'load_date=' + now.strftime("%Y-%m-%d")
        dest_file_path = self.dest_file_path + '/' + partition_date
        schema_name = 'prd_mdf_lzn'
        landing_table_name = 'btch_exec_stat_ingestion'

        return dest_file_path,schema_name,landing_table_name

    def snapchat_sftp_bigred3(self):
        ##import datetime
        schema_name = ''
        landing_table_name = ''
        logging.info("inside  snapchat_sftp_bigred3 function")
        match_snapchat_category = re.search('Snapchat_mmo', self.source_file)
        if match_snapchat_category:
            date_match_snapchat = re.search('(\d{4}\d{2}\d{2})', self.source_file)
            if date_match_snapchat:
                date = date_match_snapchat.group(1)
                partition_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                partition_date = 'querydate' + '=' + partition_date
                dest_file_path = self.dest_file_path + '/' + 'snapchat_sftp_e' + '/' + partition_date
                schema_name = 'prd_mdf_lzn'
                landing_table_name = 'snapchat_sftp_e'
                logging.info("dest_file_path is " + dest_file_path)
            else:
                logging.info("snapchat filename does not date in the format YYYYMMDD")
                now = datetime.now()
                partition_date = 'querydate=' + now.strftime("%Y-%m-%d")
                dest_file_path = self.dest_file_path + '/' + 'snapchat_sftp_e' + '/' + partition_date
                schema_name = 'prd_mdf_lzn'
                landing_table_name = 'snapchat_sftp_e'
        try:
            if match_snapchat_category == '':
                print_msg = "snapchat filename does not have Snapchat_mmo in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "snapchat filename does not have Snapchat_mmo in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

    def criteo_bigred3(self):
        schema_name = ''
        landing_table_name = ''
        match_criteo_advertiser_category_dly = re.search('criteo_advertiser_category_dly', self.source_file)
        match_criteo_category_dly = re.search('criteo_category_dly', self.source_file)
        if match_criteo_advertiser_category_dly:
            dest_file_path = self.dest_file_path + '/' + 'criteo_advertiser_category_daily_land'
            local_f = 'Y'
        if match_criteo_category_dly:
            dest_file_path = self.dest_file_path + '/' + 'criteo_category_daily_land'
        try:
            if match_criteo_advertiser_category_dly == '' and match_criteo_category_dly == '':
                print_msg = "criteo filename does not have match_criteo_advertiser_category_dly or match_criteo_category_dly in the filename %s" % self.source_file
                raise Exception(print_msg)
        except Exception:
            logging.error(
                "criteo filename does not have match_criteo_advertiser_category_dly or match_criteo_category_dly in the filename %s" % self.source_file)
            logging.error(traceback.format_exc())

        return dest_file_path, schema_name, landing_table_name

#partition_date = Partition_date("circular_bill_pay_summary_2020-03-03")

#output=partition_date.method_call('nsa')
#print(output)
