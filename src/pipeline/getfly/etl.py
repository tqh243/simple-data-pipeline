from concurrent.futures import process
import os
import json
import argparse
from pkgutil import get_data
from urllib import response

import requests
import yaml
 
from src.utils.rbms.postgresdb import PostgresDB
   
class GetFlyETL:
 
    base_folder = '/src/src/pipeline/getfly'
    api_version = 'v3'
    base_getfly = '/src/src/schemas/getfly'
   
    def __init__(self, args):
        self.job_name = args.job_name
        self.job_config = self.get_job_config

    def get_credentials(self):
        file_credentials = f'{self.base_folder}/credentials.json'
        with open(file_credentials, 'r') as openfile:
            # Reading from json file
            json_object = json.load(openfile)
            return json_object
            
            
    def get_configs(self):
        file_config = f'{self.base_folder}/config.yml'
         # Reading from yml file
        with open(file_config, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
   
    def get_job_config(self):
        configs = self.get_configs()
        job_config = configs.get(self.job_name)
        credential_key = job_config.get('credential_key')
        credentials = self.get_credentials()
        job_credential = credentials.get(credential_key)
        job_config['api_key'] = job_credential.get('api_key')
        return job_config
 
    def get_data(self, job_config: dict, page: int=1):
        base_endpoint = job_config.get('base_endpoint')
        service_name = job_config.get('service_name')
        api_key = job_config.get('api_key')
       
        url = f"https://{base_endpoint}/api/{self.api_version}/{service_name}/?page={page}"

        headers = {
            'X-API-KEY': api_key
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)

    def get_accounts(self):
        file_accounts = f'{self.base_getfly}/accounts.json'
        with open(file_accounts, 'r') as openfile:
            json_object = json.load(openfile)
            return json_object
 
    def get_campaigns(self):
        file_accounts = f'{self.base_getfly}/campaigns.json'
        with open(file_accounts, 'r') as openfile:
            json_object = json.load(openfile)
            return json_object
 
    def get_products(self):
        file_accounts = f'{self.base_getfly}/products.json'
        with open(file_accounts, 'r') as openfile:
            json_object = json.load(openfile)
            return json_object
 
    def parse_account(self, data: dict):
        account_info = {}
        account_info['account_id'] = data.get('account_id')
        account_info['account_code'] = data.get('account_code')
        account_info['account_name'] = data.get('account_name')
        account_info['address'] = data.get('address')
        account_info['phone'] = data.get('phone')
        account_info['email'] = data.get('email')
        account_info['manager_email'] = data.get('manager_email')
        account_info['manager_user_name'] = data.get('manager_user_name')
        account_info['website'] = data.get('website')
        account_info['logo'] = data.get('logo')
        account_info['birthday'] = data.get('birthday')
        account_info['sic_code'] = data.get('sic_code')
        account_info['description'] = data.get('description')
        account_info['created_at'] = data.get('created_at')
        account_info['account_type_id'] = data.get('account_type_id')
        account_info['account_type"'] = data.get('account_type"')
        account_info['account_source_id'] = data.get('account_source_id')
        account_info['account_source'] = data.get('account_source')
        account_info['relation_id'] = data.get('relation_id')
        account_info['relation_name'] = data.get('relation_name')
        account_info['gender'] = data.get('gender')
        account_info['revenue'] = data.get('revenue')
        account_info['mssv'] = data.get('mssv')
        account_info['tinh_trang_goi_dien'] = data.get('tinh_trang_goi_dien')
        account_info['nganh_dang_ky'] = data.get('nganh_dang_ky')
        account_info['ngay_chinh_sua'] = data.get('ngay_chinh_sua')
        account_info['truong_dang_ky'] = data.get('truong_dang_ky')
        account_info['facebook'] = data.get('facebook')
        account_info['lop'] = data.get('lop')
        account_info['test_loep'] = data.get('test_loep')
        account_info['ho_so'] = data.get('ho_so')
        account_info['dong_hoc_phi'] = data.get('dong_hoc_phi')
        account_info['so_dien_thoai_phu_huynh'] = data.get('so_dien_thoai_phu_huynh')
        account_info['ten_phu_huynh'] = data.get('ten_phu_huynh')
        account_info['biet_broward_chua'] = data.get('biet_broward_chua')
        account_info['contacts'] = list(map(lambda x: json.dumps(x, indent=1, ensure_ascii=False), data.get('contacts')))
        return account_info
   
    def parse_product(self, data: dict):
        product_info = {}
        product_info['product_id'] = data.get('product_id')
        product_info['category_id'] = data.get('category_id')
        product_info['origin_id'] = data.get('origin_id')
        product_info['unit_id'] = data.get('unit_id')
        product_info['manufacturer_id'] = data.get('manufacturer_id')
        product_info['services'] = data.get('services')
        product_info['saleoff_price'] = data.get('saleoff_price')
        product_info['created_at'] = data.get('created_at')
        product_info['technique'] = data.get('technique')
        product_info['updated_at'] = data.get('updated_at')
        product_info['featured_image'] = data.get('featured_image')
        product_info['product_code'] = data.get('product_code')
        product_info['product_name'] = data.get('product_name')
        product_info['last_active'] = data.get('last_active')
        product_info['description'] = data.get('description')
        product_info['cover_price'] = data.get('cover_price')
        product_info['discount'] = data.get('discount')
        product_info['price_wholesale'] = data.get('price_wholesale')
        product_info['discount_wholesale'] = data.get('discount_wholesale')
        product_info['price_online'] = data.get('price_online')
        product_info['discount_online'] = data.get('discount_online')
        product_info['price_average_in'] = data.get('price_average_in')
        product_info['discount_in'] = data.get('discount_in')
        product_info['short_description'] = data.get('short_description')
        product_info['product_vat'] = data.get('product_vat')
        product_info['images'] = data.get('images')
        product_info['thumbnail_file'] = data.get('thumbnail_file')
        return  product_info
 
    def process_accounts_data(self, data: list):
        all_data = []
        for item in data:
            parsed_data = self.parse_account(item)
            all_data.append(parsed_data)
        return all_data

    def process_campaigns_data(self):
        job_config = self.get_job_config()
        response_data = self.get_data(job_config)
        return response_data
 
    def process_products_data(self, data: list):
        all_data = []
        for item in data:
            parsed_data = self.parse_product(item)
            all_data.append(parsed_data)
        return all_data
   
    def get_total_pages(self):  
        job_config = self.get_job_config()
        response_data = self.get_data(job_config)
        pagination = response_data.get('pagination')
        total_page = pagination.get('total_page')
        return total_page
       
    def get_records_data(self, page: int=1):
        job_config = self.get_job_config()
        service_name = job_config.get('service_name')
        response_data = self.get_data(job_config,page)
        records = response_data.get('records')
 
        if service_name == 'accounts':
            data = self.process_accounts_data(records)  
        elif service_name == 'products':
            data = self.process_products_data(records)
        return data
   
    def get_postgres(self, data: list):
        job_config = self.get_job_config()
        name = job_config.get('table_name_postgres')
        service_name = job_config.get('service_name')
        if service_name == 'accounts':
            schema = self.get_accounts()
        elif service_name == 'campaigns':
            schema = self.get_campaigns()
        elif service_name == 'products':
            schema = self.get_products()
 
        postgres_db = PostgresDB()  
        postgres_db.sync_data_to_postgres (
        table_name = name,
        table_schema = schema,
        data = data
        )
 
    def execute(self):
        total_pages = self.get_total_pages() + 1
        job_config = self.get_job_config()
        service_name = job_config.get('service_name')
        print(job_config)
        data = []
        for item in range(1,total_pages):
            data.extend(self.get_records_data(item))
        if service_name == 'accounts' or service_name == 'products' :
             self.get_postgres(data)
        elif service_name == 'campaigns':
            self.get_postgres(self.process_campaigns_data())
 
if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-j', '--job_name', required=True)
    args = args_parser.parse_args()
    GetFlyETL(args).execute()
