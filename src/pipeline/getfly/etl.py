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
    base_page ='?page='

    def __init__(self, args):
        self.job_name = args.job_name

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

    def get_data(self, job_config: dict):
        base_endpoint = job_config.get('base_endpoint')
        service_name = job_config.get('service_name')
        api_key = job_config.get('api_key')
        
        url = f"https://{base_endpoint}/api/{self.api_version}/{service_name}/"
        headers = {
            'X-API-KEY': api_key
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)
            
    def parse_account(self, data: dict):
        account_info = {}
        account_info['account_id'] = data.get('account_id')
        account_info['account_code'] = data.get('account_code')
        account_info['account_name'] = data.get('account_name')
        return account_info #
    
    def process_accounts_data(self, data:list):
        all_data = []
        for item in data:
            parsed_data = self.parse_account(item)
            all_data.append(parsed_data)
        return all_data
    
    def total_pages(self):   
        #count total pages
        job_config = self.get_job_config()
        service_name = job_config.get('service_name')
        if service_name != 'campaigns':
            response_data = self.get_data(job_config)
            pagination = response_data.get('pagination')
            total_page = pagination.get('total_page')
            return total_page

    def get_all_data(self,job_config: dict):
        all_data = []
        total_pages = self.total_pages() + 1
        base_endpoint = job_config.get('base_endpoint')
        service_name = job_config.get('service_name')
        api_key = job_config.get('api_key')
        for item in range(0, total_pages) :   
            url = f"https://{base_endpoint}/api/{self.api_version}/{service_name}/{self.base_page}{item}" 
            headers = {
                'X-API-KEY': api_key
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                response_data = response.json()
                all_data.append(response_data)
            else:
                print(response.text)
        return(all_data)

    def process_campaigns_data(self, data: list):
        ...
    
    def open_w_json(self, data: list) :
        file_credentials = f'{self.base_folder}/test.json'
        with open(file_credentials,'w',encoding='utf-8') as f:
            file_json = json.dump(data,f,indent=4)
            return(file_json)  

    def process_account_product(self, data: list) :
        data_all = []
        for k in range(0,len(data)) :
            x = data[k]["records"]
            data_all.extend(x)
        return(data_all)

    def execute(self):
        job_config = self.get_job_config()
        print(job_config)
        service_name = job_config.get('service_name')
        if service_name == 'accounts' or service_name == 'products' :
            process = self.get_all_data(job_config)
            data = self.process_account_product(process)
            print(data)
        elif service_name == 'campaigns':
            campaigns = self.get_data(job_config)
            print(campaigns)
        # TODO:
        ## sync data vo dwh

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-j', '--job_name', required=True)
    args = args_parser.parse_args()

    GetFlyETL(args).execute()
   
