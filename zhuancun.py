import sys

import requests
import time
import logging
from typing import List, Dict, Optional
from requests.exceptions import RequestException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AliPCS:
    def __init__(self, access_token: str, share_token: str, drive_id: str):
        self.base_url = "https://api.aliyundrive.com"
        self.access_token = access_token
        self.drive_id = drive_id
        self.share_token = share_token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "X-Canary": "client=web,app=adrive,version=v6.4.2",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        self.request_interval = 1  # 初始请求间隔为1秒

    def make_request(self, method, url, headers=None, json=None, max_retries=5):
        if headers is None:
            headers = self.headers

        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=headers, json=json)
                response.raise_for_status()
                self.request_interval = max(1, self.request_interval - 0.1)  # 成功后稍微减少间隔
                return response.json()
            except RequestException as e:
                if response.status_code == 429:
                    logger.warning(f"请求频率过高，等待后重试。尝试次数：{attempt + 1}")
                    self.request_interval *= 2  # 遇到429错误时增加等待时间
                    time.sleep(self.request_interval)
                else:
                    logger.error(f"请求失败：{e}")
                    if attempt == max_retries - 1:
                        raise

    def get_share_info(self, share_id: str) -> Dict:
        url = f"{self.base_url}/adrive/v3/share_link/get_share_by_anonymous"
        headers = self.headers.copy()
        headers["X-Share-Token"] = self.share_token
        data = {"share_id": share_id}
        response = self.make_request("POST", url, headers=headers, json=data)
        return response

    def list_files(self, share_id: str, parent_file_id: str, limit: int = 20) -> List[Dict]:
        url = f"{self.base_url}/adrive/v2/file/list_by_share"
        headers = self.headers.copy()
        headers["X-Share-Token"] = self.share_token
        data = {
            "share_id": share_id,
            "parent_file_id": parent_file_id,
            "limit": limit,
            "order_by": "name",
            "order_direction": "DESC"
        }
        response = self.make_request("POST", url, headers=headers, json=data)
        return response.get('items', [])

    def create_folder(self, parent_file_id: str, folder_name: str) -> Dict:
        url = f"{self.base_url}/adrive/v2/file/createWithFolders"
        data = {
            "drive_id": self.drive_id,
            "parent_file_id": parent_file_id,
            "name": folder_name,
            "check_name_mode": "refuse",
            "type": "folder"
        }
        response = self.make_request("POST", url, headers=self.headers, json=data)
        print(response)
        return response

    def copy_file(self, share_id: str, file_id: str, to_parent_file_id: str) -> Dict:
        url = f"{self.base_url}/adrive/v4/batch"
        data = {
            "requests": [{
                "body": {
                    "file_id": file_id,
                    "share_id": share_id,
                    "auto_rename": True,
                    "to_parent_file_id": to_parent_file_id,
                    "to_drive_id": self.drive_id
                },
                "headers": {"Content-Type": "application/json"},
                "id": "0",
                "method": "POST",
                "url": "/file/copy"
            }],
            "resource": "file"
        }
        response = self.make_request("POST", url, headers=self.headers, json=data)
        print("保存文件", response)
        return response['responses'][0]['body']

    def batch_copy_files(self, share_id: str, file_list: List[Dict], to_parent_file_id: str) -> List[Dict]:
        url = f"{self.base_url}/adrive/v4/batch"
        headers = self.headers.copy()
        headers["X-Share-Token"] = self.share_token

        batch_size = 500
        results = []

        for i in range(0, len(file_list), batch_size):
            batch = file_list[i:i+batch_size]
            requests_data = []
            for index, file in enumerate(batch):
                requests_data.append({
                    "body": {
                        "file_id": file["file_id"],
                        "share_id": share_id,
                        "auto_rename": True,
                        "to_parent_file_id": to_parent_file_id,
                        "to_drive_id": self.drive_id
                    },
                    "headers": {"Content-Type": "application/json"},
                    "id": str(index),
                    "method": "POST",
                    "url": "/file/copy"
                })

            data = {
                "requests": requests_data,
                "resource": "file"
            }
            response = self.make_request("POST", url, headers=headers, json=data)
            logger.info(f"批量保存文件响应: {response}")
            results.extend(response['responses'])

        return results

def save_shared_folder(api: AliPCS, share_id: str, source_folder_id: str, target_folder_id: str):
    files = api.list_files(share_id, source_folder_id)
    file_list = []
    folder_list = []

    for file in files:
        if file["type"] == "folder":
            folder_list.append(file)
        else:
            file_list.append(file)

    if file_list:
        try:
            results = api.batch_copy_files(share_id, file_list, target_folder_id)
            for result in results:
                if result['status'] == 201:
                    logger.info(f"复制文件成功: {result['body']['file_id']}")
                else:
                    logger.error(f"复制文件失败: {result['body']}")
        except Exception as e:
            logger.error(f"批量复制文件时出错: {str(e)}")

    for folder in folder_list:
        try:
            new_folder = api.create_folder(target_folder_id, folder["name"])
            logger.info(f"创建文件夹: {folder['name']} -> {new_folder['file_id']}")
            save_shared_folder(api, share_id, folder["file_id"], new_folder["file_id"])
        except Exception as e:
            logger.error(f"处理文件夹 {folder['name']} 时出错: {str(e)}")
        time.sleep(api.request_interval)  # 在处理每个文件夹之后添加延迟

def main():
    # 要转存的链接
    share_link = "https://www.alipan.com/s/*******"
    share_token = ""
    access_token = ""
    drive_id = ""
    try:
        api = AliPCS(access_token, share_token, drive_id)
        share_id = share_link.split('/')[-1]

        # 获取分享信息
        share_info = api.get_share_info(share_id)
        root_folder_id = share_info['file_infos'][0]['file_id']

        logger.info(f"分享名称: {share_info['share_name']}")
        logger.info(f"文件数量: {share_info['file_count']}")

        # 创建目标文件夹
        target_folder_name = "保存的分享文件"
        target_folder = api.create_folder("root", target_folder_name)
        target_folder_id = target_folder["file_id"]

        # 开始保存分享的文件夹结构
        save_shared_folder(api, share_id, root_folder_id, target_folder_id)

        logger.info("所有文件和文件夹已成功转存。")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
