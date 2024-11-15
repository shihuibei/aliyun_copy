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

    def make_request(self, method, url, headers=None, json=None, max_retries=2):
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

        all_files = []
        next_marker = None

        while True:
            data = {
                "share_id": share_id,
                "parent_file_id": parent_file_id,
                "limit": limit,
                "order_by": "name",
                "order_direction": "DESC"
            }
            if next_marker:
                data["marker"] = next_marker

            response = self.make_request("POST", url, headers=headers, json=data)
            all_files.extend(response.get('items', []))

            next_marker = response.get('next_marker')
            if not next_marker:
                break  # 如果没有更多的文件，跳出循环

        return all_files

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

    def batch_copy_files(self, share_id: str, file_list: List[Dict], to_parent_file_id: str) -> Dict:
        """批量复制文件（不包含文件夹）"""
        url = f"{self.base_url}/adrive/v4/batch"
        headers = self.headers.copy()
        headers["X-Share-Token"] = self.share_token

        requests_data = []
        for index, file in enumerate(file_list):
            file_id = file["file_id"] if isinstance(file, dict) else file
            requests_data.append({
                "body": {
                    "file_id": file_id,
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
        response = self.make_request("POST", url, headers=headers, json=data, max_retries=1)
        return response

    def batch_copy_folder(self, share_id: str, folder_id: str, to_parent_file_id: str) -> Dict:
        """批量复制文件夹"""
        url = f"{self.base_url}/adrive/v4/batch"
        headers = self.headers.copy()
        headers["X-Share-Token"] = self.share_token

        requests_data = [{
            "body": {
                "file_id": folder_id,
                "share_id": share_id,
                "auto_rename": True,
                "to_parent_file_id": to_parent_file_id,
                "to_drive_id": self.drive_id
            },
            "headers": {"Content-Type": "application/json"},
            "id": "0",
            "method": "POST",
            "url": "/file/copy"
        }]

        data = {
            "requests": requests_data,
            "resource": "file"
        }
        response = self.make_request("POST", url, headers=headers, json=data,max_retries=1)
        return response

    def check_async_task(self, task_id: str) -> Dict:
        """检查异步任务状态"""
        url = f"{self.base_url}/adrive/v4/batch"
        headers = self.headers.copy()
        
        data = {
            "requests": [{
                "body": {
                    "async_task_id": task_id
                },
                "headers": {"Content-Type": "application/json"},
                "id": task_id,
                "method": "POST",
                "url": "/async_task/get"
            }],
            "resource": "file"
        }
        
        response = self.make_request("POST", url, headers=headers, json=data, max_retries=1)
        return response

def save_shared_folder(api: AliPCS, share_id: str, source_folder_id: str, target_folder_id: str):
    """保存分享的文件夹"""
    # 首先尝试直接转存整个目录
    try:
        # 调用文件夹批量转存
        result = api.batch_copy_folder(share_id, source_folder_id, target_folder_id)
        
        # 检查返回结果
        if result.get('responses') and result['responses'][0]['status'] == 202:
            # 获取异步任务ID
            task_id = result['responses'][0]['body']['async_task_id']
            
            # 轮询检查任务状态
            while True:
                logger.info(f"{task_id}查询是否成功转存整个目录...")
                task_result = api.check_async_task(task_id)

                if task_result.get('responses'):
                    task_status = task_result['responses'][0]['body']
                    if task_status['state'] == 'Succeed':
                        logger.info(f"成功转存整个目录，共处理 {task_status['total_process']} 个文件")
                        return True
                    elif task_status['state'] in ['Failed', 'Cancelled']:
                        logger.error(f"转存失败: {task_status}")
                        break
                time.sleep(1)  # 等待1秒后再次检查
        
        # 处理错误情况
        elif result.get('code'):
            logger.error(f"批量转存失败: {result.get('message')} ({result.get('code')})")
            if result.get('code') == 'MaxSaveFileCountExceed':
                logger.info("文件数量超出限制，将尝试逐个转存")
            
    except Exception as e:
        logger.error(f"直接转存目录时发生异常: {str(e)}")

    # 如果直接转存失败，执行原有的逐个转存逻辑
    logger.info("开始执行逐个转存...")
    files = api.list_files(share_id, source_folder_id)
    if not files:
        logger.info("目录为空或无法获取文件列表")
        return True

    file_list = []
    folder_list = []

    # 分离文件和文件夹
    for file in files:
        if file["type"] == "folder":
            folder_list.append(file)
        else:
            file_list.append(file)

    # 批量处理文件
    if file_list:
        try:
            # 每500个文件一批进行处理
            batch_size = 500
            for i in range(0, len(file_list), batch_size):
                batch = file_list[i:i+batch_size]
                results = api.batch_copy_files(share_id, batch, target_folder_id)
                for result in results:
                    if result['status'] == 201:
                        logger.info(f"复制文件成功: {result['body']['file_id']}")
                    else:
                        logger.error(f"复制文件失败: {result['body']}")
                time.sleep(api.request_interval)  # 添加延迟避免频率限制
        except Exception as e:
            logger.error(f"批量复制文件时出错: {str(e)}")

    # 处理文件夹
    for folder in folder_list:
        try:
            new_folder = api.create_folder(target_folder_id, folder["name"])
            logger.info(f"创建文件夹: {folder['name']} -> {new_folder['file_id']}")
            save_shared_folder(api, share_id, folder["file_id"], new_folder["file_id"])
        except Exception as e:
            logger.error(f"处理文件夹 {folder['name']} 时出错: {str(e)}")
        time.sleep(api.request_interval)

    return True

def extract_ids_from_link(share_link):
    parts = share_link.split('/')
    share_id = parts[4]  # 提取 share_id
    folder_id = parts[-1] if len(parts) > 5 else None  # 提取 folder_id（如果存在）
    return share_id, folder_id

def main():
    share_link = "https://www.aliyundrive.com/s/hocV43RFQay"
    # share_link = "https://www.aliyundrive.com/s/hocV43RFQay/folder/61517c215fa6150d644e4a2b8fd2122ea876ac46"
    share_token = "请填写"
    access_token = "请填写"
    drive_id = "请填写"

    try:
        api = AliPCS(access_token, share_token, drive_id)
        # 提取 share_id 和 folder_id
        share_id, folder_id = extract_ids_from_link(share_link)

        # 获取分享信息
        share_info = api.get_share_info(share_id)
        if folder_id:
            root_folder_id = folder_id
        else:
            root_folder_id = share_info['file_infos'][0]['file_id']
        logger.info(f"分享名称: {share_info['share_name']}")
        logger.info(f"文件数量: {share_info['file_count']}")

        # 创建目标文件夹
        target_folder_name = share_info['share_name']
        target_folder = api.create_folder("root", target_folder_name)
        target_folder_id = target_folder["file_id"]

        # 开始保存分享的文件夹结构
        save_shared_folder(api, share_id, root_folder_id, target_folder_id)

        logger.info("所有文件和文件夹已成功转存。")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
