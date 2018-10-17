from selenium import webdriver
from selenium.common import exceptions
from lxml import etree
import pymongo
import socket
import time

__SLAVE_ID__ = int(time.time())              # 使用Unix时间戳作为Slave的ID

__HOST_ADDR__ = "127.0.0.1"                 # Master的IP地址
__HOST_PORT__ = 8888                         # Master监听的端口

__DB_ADDR__ = "127.0.0.1"                   # MongoDB的IP地址
__DB_PORT__ = 27017                          # MongoDB监听的端口

__XPATHS__ = {                               # XPath解析规则
    "title": "/html/head/meta[@property='og:title']/@content",
    "time": "/html/head/meta[@property='article:published_time']/@content",
    "author": "/html/head/meta[@property='article:author']/@content",
    "tags": "/html/head/meta[@name='tags']/@content",
    "url": "/html/head/meta[@property='og:url']/@content",
    "content": "//div[@id='artibody']/p/text()"
}

# 初始化MongoDB
mongo_client = pymongo.MongoClient("mongodb://{0}:{1}/".format(__DB_ADDR__, __DB_PORT__))
mongo_db = mongo_client["News"]
mongo_col = mongo_db["FinanceNews"]

# 初始化一个Selenium WebDriver
chrome_options = webdriver.ChromeOptions()  # 获取ChromeWebdriver配置文件
prefs = {"profile.managed_default_content_settings.images": 2}  # 设置不加载图片以加快速度
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--headless")  # 不使用GUI界面
chrome_options.add_argument("--disable-gpu")  # 禁用GPU渲染加速
driver = webdriver.Chrome(chrome_options=chrome_options)  # 创建ChromeWebdriver
driver.set_page_load_timeout(10)  # 设置连接超时时间为15s

# 从Master请求需要爬取的页面的URL并爬取
while True:
    try:
        # ---------------------- 向Master请求获取URL部分 ---------------------- #
        # 创建一个套接字连接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 连接Master
        sock.connect((__HOST_ADDR__, __HOST_PORT__))
        # 向Master发送获取URL的请求消息
        # 消息的格式："get,123456"，其中'123456'是Slave的ID
        req = "get,{0}".format(__SLAVE_ID__)
        sock.send(req.encode("utf-8"))
        # 接收Master发挥的响应消息（程序会在这里停住，直到收到来自Master的响应）
        # 消息的格式："https://finance.sina.com.cn/china/gncj/2018-10-17/doc-ifxeuwws5236619.shtml"
        res = sock.recv(1024)
        # 收到消息后关闭套接字，并解码出url
        sock.close()
        task_url = res.decode("utf-8")
        # --------------------------- 网页爬取部分 --------------------------- #
        print("正在爬取网页：", task_url)
        cnt = 0
        # 尝试3次连接
        while cnt < 3:
            try:
                driver.get(task_url)
                break
            except exceptions.TimeoutException:
                cnt += 1
                if cnt == 3:
                    raise exceptions.TimeoutException("timeout")
                print("超时，重试爬取：", task_url)
                time.sleep(2)
        time.sleep(1)
        # 用lxml.etree和XPath解析页面
        selector = etree.HTML(driver.page_source)
        article = {
            "title": selector.xpath(__XPATHS__["title"])[0],
            "time": selector.xpath(__XPATHS__["time"])[0],
            "author": selector.xpath(__XPATHS__["author"])[0],
            "tags": selector.xpath(__XPATHS__["tags"])[0].split(","),
            "url": selector.xpath(__XPATHS__["url"])[0],
            "content": ""
        }
        # 处理文章内容，保证换行正确
        arti_content = selector.xpath(__XPATHS__["content"])
        for arti_line in arti_content:
            if arti_line.isspace():
                continue
            article["content"] += (arti_line.lstrip() + "\n")
        # ------------------------- 数据存入MongoDB ------------------------- #
        if mongo_col.find_one({"url": article["url"]}) is None:
            mongo_col.insert_one(article)
        # ----------------------- 通知Master已完成爬取 ---------------------- #
        # 创建一个套接字连接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 连接Master
        sock.connect((__HOST_ADDR__, __HOST_PORT__))
        # 向Master发送获取URL的请求消息
        # 消息的格式："done,123456,https://finance.sina.com.cn/china/gncj/2018-10-17/doc-ifxeuwws5236619.shtml"
        req = "done,{0},{1}".format(__SLAVE_ID__, task_url)
        sock.send(req.encode("utf-8"))
    except socket.error:
        print("Master节点已下线，等待5秒后重试")
        time.sleep(5)
