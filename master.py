from selenium import webdriver
from tasklist import TaskList
from lxml import etree
import time
import socket

__BIND_ADDR__ = "0.0.0.0"                                 # 监听本机的所有网卡IP
__BIND_PORT__ = 8888                                       # 监听8888端口

__LIST_URL__ = "http://finance.sina.com.cn/china/"     # 文章列表的网址
__COUNT_URL__ = 50                                         # 设定只获取50篇文章的URL

__XPATH__URL__ = "//div[@class='feed-card-item']/h2/a/@href"
__XPATH_NEXT__ = "//span[@class='pagebox_next']/a"

# 初始化一个任务列表
task_list = TaskList(timeout=30)

# 初始化一个套接字
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((__BIND_ADDR__, int(__BIND_PORT__)))
sock.listen(50)

# 初始化一个Selenium WebDriver
chrome_options = webdriver.ChromeOptions()  # 获取ChromeWebdriver配置文件
prefs = {"profile.managed_default_content_settings.images": 2}  # 设置不加载图片以加快速度
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--headless")  # 不使用GUI界面
chrome_options.add_argument("--disable-gpu")  # 禁用GPU渲染加速
driver = webdriver.Chrome(chrome_options=chrome_options)  # 创建ChromeWebdriver
driver.set_page_load_timeout(10)  # 设置连接超时时间为15s

# 从页面中解析url，并放入任务列表中
print("正在从网页中解析URL链接...")
driver.get(__LIST_URL__)
url_cnt = 0
while url_cnt < __COUNT_URL__:
    # 如果不是第一个页面，那么需要点击下一页
    if url_cnt != 0:
        time.sleep(1)
        driver.find_element_by_xpath(__XPATH_NEXT__).click()
    # 从已经加载好的网页源码中读取url
    selector = etree.HTML(driver.page_source)
    urls = selector.xpath(__XPATH__URL__)
    url_cnt += len(urls)
    # 将这一个页面中读取到的url加入任务列表中
    task_list.put_tasks(urls)
driver.close()

# 监听8888端口，等待slave连接并为其分配任务
print("等待Slave节点连接中...")
while True:
    # 若任务列表中的所有任务都已完成，则程序退出
    if task_list.is_empty():
        print("所有任务已完成")
        sock.close()
        break
    # 等待并接收来自Slave的连接（程序会在这里停住，直到有Slave连接）
    conn, addr = sock.accept()
    conn.settimeout(10)
    try:
        # 接收到来自Slave的请求（程序会在这里停住，直到接收到Slave的消息）
        req = conn.recv(1024).decode("utf-8")
        if req.startswith("get"):
            # 如果Slave发送的消息以"get"开头，则给它发回一个用来爬取的URL
            # 消息的格式："get,123456"
            # slave_id取得发来消息的Slave的ID
            slave_id = req.split(",")[1]
            task_url = task_list.get_task()
            # 把url发给Slave
            print("向'Slave {0}' 分配爬取 '{1}'".format(slave_id, task_url))
            conn.send(task_url.encode("utf-8"))
        elif req.startswith("done"):
            # 如果Slave发送的消息以"done"开头，说明它是在告诉master它完成了一个任务
            # 消息的格式："done,123456,https://finance.sina.com.cn/china/gncj/2018-10-17/doc-ifxeuwws5236619.shtml"
            # slave_id取得发来消息的Slave的ID，done_url取得Slave发来的完成爬取的页面的链接
            slave_id = req.split(",")[1]
            done_url = req.split(",")[2]
            # 这里将已爬取的页面完全从任务列表中删除
            print("'Slave {0}' 完成爬取 '{1}'".format(slave_id, done_url))
            task_list.done_task(done_url)
            conn.send("ok".encode("utf-8"))
    except socket.timeout:
        print("套接字连接超时")
    conn.close()
