import requests
from bs4 import BeautifulSoup
import re
import os
import time
import random
import pymysql


class crawl:
    class db:
        def __init__(self, host='47.106.137.93', user='root', password='Zkw012300', db='sys_edu', charset='utf8'):
            self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset)
            self.cursor = self.conn.cursor()

        def executeUpdate(self, sql, params=None):
            self.cursor.execute(sql, params)
            self.conn.commit()

        def select(self, sql, params=None):
            self.cursor.execute(sql, params)
            resultSet = self.cursor.fetchall()
            return resultSet

        def release(self):
            self.cursor.close()
            self.conn.close()

    def __init__(self):
        self.tuple_type = (
        '互联网', 'mobile-internet', 'electronic-commerce', 'social-network', 'advertising', 'economic-data', 'service')
        self.type_id = {'互联网': 1, 'mobile-internet': 2, 'electronic-commerce': 3, 'social-network': 4, 'advertising': 5,
                        'economic-data': 6, 'service': 7}
        self.id_type = {value: key for key, value in self.type_id.items()}
        self.base_url = 'http://www.199it.com/archives/category/<type>/page/<page>'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.103 Safari/537.36'}
        self.content_dir = '/home/admin/199IT/'
        self.content_dir = '/users/zhangkunwei/desktop/199IT'
        self.log_dir = '/home/admin/199IT/log/'
        self.log_dir = '/users/zhangkunwei/desktop/199IT/log'
        self.log_file = 'log.log'
        self.dbo = crawl.db()
        self.all_info = []
        self.new_info = []

    def getHtml(self, url, retries=3):
        try:
            session = requests.session()
            res = session.get(url, headers=self.headers, timeout=5)
            return res
        except:
            if (retries > 0):
                return self.getHtml(url, retries - 1)

    def get_article_content(self, url):
        res = self.getHtml(url)
        ##剔除所有不允许转载的文章
        if (str(res.content.decode('utf8')).find('请勿转载') != -1):
            return 'Delete'
        ##允许转载的文章
        else:
            ##默认为pdf类型行业资讯
            ispdf = True
            article_htmlType = []
            soup = BeautifulSoup(res.content.decode('utf8'), "html.parser")
            article = soup.find('article')
            temp_soup = BeautifulSoup(str(article), "html.parser")
            body = temp_soup.find_all('p')
            for child in body:
                if (len(child.attrs) == 0):
                    article_htmlType.append(child)
                    ##观察格式，若其中一个p标签的内容(p.string)不为空，即p标签下没有子标签，则该资讯为非[pdf型]
                    if (child.string is not None):
                        ispdf = False
            ##获取所有合法标签的list
            if (article_htmlType is not None and ispdf is False):
                content_str = ''
                ##取出每条标签
                for ars in article_htmlType:
                    ##取出标签中的所有string或者子标签，所有元素记为content
                    contents = ars.contents
                    for content in contents:
                        ##如果contents中的元素content的内容为空，说明该元素是一个子标签(保存图片的标签)
                        if (content.string == None):
                            img = re.findall(r'src=\"(.*?)\"', str(content))
                            if (len(img) > 0 and img[0].find('Wechat') == -1):
                                content_str += '<img>' + img[0] + '</img>'
                        ##否则，该元素是内容
                        else:
                            content_str += '\t' + str(content.string).replace('\r', '').replace('\n', '') + '\n'
                return content_str
            else:
                return 'Delete'

    def save_article_content(self, content, file_name):
        if (os.path.exists(self.content_dir) is False):
            os.makedirs(self.content_dir)
        with open(self.content_dir + file_name,'w+') as content_file:
            content_file.write(content)

    def get_col_list(self, type, page):
        res = self.getHtml(self.base_url.replace('<type>', type).replace('<page>', str(page)))
        soup = BeautifulSoup(res.content, "html.parser")
        ars = soup.find_all('article')
        for ar in ars:
            ##避过大部分pdf类型的行业资讯
            if (str(ar).find('附下载') != -1):
                continue
            temp_soup = BeautifulSoup(str(ar), "html.parser")
            ##找到所有带有title属性的a标签
            tags = temp_soup.find_all('a', attrs={'title': re.compile('')})
            for at in tags:
                if (at.string is None):
                    continue
                article_content = self.get_article_content(at['href'])
                if (article_content is not 'Delete'):
                    self.save_article_content(article_content, os.path.basename(at['href']).replace('.html','') + '.txt')
                    tp = (self.type_id[type], str(time.strftime('%Y/%m/%d', time.localtime(time.time()))), at['href'],
                          at.string, '199IT', '199IT', self.content_dir + os.path.basename(at['href']).replace('.html','') + '.txt', 'pictures')
                    '''
                    ind_id      |   行业id
                    ind_date    |   插入时间
                    url         |   资讯源地址
                    title       |   标题
                    source      |   来源
                    author      |   作者
                    text        |   资讯内容
                    picture     |   **内容存放地址**
                    '''
                    self.new_info.append(tp)
                    print(tp)

    def get_info_list(self, max_Page=10):
        for i in range(len(self.id_type)):
            type = self.id_type[i + 1]
            for j in range(1, max_Page + 1):
                self.get_col_list(type, j)
            time.sleep(random.random())

    def op_db(self):
        self.get_info_list()
        for l in self.new_info:
            if (l in self.all_info):
                continue
            try:
                sql = 'Insert into industry_info_list(ind_id,ind_date,url,title,source,author,text,picture) values(%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')' % l
                self.dbo.executeUpdate(sql)
            except pymysql.err.IntegrityError as e:
                print(e)
            self.all_info.append(l)
        self.new_info = []

    def init_db(self):
        db_op = self.dbo
        for key in self.id_type.keys():
            try:
                sql = 'Insert into industry_list(ind_id,ind_name,ind_text) value(%d,\'%s\',\'\')' % (
                    int(key), self.id_type[key])
                db_op.executeUpdate(sql)
            except pymysql.err.IntegrityError as e:
                print(e)

    def log(self, text):
        if (os.path.exists(self.log_dir + self.log_file) is False):
            os.mknod(self.log_dir + self.log_file, 'rw')
        with open(self.log_dir + self.log_file, 'a+') as logFile:
            logFile.write(text + '\t' + str(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(time.time()))) + '\n')

    def test(self):
        print(self.dbo)
        rs=self.dbo.executeUpdate('Select ind_id From industry_list')
        print(rs)


crawlling = crawl()
crawlling.init_db()
crawlling.op_db()
