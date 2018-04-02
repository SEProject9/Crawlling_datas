from bs4 import BeautifulSoup
import requests
import os
import hashlib
import time
import pymysql
import random


class crawl:
    class db:
        def __init__(self, host='120.78.62.217', user='root', password='Zkw012300', db='testforcrawl', charset='utf8'):
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

    ##初始化变量
    def __init__(self):
        self.host='http://report.iresearch.cn/'
        self.headers= {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.103 Safari/537.36'}
        self.type_vid_map= {'媒体营销': '59', '网络服务': '60', '文化娱乐': '61', '医疗': '62', '教育': '63', '云服务': '64',
           '''人工智能': '65',''' '体育': '66', '工具与技术': '67', '房产': '68', '智能硬件': '69', '金融': '70',
           '''农业': '71',''' '航天航空': '72', '零售': '73', 'B2B': '74', '物流': '75', '旅游': '76',
           '生活服务': '77', '物联网': '78', '用户洞察': '79'}
        ##人工智能&&农业测试不通过？
        self.vid_type_map=  {value: key for key,value in self.type_vid_map.items()}
        self.article_base_address= self.host + 'include/ajax/user_ajax.ashx?reportid=<?aid?>&work=rdown&url=http%3A%2F%2Freport.iresearch.cn%2Freport%2F201803%2F<?aid?>.shtml'
        self.login_request = 'http://center.iresearch.cn/ajax/process.ashx?work=login&uAccount=2560513293%40qq.com&uPassword=hahaha123456&days=15&t=0.24829489579742514'
        self.dbu= crawl.db()


    ##get_tuplelist('媒体营销')
    def get_tuplelist(self, type):
        session = requests.session()

        def get_report_url(type):
            base_url = 'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=<??>&sid=2&yid=0'
            vid = self.type_vid_map[type]
            if (vid is not None):
                return base_url.replace('<??>', vid)
            else:
                return base_url.replace('<??>', '59')

        def getmd5(obj=None):
            string = str(obj).encode(encoding='gb2312')
            md5 = hashlib.md5()
            md5.update(string)
            return md5.hexdigest()

        search_url = get_report_url(type)
        res = session.get(search_url, headers=self.headers)
        soup = BeautifulSoup(res.content, 'html.parser')
        a = soup.find_all('a')
        p = soup.find_all('p')
        list = []
        for aaa in a:
            if (aaa.string is None):
                a.remove(aaa)
        for (aaa, ppp) in zip(a, p):
            list.append(
                (getmd5(aaa.string), aaa.string, type, ppp.string, self.get_rs_addr(aaa.get('href')), aaa.get('href')))
        return list


    ##get_rs_addr('http://report.iresearch.cn/report/201803/3185.shtml')
    def get_rs_addr(self,aaddr):
        aid = os.path.basename(aaddr).replace('.shtml', '')
        return self.host + 'include/ajax/user_ajax.ashx?reportid=<?aid?>&work=rdown&url=http%3A%2F%2Freport.iresearch.cn%2Freport%2F201803%2F<?aid?>.shtml'.replace(
            '<?aid?>', aid)

    ##download_rs('http://report.iresearch.cn/report/201803/3184.shtml', '/users/zspirytus/desktop/', '3184.pdf')
    def download_rs(self,rs_addr, dir, file_name, retries=3):
        def login():

            session = requests.session()
            session.get(self.login_request)

        login()
        session = requests.session()
        aaddr = self.get_rs_addr(rs_addr)
        rs = session.get(aaddr)
        try:
            with open(dir + file_name, 'wb') as f:
                f.write(rs.content)
        except:
            if (retries > 0):
                self.download_rs(self,rs_addr, dir, file_name, retries - 1)

    def op_db(self):
        for key in self.vid_type_map.keys():
            ls = self.get_tuplelist(self.vid_type_map[key])
            for l in ls:
                try:
                    print(l)
                    self.dbu.executeUpdate(
                        'Insert into datas(aid,atitle,atype,aintro,adownloadaddr,apageaddr) values(%s,%s,%s,%s,%s,%s)',
                        l)
                    rs = self.dbu.select('Select atitle From datas')
                except pymysql.err.IntegrityError as e:
                    pass
            time.sleep(random.random())
        self.dbu.release()


    def create_table(self):
        try:
            self.dbu.executeUpdate('Create Table datas(aid char(32) primary key,'
                                   'atitle varchar(100),'
                                   'atype varchar(30),'
                                   'aintro varchar(200),'
                                   'adownloadaddr char(255),'
                                   'apageaddr char(255)'
                                   ')engine=innodb default charset=utf8')
        except pymysql.err.IntegrityError as e:
            pass
        finally:
            self.dbu.release()


def split_list(list, n):
    lists = []
    step = len(list) // n
    for i in range(0, step):
        lists.append(list[i * n:(i + 1) * n])
    lists.append(list[step * n:])
    return lists


if(__name__=='__main__'):
    crawlling = crawl()
    crawlling.op_db()