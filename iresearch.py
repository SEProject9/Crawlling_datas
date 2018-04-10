from bs4 import BeautifulSoup
import requests
import os
import time
import pymysql
import random


class crawl:
    class db:
        def __init__(self, host='localhost', user='root', password='Zkw012300', db='sys_edu', charset='utf8'):
            self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset)
            self.cursor = self.conn.cursor()

        def executeUpdate(self, sql):
            self.cursor.execute(sql)
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
        self.cookie={'Set-Cookie' : ''}
        self.type_vid_map= {'媒体营销': '59', '网络服务': '60', '文化娱乐': '61', '医疗': '62', '教育': '63', '云服务': '64',
            '体育': '66', '工具与技术': '67', '房产': '68', '智能硬件': '69', '金融': '70',
            '航天航空': '72', '零售': '73', 'B2B': '74', '物流': '75', '旅游': '76',
           '生活服务': '77', '物联网': '78', '用户洞察': '79'}
        ##人工智能&&农业测试不通过？
        self.vid_type_map=  {value: key for key,value in self.type_vid_map.items()}
        self.article_base_address= self.host + 'include/ajax/user_ajax.ashx?reportid=<?aid?>&work=rdown&url=http%3A%2F%2Freport.iresearch.cn%2Freport%2F201803%2F<?aid?>.shtml'
        self.login_request = 'http://center.iresearch.cn/ajax/process.ashx?work=login&uAccount=2560513293%40qq.com&uPassword=hahaha123456&days=15&t=0.24829489579742514'
        self.save_dir='/home/admin/iresearch/'
        self.dbu= crawl.db()
        self.log_dir='/home/admin/iresearch/log/'
        self.log_file='log.log'


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
            list.append((int(self.type_vid_map[type]),str(time.strftime('%Y/%m/%d',time.localtime(time.time()))),self.get_rs_addr(aaa.get('href')),aaa.string,'iresearch','iresearch',ppp.string,self.save_dir+aaa.string+'.pdf'))
            '''
            self.type_vid_map[type]                |   ind_id
            self.nowDate                           |   ind_date
            self.get_rs_addr(aaa.get('href'))      |   url
            aaa.string                             |   title
            'iresearch'                            |   source
            'iresearch'                            |   author
            ppp.string                             |   text
            self.save_dir+aaa.string+'.pdf'        |   picture
            '''
        return list


    ##get_rs_addr('http://report.iresearch.cn/report/201803/3185.shtml')
    def get_rs_addr(self,aaddr):
        aid = os.path.basename(aaddr).replace('.shtml', '')
        return self.host + 'include/ajax/user_ajax.ashx?reportid=<?aid?>&work=rdown&url=http%3A%2F%2Freport.iresearch.cn%2Freport%2F201803%2F<?aid?>.shtml'.replace(
            '<?aid?>', aid)

    ##download_rs('http://report.iresearch.cn/report/201803/3184.shtml', '/users/zspirytus/desktop/', '3184.pdf')
    def download_rs(self,rs_down_addr, dir, file_name, retries=3):
        session = requests.session()
        session.get(self.login_request)
        rs = session.get(rs_down_addr,headers= self.headers)
        if(os.path.exists(dir + file_name)):
            return;
        try:
            if(os.path.exists(dir) is False):
                os.makedirs(dir)
            with open(dir + file_name, 'wb') as f:
                f.write(rs.content)
        except:
            print('error!')
            if (retries > 0):
                self.download_rs(self,rs_down_addr, dir, file_name, retries - 1)

    def download_all_rs(self):
        db_op=self.dbu
        sql='Select url,picture from industry_data_list'
        results=db_op.select(sql)
        for r in results:
            file_name = os.path.basename(r[1])
            dir = r[1].replace(file_name,'').rstrip("\\")
            self.download_rs(r[0], dir, file_name)
        self.log('download all resource')


    def op_db(self):
        for key in self.vid_type_map.keys():
            ls = self.get_tuplelist(self.vid_type_map[key])
            for l in ls:
                try:
                    ##print(l)
                    sql='Insert into industry_data_list(ind_id,ind_date,url,title,source,author,text,picture) values(%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')'%l
                    ##print(sql)
                    '''
                    ind_id      |   行业id
                    ind_date    |   插入时间
                    url         |   下载地址
                    title       |   标题
                    source      |   来源
                    author      |   作者
                    text        |   简介
                    picture     |   内容存放地址
                    '''
                    self.dbu.executeUpdate(sql)
                except pymysql.err.IntegrityError as e:
                    print(e)
            time.sleep(random.random())
        self.log('updated database')


    def init_db(self):
        db_op=self.dbu
        for key in self.vid_type_map.keys():
            try:
                sql = 'Insert into industry_list(ind_id,ind_name,ind_text) value(%d,\'%s\',\'\')' % (int(key), self.vid_type_map[key])
                print(sql)
                db_op.executeUpdate(sql)
            except pymysql.err.IntegrityError as e:
                print(e)

    def log(self,text):
        if(os.path.exists(self.log_dir + self.log_file) is False):
            os.mknod(self.log_dir + self.log_file,'rw')
        with open(self.log_dir + self.log_file,'a+') as logFile:
            logFile.write(text+'\t'+str(time.strftime('%Y/%m/%d %H:%M:%S',time.localtime(time.time())))+'\n')



def split_list(list, n):
    lists = []
    step = len(list) // n
    for i in range(0, step):
        lists.append(list[i * n:(i + 1) * n])
    lists.append(list[step * n:])
    return lists


if(__name__=='__main__'):
    crawlling =crawl()
    while True:
        crawlling.op_db()
        crawlling.download_all_rs()
        time.sleep(3600*24*7)