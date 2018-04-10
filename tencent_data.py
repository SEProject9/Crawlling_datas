import urllib.request
import requests
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool
import time

work_path = '/users/zspirytus/desktop/tecent_data'
url = 'http://data.qq.com/reports?page='
host = 'http://data.qq.com'
test = 'http://data.qq.com/article?id=2517'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.103 Safari/537.36'}
core_num = 4


def check_workpath():
    global work_path
    if (not os.path.exists(work_path)):
        os.mkdir(path=work_path)


def getHtml(url,
            headers=headers,
            retries=5):
    try:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request) as html:
            return html.read()
    except urllib.error.HTTPError as e:
        if (retries > 0):
            getHtml(url, headers, retries - 1)
        else:
            return None


def get_article_page(url, page=100, sleep=0.1):
    total_articles = []
    global host
    for i in range(page):
        html = getHtml(url + str(i)).decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        lists = soup.find_all('a')
        for list in lists:
            href = host + list['href']
            if (str(href).find('/article') != -1 and href not in total_articles):
                total_articles.append(href)
        time.sleep(sleep)
    return total_articles


def get_pdf_url(base_url):
    pdf_url = []
    global host
    articles_url = get_article_page(base_url, page=20)
    for article in articles_url:
        html = getHtml(article).decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all('a'):
            href = host + a['href']
            if (str(href).find('/resource') != -1 and href not in pdf_url):
                pdf_url.append(href)
                break
    return pdf_url


def savePdf(url, pdf_name):
    global work_path
    response = requests.get(url, headers=headers, stream=True)
    if not os.path.exists(work_path):
        os.makedirs(work_path)
    with open(os.path.join(work_path, pdf_name), "wb") as pdf_file:
        for content in response.iter_content():
            pdf_file.write(content)


def split_list(list, n):
    lists = []
    step = len(list) // n
    for i in range(0, step):
        lists.append(list[i * n:(i + 1) * n])
    lists.append(list[step * n:])
    return lists


def multi_save_pdf(pdfs):
    for pdf in pdfs:
        p = Pool(4)
        for pdf_url in pdf:
            p.apply_async(savePdf, args={pdf_url, os.path.basename(pdf_url)})
        p.close()
        p.join()


def single_save_pdf(pdfs):
    for pdf in pdfs:
        savePdf(pdf, os.path.basename(pdf))


if (__name__ == '__main__'):
    pdfs = split_list(get_pdf_url(url), core_num)
    time_start = time.time()
    multi_save_pdf(pdfs)
    time_end = time.time()
    print(time_end - time_start)
