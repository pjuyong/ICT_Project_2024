import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from confluent_kafka import Producer
import socket
import time
import re

# Kafka Producer 설정
producer_conf = {
    'bootstrap.servers': '172.31.31.143:9092,172.31.32.249:9092,172.31.63.124:9092',
    'client.id': socket.gethostname()
}

producer = Producer(producer_conf)

# 메시지 전달 콜백 함수
def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg.value}: {err}")
    else:
        print(f"Message produced: {msg.value}")

# 크롬 드라이버 기본 설정
options = webdriver.ChromeOptions()
options.add_argument('--headless') #브라우저 없이 실행
options.add_argument('--no-sandbox') #샌드박스 보안기능 비활성화
options.add_argument('--disable-dev-shm-usage')

# 크롬 드라이버 최신 버전 설정
service = Service(executable_path='/home/ubuntu/chromedriver-linux64/chromedriver')

# 크롬 드라이버
driver = webdriver.Chrome(service = service, options = options)

# 크롤링 함수
def crawl_and_produce():
    baseurl = 'https://cafe.naver.com/joonggonara/'
    page = 1
    display = 30
    
    driver.get(baseurl + 'ArticleList.nhn?search.clubid=10050146&search.page=' + str(page) + '&userDisplay=' + str(display))
    driver.switch_to.frame('cafe_main') #iframe 전환
    soup = bs(driver.page_source, 'html.parser')

    # 게시글 목록 추출
    print("게시글 추출을 실행합니다.")
    soup = soup.find_all(class_='article-board m-tcol-c')[1]
    datas = soup.find_all(class_= 'td_article')

    for idx, data in enumerate(datas):
        title = data.find(class_='article')
        link = title.get('href')
        title = title.get_text().strip()

        match = re.search(r'articleid=(\d+)', link)
        titleId = match.group(1) if match else None
            
        print(title)
        print(baseurl + link)
        print(titleId)
        #메시지 전송
        producer.produce('naver_cafe_posts', key=titleId, value=title, callback=acked)

        if idx % 100 == 0:
            producer.poll()
        
    # 메시지 전송 대기
    producer.flush()

if __name__ == "__main__":
    while True:
        crawl_and_produce()
        time.sleep(30)
