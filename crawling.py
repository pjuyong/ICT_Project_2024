import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from confluent_kafka import Producer
import socket
import time
import re
import os #titleId만 저장할 것이기에 csv 보단 os가 더 적합하다고 판단.

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

LAST_TITLE_ID = "./last_titleId.txt"

# 마지막 게시글 id 저장
def saveTitleId(titleId):
        with open(LAST_TITLE_ID, "w") as i:
            i.write(titleId)
            print("마지막 게시글 ID 저장 완료" + titleId)

# 마지막 게시글 id 불러오기
def loadTitleId():
    if os.path.exists(LAST_TITLE_ID):
        with open(LAST_TITLE_ID, "r") as i:
            print("마지막 게시글 ID 불러오기 완료")
            return i.read().strip()

    return None


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
    display = 50

    driver.get(baseurl + 'ArticleList.nhn?search.clubid=10050146&search.page=' + str(page) + '&userDisplay=' + str(display))
    driver.switch_to.frame('cafe_main') #iframe 전환
    soup = bs(driver.page_source, 'html.parser')

    # 마지막 게시글 id 불러오기
    last_titleId = loadTitleId()

    # 게시글 목록 추출
    print("게시글 추출을 실행합니다.")
    soup = soup.find_all(class_='article-board m-tcol-c')[1]
    datas = soup.find_all(class_= 'td_article')

    for idx, data in enumerate(datas):
        title = data.find(class_='article')
        url = baseurl + title.get('href')

        # 중복 공백 제거 작업 및 문자열 형태로 저장
        title = ' '.join(title.get_text().split())

        # 저장된 url 에서 articleid만 추출
        match = re.search(r'articleid=(\d+)', url)
        titleId = match.group(1) if match else None
        
        if titleId is None:
            print("titleId 추출 실패, 다음 게시글로 넘어감")
            continue
        print(title)
        print(url)
        print(titleId)

        # 최신 게시글부터 마지막 titleId까지 처리
        if last_titleId and int (last_titleId) >= int (titleId):
            print("마지막 게시글까지 크롤링 완료 최신 titleId " + titleId + "이전 titleId" + last_titleId)
            continue

        #메시지 전송
        producer.produce('naver_cafe_posts', key=titleId, value=title, callback=acked)
        print("메시지 전송 완료")

        if idx % 500 == 0:
            producer.poll()

    # 메시지 전송 대기
    producer.flush()
    
    # 마지막 게시글 저장
    saveTitleId(titleId)

if __name__ == "__main__":
    while True:
        crawl_and_produce()
        time.sleep(3)
