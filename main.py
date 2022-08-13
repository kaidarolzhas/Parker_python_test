import pymysql
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import date
import time
headers = {
    "Accept": "*/*",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.148 YaBrowser/22.7.2.899 Yowser/2.5 Safari/537.3"
}


def get_get_data():
    try:
        connection = pymysql.connect(
            host="localhost",
            port=3306,
            user='root',
            password="",
            database="parser",
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Connected!")
    except Exception as ex:
        print("Connection failed...")
        print(ex)
    try:
        with connection.cursor() as cursor:
            number_of_rows = cursor.execute("SELECT * FROM resource")
            print("In  process...")
            for i in range(1, number_of_rows + 1):
                link_list = []
                resource = dict()
                cursor.execute('SELECT RESOURCE_ID, RESOURCE_URL,  `top_tag`, `bottom_tag`, `title_cut`, ' \
                               '`date_cut` from resource WHERE RESOURCE_ID = (%s)' % i)
                resource = cursor.fetchone()
                req = requests.get(resource.get("RESOURCE_URL"), headers=headers)
                soup = BeautifulSoup(req.text, "lxml")
                news_links = soup.find(class_=resource.get("top_tag")).findAll("a")
                for news_link in news_links:
                    link = news_link.get("href")
                    if link[0] == "h":
                        link_list.append(link)
                    else:
                        link_list.append(resource.get("RESOURCE_URL") + link)
                for um in link_list:
                    url = um
                    req = requests.get(url, headers=headers)
                    soup = BeautifulSoup(req.text, "lxml")
                    contents = soup.find(class_=resource.get("bottom_tag")).text
                    title = soup.find(class_=resource.get("title_cut")).find("h1")
                    post_date = soup.time.attrs[resource.get("date_cut")][0:10]
                    d = datetime.date(int(post_date[0:4]), int(post_date[6:7]), int(post_date[8:10]))
                    unixtime_post = time.mktime(d.timetuple())
                    unixtime_today = time.mktime(date.today().timetuple())
                    cursor.execute("""INSERT into items (link, res_id, title, content, nd_date, s_date, not_date) VALUES
                        ('%s','%s', '%s', '%s', '%s', '%s', '%s')""" % (
                        um, i, title.text, contents, int(unixtime_post), int(unixtime_today), post_date))
                    connection.commit()
                print(f"{i} resource Done!")
    finally:
        print("Done!")
        connection.close()


def main():
    get_get_data()


if __name__ == '__main__':
    main()
