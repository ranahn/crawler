import requests
from bs4 import BeautifulSoup
import json
import concurrent.futures
from queue import Queue
from threading import Thread

visited_urls = []  # لیستی برای ذخیره URL های بازدید شده
session = requests.Session()  # استفاده از Session برای بهبود عملکرد درخواست‌ها

# داده‌ها در این صف ذخیره خواهند شد
data_queue = Queue()

# تابع برای ذخیره داده‌ها در فایل JSON به صورت پیوسته
def save_data(queue):
    with open('ta_dataset.json', 'w', encoding='utf-8') as f:
        while True:
            data = queue.get()
            if data == "DONE":
                break  # وقتی "DONE" دریافت شد، ذخیره‌سازی متوقف می‌شود
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.write("\n")
            print(f"Data saved for: {data['URL']}")  # اشکال‌زدایی

# تابع برای استخراج محتوای صفحات
def crawl_page(url):
    if url in visited_urls or not url.startswith("https://www.tebyan.net"):
        return
    visited_urls.append(url)

    print(f"Fetching: {url}")

    try:
        response = session.get(url)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            print(f"Failed to fetch {url}")
            return
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    title = soup.title.string if soup.title else 'No Title'
    content = ' '.join([p.text for p in soup.find_all('p')])

    if 'پزشکی' in title or 'سلامت' in content:
        # اضافه کردن داده‌ها به صف
        data_queue.put({
            'URL': url,
            'Title': title,
            'Content': content
        })

    for a_tag in soup.find_all('a', href=True):
        next_url = a_tag['href']
        if next_url.startswith('/'):
            next_url = f"https://www.tebyan.net{next_url}"
        if next_url.startswith("mailto:"):
            continue
        if next_url not in visited_urls:
            to_crawl.append(next_url)

# لیست URL‌های مورد بررسی
# لیست URL‌های مورد بررسی
start_url = "https://www.tebyan.net/newindex.aspx?pid=19608&Keyword=%D8%B3%D9%84%D8%A7%D9%85%D8%AA"  # صفحه مربوط به سلامت سایت تبیان

# لیستی از URL‌هایی که باید بررسی شوند
to_crawl = [start_url]

# شروع ذخیره‌سازی در یک نخ جداگانه
saver_thread = Thread(target=save_data, args=(data_queue,))
saver_thread.start()

# کاوش صفحات با استفاده از ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    while to_crawl:
        future_to_url = {executor.submit(crawl_page, url): url for url in to_crawl[:10]}
        to_crawl = to_crawl[10:]
        for future in concurrent.futures.as_completed(future_to_url):
            future_to_url[future]

# پایان ذخیره‌سازی
data_queue.put("DONE")
saver_thread.join()

print("Crawling completed! Data saved to ta_dataset.json")
