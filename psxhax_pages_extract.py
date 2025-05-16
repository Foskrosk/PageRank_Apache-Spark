import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Словарь для хранения информации о страницах
psxhax_pages_info = {
    'page_title': [],
    'page_url': [],
    'links': [],
}


def get_articles(response):
    # Разбор HTML-контента с использованием BeautifulSoup
    soup = BeautifulSoup(response.content, 'lxml')
    # Поиск всех статей на странице
    articles = soup.findAll('a', class_='porta-article-header')
    articles_urls = [link['href'] for link in articles]

    for url in articles_urls:
        article_url = 'https://www.psxhax.com' + url
        while True:
            try:
                # Отправка GET-запроса к странице статьи
                response = requests.get(article_url)
                # Проверка, успешно ли выполнен запрос
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'lxml')
                    # Поиск блока с текстом статьи
                    wiki_box = soup.find('div', class_='bbWrapper')
                    # Поиск всех ссылок на другие темы форума в тексте
                    threads = wiki_box.find_all("a", href=lambda href: href and href.startswith(
                        "https://www.psxhax.com/threads/"))
                    # Извлечение ссылок и информации о статье
                    threads_links = set([link['href'] for link in threads])
                    article_title = soup.find('h1', class_="p-title-value").text.strip()
                    article_links = ','.join(str(link) for link in threads_links)
                    psxhax_pages_info['page_title'].append(article_title)
                    psxhax_pages_info['page_url'].append(article_url)
                    psxhax_pages_info['links'].append(article_links)
                    break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                print("Произошла ошибка соединения или таймаут. Повтор через 30 секунд...")
                time.sleep(30)
                continue


def main():
    # Главная функция для обхода всех страниц с публикациями на psxhax
    url = 'https://www.psxhax.com/articles/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    # Получение общего количества страниц
    nb_page = soup.find('ul', class_="pageNav-main").find_all('li')[-1].text.strip()
    i = 1
    while True:
        try:
            if i == int(nb_page) + 1:
                break
            url = f'https://www.psxhax.com/articles/page-{i}'
            response = requests.get(url)
            if response.status_code == 200:
                # Обработка статей на текущей странице
                print(f'Обработка страницы psxhax {i}/{nb_page} ...')  # всего страниц примерно 414
                get_articles(response)
                i += 1
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print("Произошла ошибка соединения или таймаут. Повтор через 30 секунд...")
            time.sleep(30)
            continue

    # Создание DataFrame из собранной информации
    pages_df = pd.DataFrame(psxhax_pages_info)
    # Сохранение DataFrame в CSV-файл с индексом в колонке 'page_id'
    pages_df.to_csv('psxhax_pages_info.csv', index_label='page_id')
    print('==========================================================')
    print('\nИнформация о страницах успешно сохранена в файл psxhax_pages_info.csv.')


if __name__ == '__main__':
    main()

