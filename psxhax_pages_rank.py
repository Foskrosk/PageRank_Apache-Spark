import pandas as pd
import networkx as nx

# Загрузить данные страниц из CSV
pages_info_df = pd.read_csv('psxhax_pages_info.csv')
# Create a directed graph
G = nx.DiGraph()

# Добавляем узлы в граф
for index, row in pages_info_df.iterrows():
    src_page_id = row['page_id']
    G.add_node(src_page_id)
print('Calculating PageRank values please wait ...')
for index, row in pages_info_df.iterrows():
    src_page_id = row['page_id']
    # Проверьте ссылку в столбце ссылок
    links = row['links']
    if pd.notna(links):
        links = row['links'].split(',')
        for link in links:
            # Search for dest_page id
            matching_row = pages_info_df.loc[pages_info_df['page_url'] == link]
            if not matching_row.empty:
                dest_page_id = matching_row.index[0]
                G.add_edge(src_page_id, dest_page_id)

# Запускаем  алгоритм PageRank
pagerank_scores = nx.pagerank(G, max_iter=50, alpha=0.85)

# Присваиваем значения PageRank датафрейму с информацией о страницах psxhax
pages_info_df['page_rank'] = [pagerank_scores[page_id] for page_id in pages_info_df['page_id']]

# Сортируем страницы psxhax по значениям PageRank
ranked_pages_df = pages_info_df.sort_values(by=['page_rank', 'page_id'], ascending=[False, True])

# Сохраняем отсортированную информацию о страницах psxhax в CSV-файл
ranked_pages_df.to_csv('ranked_psxhax_pages.csv', index=False)
print('\n')
print('==========================================================')
print('\nPsxhax pages ranks saved to ranked_psxhax_pages.csv successfully.')
