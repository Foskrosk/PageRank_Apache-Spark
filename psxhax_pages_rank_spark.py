from pyspark.sql import SparkSession
from graphframes import GraphFrame
import sys

# Создание SparkSession
spark = SparkSession.builder.appName("PageRank").getOrCreate()

# Загрузка данных о страницах psxhax из CSV в DataFrame
pages_info_df = spark.read.csv(sys.argv[1], header=True)

# Создание DataFrame с вершинами графа
vertices_df = pages_info_df.select("page_id").withColumnRenamed("page_id", "id")


# Функция для разделения ссылок
def split_links(row):
    page_id, links = row
    if links is not None:
        links = [link.strip() for link in links.split(",")]
    else:
        links = []
    return [(page_id, link) for link in links]


# Получение пар (page_id, ссылка) из исходного DataFrame
links_rows = pages_info_df.select("page_id", "links").rdd.flatMap(split_links).collect()

# Создание списка рёбер графа
edges_list = []
for row in links_rows:
    src_page_id, link = row
    matching_row = pages_info_df.filter(pages_info_df.page_url == link).select("page_id").first()
    if matching_row is not None:
        dest_page_id = matching_row.page_id
        edges_list.append((src_page_id, dest_page_id))

# Создание DataFrame с рёбрами графа
edges_df = spark.createDataFrame(edges_list, ["src", "dst"])

# Создание графа с помощью GraphFrame
graph = GraphFrame(vertices_df, edges_df)

# Запуск алгоритма PageRank
page_rank = graph.pageRank(maxIter=50, resetProbability=0.15)

# Получение значений PageRank в виде DataFrame
page_rank_df = page_rank.vertices.select("id", "pagerank").withColumnRenamed("id", "page_id")

# Объединение исходного DataFrame с результатами PageRank
pages_info_df_with_rank = pages_info_df.join(page_rank_df, on="page_id", how="inner")

# Сортировка страниц psxhax по значению PageRank
ranked_pages_df = pages_info_df_with_rank.orderBy("pagerank", ascending=False)

# Сохранение отсортированных данных в CSV на HDFS
output_path = "hdfs://Master:9000/user/hadoop/outputs/spark_outputs/ranked_psxhax_pages"
ranked_pages_df.write.csv(output_path, header=True, mode="overwrite")

# Завершение работы SparkSession
spark.stop()
