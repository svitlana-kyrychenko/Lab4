import pandas as pd
import csv


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_review_product(self, product_id, customer_id, review_id, review_headline, review_body, review_date, star_rating):
        query = "INSERT INTO review_by_product (product_id, customer_id, review_id, review_headline, review_body, " \
                "review_date, star_rating) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s)" % (product_id, customer_id,
                                                                                     review_id, review_headline,
                                                                                     review_body, review_date,
                                                                                     star_rating)
        self.execute(query)

    def insert_review_customer(self, product_id, customer_id, review_id, review_headline, review_body, review_date, star_rating):
        query = "INSERT INTO review_by_customer (customer_id, product_id, review_id, review_headline, review_body, " \
                "review_date, star_rating) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s)" % (customer_id, product_id,
                                                                                     review_id, review_headline,
                                                                                     review_body, review_date,
                                                                                     star_rating)
        self.execute(query)

if __name__ == '__main__':
    host = 'localhost'
    port = 8080
    keyspace = 'hw4_kyrychenko'

    client = CassandraClient(host, port, keyspace)
    client.connect()
    with open("data/amazon_reviews_us_Books_v1_02.tsv", encoding='utf-8') as file:
        csvreader = csv.reader(file, delimiter="\t")
        next(csvreader)
        for row in csvreader:
            review_headline = row[12]
            review_body = row[13]
            review_headline = review_headline.replace('\'', '`')
            review_body = review_body.replace('\'', '`')
            client.insert_review_product(row[3], row[1], row[2], review_headline, review_body, row[14], row[7])
            client.insert_review_customer(row[3], row[1], row[2], review_headline, review_body, row[14], row[7])
    client.close()
