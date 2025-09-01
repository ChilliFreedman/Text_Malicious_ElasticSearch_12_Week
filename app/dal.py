from elasticsearch import Elasticsearch,helpers
from pprint import pprint
from data_loader import CsvToDf
class DAL:
    def __init__(self,index_name,df):
        self.df = df
        self.index_name = index_name
        self.es = Elasticsearch("http://localhost:9200")


    def create_index(self):
        print("self info: ")
        pprint(self.es.info())
        body = {
            "mappings": {
                "properties": {
                    "TweetID": {"type": "keyword"},
                    "CreateDate": {"type": "date",
                                   "format": "yyyy-MM-dd HH:mm:ssXXX||EEE MMM dd HH:mm:ss Z yyyy"},
                    "Antisemitic": {"type": "integer"},
                    "text": {"type": "text"},
                    "emotion": {"type": "integer"},
                    "weepon_list": {"type": "text"}

                }
            }
        }




        self.es.indices.delete(index=self.index_name,ignore_unavailable=True)
        resp = self.es.indices.create(index=self.index_name, mappings=body["mappings"])
        print("Create index response:", resp)



    def convert_df_to_elastic(self):
        actions = []
        for _, row in self.df.iterrows():
            doc = row.to_dict()
            action = {
                "_index": self.index_name,
                "_source": doc
            }
            actions.append(action)

        # הרצת bulk
        success, _ = helpers.bulk(self.es, actions)
        print(f"{success} documents indexed successfully.")


if __name__ == "__main__":
    csv_to_df = CsvToDf(r"..\data\tweets_injected 3.csv")
    df = csv_to_df.get_df()
    dal = DAL("tweets",df)
    dal.create_index()
    dal.convert_df_to_elastic()


