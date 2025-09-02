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


        success, _ = helpers.bulk(self.es, actions)
        print(f"{success} documents indexed successfully.")

    def bulk_update(self, actions):
        helpers.bulk(self.es, actions)
        print(f"Bulk update applied to {len(actions)} documents")

    def get_all_documents(self, size=10000):
        res = self.es.search(
            index=self.index_name,
            query={"match_all": {}},
            size=size
        )
        return res["hits"]["hits"]

    def update_document(self, doc_id, new_field_name, new_field_value):
        if self.es.exists(index=self.index_name, id=doc_id):
            self.es.update(
                index=self.index_name,
                id=doc_id,
                body={"doc": {new_field_name: new_field_value}}
            )
            print(f"Added field '{new_field_name}' to document {doc_id}")
        else:
            print(f"Document {doc_id} does not exist")

    def delete_docs_by_query(self, query):
        response = self.es.delete_by_query(
            index=self.index_name,
            body={"query": query}
        )
        return response.get('deleted', 0)

if __name__ == "__main__":
    csv_to_df = CsvToDf(r"..\data\tweets_injected 3.csv")
    df = csv_to_df.get_df()
    dal = DAL("tweets",df)
    dal.create_index()
    dal.convert_df_to_elastic()



