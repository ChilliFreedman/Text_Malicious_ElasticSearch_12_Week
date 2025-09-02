import pandas as pd
from elasticsearch import helpers
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')
from data_loader import CsvToDf
from dal import DAL
from pprint import pprint
class Processor:
    def __init__(self,all_docs):
        self.all_docs = all_docs
        self.df = pd.DataFrame([{**doc["_source"],"_id":doc["_id"]} for doc in all_docs])

    def add_emotion_as_df(self):
        emotion_list = []
        col_text = self.df["text"]
        for field_text in col_text:
            tweet = field_text
            score = SentimentIntensityAnalyzer().polarity_scores(tweet)
            if 1 >= score['compound'] >= 0.5:
                emot = "positive"
            elif 0.49 >= score['compound'] >= -0.49:
                emot = "neutral"
            else:
                emot = "negative"

            emotion_list.append(emot)
        # print(len(emotion_list))
        self.df["emotion"] = emotion_list

    def add_weapons(self, weapon_file="../data/weapon_list.txt"):
        with open(weapon_file, "r") as f:
            weapons = f.read().splitlines()

        def detect_weapons(text):
            for word in text.split():
                if word.lower() in [w.lower() for w in weapons]:
                    return [word.lower()]
            return []

        self.df["weepon_list"] = self.df["text"].apply(detect_weapons)

    def update_elastic_with_new_fields(self, dal):
        actions = []
        for idx, row in self.df.iterrows():
            doc_id = row["_id"]
            doc_update = {
                "_op_type": "update",
                "_index": dal.index_name,
                "_id": doc_id,
                "doc": {
                    "emotion": row["emotion"],
                    "weepon_list": row["weepon_list"]
                }
            }
            actions.append(doc_update)
        helpers.bulk(dal.es, actions)
        print("All documents updated in Elasticsearch")


if __name__ == "__main__":
    csv_to_df = CsvToDf(r"..\data\tweets_injected 3.csv")
    df = csv_to_df.get_df()
    dal = DAL("tweets", df)
    dal.create_index()
    dal.convert_df_to_elastic()
    processor = Processor(dal.get_all_documents())
    #pprint(processor.all_docs)
    processor.add_emotion_as_df()
    processor.add_weapons()
    processor.update_elastic_with_new_fields(dal)