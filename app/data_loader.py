import pandas as pd
class CsvToDf:
    def __init__(self,path):
        self.df = pd.read_csv(path)

    def get_df(self):
        return self.df

if __name__ == "__main__":
    csv_to_df = CsvToDf(r"..\data\tweets_injected 3.csv")
    print(csv_to_df.get_df().CreateDate)