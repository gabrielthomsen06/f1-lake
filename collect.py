import pandas as pd
pd.set_option('display.max_columns', None)

import fastf1

class CollectResults:

    def __init__(self, years=[2021,2022,2023], models=['R', 'S']):
        self.years = years
        self.models = models

    def get_data(self, year, gp, mode) -> pd.DataFrame:
        try:
            session = fastf1.get_session(year, gp, mode)
        
        except ValueError as err:
            return pd.DataFrame()

        session.load()

        df = session.results
        df["Mode"] = mode

        return df

    def save_data(self, df, year, gp, mode):
        df.to_parquet(f'data/{year}_{gp:02d}_{mode}.parquet')

    def process(self, year, gp, mode):
        df = self.get_data(year, gp, mode)

        if df.empty:
            return False
        
        self.save_data(df, year, gp, mode)
        return True

    def process_year_mode(self, year, mode):
        for i in range(1, 50):
            if not self.process(year, i, mode):
                break    

collect = CollectResults([2021,2022], ['R'])
collect.process_year_mode(2021, 'R')