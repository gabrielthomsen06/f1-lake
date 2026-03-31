import pandas as pd
pd.set_option('display.max_columns', None)

import fastf1
import argparse

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

    def process_year_modes(self, year):
        for i in range(1, 50):
            for mode in self.models:
                if not self.process(year, i, mode) and mode=='R':
                    return    
                
    def process_years(self):
        for year in self.years:
            self.process_year_modes(year)
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", "-y", nargs="+", type=int)
    parser.add_argument("--modes", "-m", nargs="+")

    args = parser.parse_args()


    collect = CollectResults(args.years, args.modes)
    collect.process_years()
