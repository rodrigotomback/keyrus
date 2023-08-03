import pandas as pd

class DataFrameCaster:
    def __init__(self, df):
        self.df = df

    def cast_to_int(self, columns):
        for col in columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype(pd.Int64Dtype())
        return self.df

    def cast_to_date(self, columns):
        for col in columns:
            self.df[col] = pd.to_datetime(self.df[col], format='%Y-%m-%d', errors='coerce')
        return self.df

    def cast_to_float(self, columns):
        for col in columns:
            self.df[col] = self.df[col].astype(float)
        return self.df

    def cast_to_timestamp(self, columns):
        for col in columns:
            self.df[col] = pd.to_datetime(self.df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        return self.df
