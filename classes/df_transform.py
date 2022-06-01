class Transform:
    def __init__(self, merged_df):
        self.merged_df = merged_df

    def transform(self):
        #print(self.merged_df)
        #removing leading zeros in customer id
        self.merged_df['customer_id'] = self.merged_df['customer_id'].apply(lambda x: str(int(x)))
        # resetting indexes starting from zero
        self.merged_df = self.merged_df.reset_index(drop=True)
        # replace nan to blank in secondary_address field
        self.merged_df['secondary_address'] = self.merged_df['secondary_address'].fillna('')
        #print(self.merged_df)
        return self.merged_df