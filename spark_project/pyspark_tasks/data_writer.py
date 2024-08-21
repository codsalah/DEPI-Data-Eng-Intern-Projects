import os 
def save_to_csv(df, path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

    df.write.mode('overwrite').csv(path, header=True)