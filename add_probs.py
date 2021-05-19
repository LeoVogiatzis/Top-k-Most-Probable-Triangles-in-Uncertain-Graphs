import pandas as pd
import numpy as np

file = '.csv'
df = pd.read_csv(file, sep='\t')
df_size = len(df)
probabilities = np.random.uniform(0, 1, df_size)
df['probs'] = probabilities
df.to_csv('rdy_' + str(file), index=False, sep=' ', header=False)
