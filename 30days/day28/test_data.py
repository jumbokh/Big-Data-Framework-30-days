import pandas as pd
import random
from datetime import datetime, timedelta

now = datetime.now()
rows = []

for i in range(100):
    now += timedelta(seconds=random.randint(1, 10))
    v = random.uniform(-10, 10)
    rows.append([now, v])

df = pd.DataFrame(rows, columns=['event_time', 'value'])
df.to_csv('test_data.csv', header=None, index=None)