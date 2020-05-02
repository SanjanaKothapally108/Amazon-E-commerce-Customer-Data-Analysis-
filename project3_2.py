import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pymongo
import pandas as pd

cl = pymongo.MongoClient()
db = cl.test
print("Collections in the respective database test")
print(db.list_collection_names())
df2 = pd.DataFrame(db.amazon3.find({}))
# print(df2.columns)
data = df2['reviews']
rows = []
for row in data:
    rows.append(row)
d1 = pd.DataFrame(rows)
# print(d1)
d1.dropna()
print(d1.columns)
plt.figure(figsize=(10,5))
sns.countplot(d1['rating'])
plt.title('Count ratings')
plt.show()
df3 = pd.DataFrame(db.amazon1.aggregate([{"$group": {"_id": "$reviews.username", "count": {"$sum": 1}}},{"$limit": 10}]))
print(df3)
df3.plot(kind='bar',x='_id',y='count')
plt.show()

