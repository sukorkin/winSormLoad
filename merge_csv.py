import pandas as pd

a = pd.read_csv("out.txt", delimiter=';')
# print(a)
b = pd.read_csv("rep.txt", delimiter=';')
# print(b)
# b = b.dropna(axis=1)
merged = pd.merge(a, b, on='MSISDN', how='left')
# merged = a.merge(b, left_on='MSISDN')
merged.to_csv("output.txt", index=False, sep=';')
