"""
    itertools batched
"""
from itertools import batched
batches = batched([1, 2, 3, 4, 5, 6, 7], 3)
print(list(batches))


names = ["Alice",  "Bob",  "Charlie"]
scores = [85,  90]
pairs = zip(names, scores)
print(list(pairs))


from  itertools  import  zip_longest
pairs = zip_longest(names, scores, fillvalue="N/A")
print(list(pairs))


from  itertools  import  product
flavors = ["草莓",  "芒果",  "蓝莓"]
sizes = ["大杯",  "小杯"]
combinations = product(flavors, sizes)
print(list(combinations))


digits = [0, 1]
print(list(product(digits, repeat=3)))



from  itertools  import  starmap
pairs = [(2,  3), (4,  5), (6,  7)]
results = starmap(lambda a,b: a*b, pairs)
print(list(results))


from  itertools  import  groupby
words = ["cat",  "dog",  "banana",  "apple",  "cherry"]
words_sorted =  sorted(words, key=len)   # 先按长度排序
groups = groupby(words_sorted, key=len)
for  length, group  in  groups:
       print(f"长度为  {length}  的单词:",  list(group))






if __name__ == '__main__':
    pass
