

def fun():
    i=0
    yield i
    while i<10:
        i+=1
        yield i

for j in fun():
    print(j)