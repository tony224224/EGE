import hashlib

print(hashlib.md5(str('10971').encode()).hexdigest())
a1 = 390205, 182699
a2 = 60306, 16460
print(hashlib.md5(str(a1 + a2).encode()).hexdigest())
print(hashlib.md5(str([78020, 78021]).encode()).hexdigest())
