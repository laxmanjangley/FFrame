import random
from datetime import datetime
random.seed(datetime.now())

line = "a,b,c,d,e\n"
for i in range(1, 100000):
	for j in range(0, 5):
		line += str(random.randint(1,100))
		if j<4:
			line += ","
	line += "\n"

print (line)
# 1403817121
# 1225208672
# 174017440
# 1361557171
# 1249107209
# 1218670403
