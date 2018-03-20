import matplotlib.pyplot as plt
import numpy as np

N = 4

RDD= [1.0,3.0,4.0,6.0]
SQL= [0.8,1.0,3.0,5.0]


ind = np.arange(N) 
width = 0.25       

#plt.bar(ind-width, Python, width, label='Python', color="red")
plt.bar(ind, RDD, width,label='RDD', color="green")
plt.bar(ind+width, SQL, width, label='SQL', color="blue")


plt.ylabel('Seconds (time)')
plt.title('Query 1 - 16 Threads')

plt.xticks(ind + width / 3, ('1 file', '4 files', '8 files', '12 files'))
plt.legend(loc='best')
plt.savefig("Bar Chart (Q1).pdf")
#plt.show()