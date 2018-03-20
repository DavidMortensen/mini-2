import matplotlib.pyplot as plt
import numpy as np

N = 4

Python = [7.3,31.8, 76.2, 121.8]
RDD= [5.0,20.0,52.0,78.0]
SQL= [3.0,11.0,26.0,2.0]


ind = np.arange(N) 
width = 0.25       

plt.bar(ind-width, Python, width, label='Python', color="red")
plt.bar(ind, RDD, width,label='RDD', color="green")
plt.bar(ind+width, SQL, width, label='SQL', color="blue")


plt.ylabel('Seconds (time)')
plt.title('Query 1 - 1 Thread')

plt.xticks(ind + width / 3, ('1 file', '4 files', '8 files', '12 files'))
plt.legend(loc='best')
plt.savefig("Bar Chart (Q1).pdf")
#plt.show()