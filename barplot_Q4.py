import matplotlib.pyplot as plt
import numpy as np

N = 4

Python = [50]
RDD= [5.0,21.0, 51.0, 39.4]
SQL= [3.8,10.6,24.3, 80.0]

ind = np.arange(N) 
width = 0.25       

plt.bar(ind-width, Python, width, label='Python', color="red")
plt.bar(ind, RDD, width,label='RDD', color="green")
plt.bar(ind+width, SQL, width, label='SQL', color="blue")


plt.ylabel('Seconds (time)')
plt.title('Querie 4')

plt.xticks(ind + width / 3, ('1 file', '4 files', '8 files', '12 files'))
plt.legend(loc='best')
plt.savefig("Bar Chart (Q4).pdf")
#plt.show()