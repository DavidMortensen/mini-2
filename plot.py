from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

Python = [0.2]
RDD= [5.0, 21.0, 51.0, 39.4]
SQL= [3.8,10.6,24.3, 80.0]

x=np.arange(4)


with PdfPages('plot.pdf') as pdf:
    plt.figure(figsize=(7,7))
    plt.xticks(x ,('1', '4', '8', '12'))
    plt.plot(Python, color="red")
    plt.plot(SQL, color="green")
    plt.plot(RDD, color="yellow")
    plt.title("Title")
    plt.ylabel("ylabel")
    plt.axis([0, 4, 0, 120])
    plt.xlabel("xlabel")
    plt.legend(["Python", "SQL", "RDD"], loc='upper right')
    pdf.savefig()
    plt.close()

