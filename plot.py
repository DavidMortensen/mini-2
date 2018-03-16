from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

Python = [1,3,9,2,3,5,9,11]
SQL= [9,2,3,4,5,6,7,3,2]
RDD= [3,2,5,3,6,8,9,0,1]

with PdfPages('plot.pdf') as pdf:
    plt.figure(figsize=(7,7))
    plt.plot(Python, color="red")
    plt.plot(SQL, color="green")
    plt.plot(RDD, color="yellow")
    plt.title("Title")
    plt.ylabel("ylabel")
    plt.axis([0,10,0,15])
    plt.xlabel("xlabel")
    plt.legend(["Python", "SQL", "RDD"], loc='upper right')
    pdf.savefig()
    plt.close()

