import sys
import subprocess
from timeit import default_timer as timer

start = timer()
procs = []
for i in range(100):
#     proc = subprocess.call([sys.executable, 'michael_test.py', '{}in.csv'.format(i), '{}out.csv'.format(i)])
     proc = subprocess.Popen([sys.executable, 'michael_test.py', '{}in.csv'.format(i), '{}out.csv'.format(i)])
     procs.append(proc)

for proc in procs:
     proc.wait()


end = timer()

print(end - start)