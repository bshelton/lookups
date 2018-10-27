import socket
import re
import threading
import sys
import timeit
import queue
import csv

start = timeit.default_timer()

lines = []
num = int(sys.argv[1])

with open('output.csv','w') as file:
    file.write("orig_domain,recur_ip,recur_domain,timestamp\n")

    def is_ip(ip):

        validIpAddressRegex = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
        
        aa = re.match(validIpAddressRegex,ip)
        if aa:
            return True
        else:
            return False

    def query(line):
        flag = is_ip(line.strip())
        current_dns = ""
        current_ips = ""
        recursive = []

        if flag == True:
            print ("IP, needs to be converted to DNS")
            try:
                current_dns = socket.gethostbyaddr(line.strip())
                current_dns = current_dns[0]
                
                file.write("," + current_dns + "\n")
            except:
                print ("Cant resolve")
        elif flag == False:
            print ("DNS, need to resolve all ips")
            current_ips = list( map( lambda x: x[4][0], socket.getaddrinfo( \
            line.strip(),22,type=socket.SOCK_STREAM)))
            print (current_ips)

            for i in current_ips:
                try:
                    temp = socket.gethostbyaddr(i)
                    print (temp[0])
                    file.write("," + temp[0]+ "\n")
                except:
                    print("Can't resolve")
            file.write("," + str(current_ips)+ "\n")


    with open('list.txt', 'r') as f:
        for line in f:
            lines.append(line)


    stop = timeit.default_timer()

    def wrapper_targetFunc(f, q):
        while True:
            try:
                work = q.get(timeout=3)  # or whatever
            except queue.Empty:
                return
            f(work)
            q.task_done()

    q = queue.Queue()
    for i in lines:
        q.put_nowait(i)

    for _ in range(num):
        threading.Thread(target=wrapper_targetFunc,
                        args=(query, q)).start()
    q.join()

    print('Time: ', stop - start)