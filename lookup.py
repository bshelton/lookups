import socket
import re
import threading
import sys
import time
import queue
import csv
from datetime import datetime, timedelta
import zipfile
import os

numberOfThreads = int(sys.argv[1])
minutesToSleep = 0.1
minutesToZipFile = 3
outputFileName = 'output.csv'
inputFileName = 'list.txt'

def formattedDateWithSeconds(date):
    return date.strftime('%Y%m%d%H%M%S')

def formattedDate(date):
    return date.strftime('%Y%m%d%H%M')

def zipFile(fileName):
    print("Zipping file " + fileName + ".zip")
    zipfile.ZipFile(formattedDate(datetime.now()) + "_resolutions_archive" + ".zip", mode='w').write(fileName)

def addHeaderIfNeeded():
    headerText = "orig_domain,recur_ip,recur_domain,timestamp\n"
    containsHeader = False
    with open(outputFileName, 'r') as listOutputFile:
        for line in listOutputFile:
            if (line == headerText):
                containsHeader = True
            break;
        listOutputFile.close()
    if not containsHeader:
        with open(outputFileName, 'w+') as outputFile:
            outputFile.write(headerText)

def isIP(ip):
    validIpAddressRegex = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"    
    return re.match(validIpAddressRegex, ip)

def query(line, outputFile):
    strippedLine = line.strip()

    if  isIP(strippedLine):
        print ("IP needs to be converted to DNS")
        try:
            currentDNS = socket.gethostbyaddr(strippedLine)[0]
            outputFile.write(currentDNS + "," + strippedLine + ",NO_DOMAIN," + formattedDateWithSeconds(datetime.now()) + "\n")
        except:
            print ("Cant resolve")
    else:
        # print ("DNS, need to resolve all ips")
        currentIPs = list( map( lambda x: x[4][0], socket.getaddrinfo( \
        line.strip(),22,type=socket.SOCK_STREAM)))

        for currentIP in currentIPs:
            try:
                originDomain = strippedLine
                recurDomain = socket.gethostbyaddr(currentIP)[0]
                print (recurDomain)
                outputFile.write(originDomain + "," + currentIP + "," + recurDomain + "," + formattedDateWithSeconds(datetime.now()) + "\n")
            except:
                 print("Can't resolve domain to ip: " + currentIP)
                 outputFile.write(originDomain + "," + currentIP + ",NO_DOMAIN," + formattedDateWithSeconds(datetime.now()) + "\n")

def runFile():
    def wrapperTargetFunc(workerFunction, q, outputFile):
        while not q.empty():
            try:
                work = q.get(timeout=3)  # or whatever
            except queue.Empty:
                print("Error getting url from queue")
            workerFunction(work, outputFile)
            q.task_done()

    lines = [] #reset the input array

    with open(outputFileName, 'a+') as outputFile:
        #Read lines from input file
        with open(inputFileName, 'r') as listFile:
            for line in listFile:
                lines.append(line)
            listFile.close()

        threadQueue = queue.Queue()
        for line in lines:
            threadQueue.put_nowait(line)

        for _ in range(numberOfThreads):
            threading.Thread(target=wrapperTargetFunc,
                            args=(query, threadQueue, outputFile)).start()
        threadQueue.join()
        stop = datetime.now()
        outputFile.close()
        print('Time: ', stop - timeSinceLastZip)

timeSinceLastZip = datetime.now()
addHeaderIfNeeded()
while True:
    runFile()
    if (datetime.now() - timeSinceLastZip > timedelta(minutes=minutesToZipFile)):
        zipFile(outputFileName)
        os.remove(outputFileName)
        timeSinceLastZip = datetime.now()
        
    print("Sleeping ", minutesToSleep, " minutes...")
    time.sleep(minutesToSleep * 60)