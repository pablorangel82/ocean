from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from PIL import Image
import traceback
import threading
import datetime
import logging
import codecs
import math
import os
import re
import json

FILE_TO_DOWNLOAD_FROM = "VesselClassification.dat"

NUMBER_OF_WORKERS = 10
MAX_NUM_OF_FILES_IN_FOLDER = 5000
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640
ORIGINAL_SIZE = 0 # 1 for yes, 0 for no
JUST_IMAGE = 0 # 1 for yes, 0 for no

sourceLink = "https://www.shipspotting.com/photos/"

logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-10s) %(message)s', )
logging.debug("Process started at " + str(datetime.datetime.now()))

def save_image(url,path,file):
    print("Getting from: " + url)
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    con = urlopen(req, timeout=300)
    print("Saving image at: " + path)
    if not os.path.exists(path):
        os.mkdir(path)
    path = os.path.join(path, file)
    with open(path, "wb") as local_file:
        local_file.write(con.read())
    if ORIGINAL_SIZE == 0:
        img = Image.open(path).resize((IMAGE_HEIGHT, IMAGE_WIDTH), Image.LANCZOS)
        os.remove(path)
        out = open(path, "wb")
        img.save(out, "JPEG")

def save_data(ID, justImage, outFolder):
    _id = int(re.sub('[^0-9]', '', ID))
    url = sourceLink + str(_id)
    print(url)
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    con = urlopen(req, timeout=300)
    html=con.read()
    soup = BeautifulSoup(html, "lxml")
    values = None
    images = [img for img in soup.findAll('img')]
    image_links = [each.get('src') for each in images]
    if not justImage:
        tags = [tg for tg in soup.findAll('script')]
        scripts = [each.getText() for each in tags]
        for each in scripts:
            if each is None:
                continue
            start = each.find("window._INITIAL_DATA")
            if start != -1:
                start = each.find("=",start,len(each)) +1
                end = each.rfind(";",start,len(each))
                json_value = each[start:end]
                values  = json.loads(json_value)
                break
                
    filename = " "
    for img in image_links:
        if img is None:
            continue
        if "http" in img and "jpg" in img and "photos/middle" in img:
            filename = img.split('/')[-1]
            path = f'%s' % os.path.join(outFolder, filename.split('.')[0])
            url = img.split('?')[0]
            save_image(url,path, filename.split('.')[0]+'.jpg')
            break

    if filename != " " and not justImage and values is not None:
        path = f'%s' % os.path.join(outFolder, filename.split('.')[0])
        path_meta_data = os.path.join(path, filename.split('.')[0]+'.json')
        print("Saving metadata at: " + path)
        tFile = codecs.open(path_meta_data, 'w', 'utf-8')
        page_data = values['page_data']
        ship_data = page_data['ship_data']
        tFile.write(json.dumps(ship_data) + '\n')
        tFile.close()
        more = page_data ['more_of_this_ship']
        photos = more['items']
        for photo in photos:
            lid = str(photo['lid'])
            url = sourceLink + 'big/' + lid[len(lid)-1] + '/' + lid[len(lid)-2] + '/' + lid[len(lid)-3]+'/'+lid+'.jpg'
            save_image(url,path,lid+'.jpg')
        
    if filename == " ":
        return 0
    else:
        return 1


def worker(content, workerNo):
    workerIndex = 0
    folderIndex = 0
    folderNo = 1
    currFolder = os.path.join(os.getcwd(), 'W'+str(workerNo)+'_'+str(folderNo))
    if not os.path.exists(currFolder):
        os.mkdir(currFolder)
    for ID in content:
        if folderIndex == MAX_NUM_OF_FILES_IN_FOLDER:
            folderIndex = 0
            folderNo = folderNo + 1
            currFolder = os.path.join(os.getcwd(), 'W'+str(workerNo)+'_'+str(folderNo))
            if not os.path.exists(currFolder):
                os.mkdir(currFolder)
        try:
            status = save_data(ID, JUST_IMAGE, currFolder)
            workerIndex = workerIndex + 1
            if status == 1:
                folderIndex = folderIndex + 1
                logging.debug(str(ID) + "\t - Downloaded... - " + str(workerIndex) + "\t/" + str(len(content)))
            else:
                logging.debug(str(ID) + "\t - NO SUCH FILE  - " + str(workerIndex) + "\t/" + str(len(content)))
        except:
            traceback.print_exc()
    logging.debug(str(datetime.datetime.now()) + "-------------- DONE ")
    return

priorFiles = []
dirs = os.listdir(os.getcwd())
for eachDir in dirs:
    if 'W' in eachDir:
        oldFiles = os.listdir(os.path.join(os.getcwd(),eachDir))
        for eachFile in oldFiles:
            if ".jpg" in eachFile:
                oldID = eachFile.split(".")[0]
                priorFiles.append(oldID)

downloadFile = codecs.open(FILE_TO_DOWNLOAD_FROM, "r", "utf-8")
downloadContent = downloadFile.readlines()
downloadFile.close()
finalContent = []
for index, eachLine in enumerate(downloadContent):
    temp = eachLine.split(',')[0]
    if temp not in priorFiles:
        finalContent.append(temp)

numOfFiles = len(finalContent)

numOfFilesPerEachWorker = [int(math.floor(float(numOfFiles)/NUMBER_OF_WORKERS)) for x in range(0,NUMBER_OF_WORKERS-1)]
numOfFilesPerEachWorker.append(numOfFiles - (NUMBER_OF_WORKERS-1)*int(round(numOfFiles/NUMBER_OF_WORKERS,0)))

logging.debug("There will be %s workers in this download process" % NUMBER_OF_WORKERS)
logging.debug("%s files will be downloaded" % numOfFiles)

threads = []
imageCount = 0
for i in range(0,NUMBER_OF_WORKERS):
    t = threading.Thread(name='Worker'+str(i), target=worker, args=(finalContent[imageCount:imageCount + numOfFilesPerEachWorker[i]],i,))
    imageCount = imageCount + numOfFilesPerEachWorker[i]
    threads.append(t)
    t.start()

flag = True
while flag:
    counter = 0
    for eachT in threads:
        if eachT.is_alive() == False:
            counter = counter + 1
    if counter == NUMBER_OF_WORKERS:
        flag = False

logging.debug(str(datetime.datetime.now()) + " - list all files startes ")
allPaths = []
allIDs = []
dirs = os.listdir(os.getcwd())
for eachDir in dirs:
    if 'W' in eachDir:
        FinalList = os.listdir(os.path.join(os.getcwd(),eachDir))
        for eachFile in FinalList:
            if ".jpg" in eachFile:
                fPath = os.path.join(os.getcwd(),eachDir,eachFile)
                fID = eachFile.split(".")[0]
                allPaths.append(fPath)
                allIDs.append(fID)
logging.debug(str(datetime.datetime.now()) + " - write to disc ")

FINAL = codecs.open("FINAL.dat", "w", "utf-8")
for eachLine in downloadContent:
    tempID = eachLine.split(",")[0]
    try:
        tempIndex = allIDs.index(tempID)
        FINAL.write(eachLine[:-1]+","+str(allPaths[tempIndex])+"\n")
    except:
        FINAL.write(eachLine[:-1]+","+"-\n")
FINAL.close()















                                                                  





