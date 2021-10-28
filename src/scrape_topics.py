import requests, bs4

# get HTML from internet
res = requests.get('http://nostarch.com')
res.raise_for_status()
noStarchSoup = bs4.BeautifulSoup(res.text)
type(noStarchSoup)

# get HTML from disk
exampleFile = open('C:/Users/wb256280/Dropbox/alexei_roy_shared_folder/python_examples/example.html')
exampleSoup = bs4.BeautifulSoup(exampleFile.read())
type(exampleSoup)

elems = exampleSoup.select('#author')
type(elems)
len(elems)
type(elems[0])
elems[0].getText()
str(elems[0])
elems[0].attrs

pElems = exampleSoup.select('p')
pElems[0].getText()
pElems[1].getText()
pElems[2].getText()

spanElem = exampleSoup.select('span')[0]
str(spanElem)
spanElem.get('id')
spanElem.attrs


####################

#topicsFile = open('C:/Users/wb256280/Dropbox/alexei_roy_shared_folder/topics/topics.html')
#topicsSoup = bs4.BeautifulSoup(topicsFile.read())

top = requests.get('https://www.isidewith.com/polls')
top.raise_for_status()
topSoup = bs4.BeautifulSoup(top.text)

#polls = topSoup.select('div.poll')
polls_question = topSoup.select('div.poll p.question')
polls_question[4].getText()

polls_a = topSoup.select('div.poll a')
polls_title=[x.get('title') for x in polls_a]
len(polls_title)
polls_title[0]

