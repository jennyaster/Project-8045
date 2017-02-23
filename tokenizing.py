import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords 

def getWordList(text, word_proc = lambda x:x):
    word_list = []
    for sent in sent_tokenize(text):
        for word in word_tokenize(sent):
            word_list.append(word)
    return word_list 
    
stemmer = SnowballStemmer('english')

for review in text:
    word_list = getWordList(review, lambda x: x.lower())
    word_list = [word for word in word_list
                 if not word in stopwords.words('english')]
    word_list = [stemmer.stem(word) for word in word_list]
