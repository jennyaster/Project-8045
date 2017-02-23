import pandas as pd
import csv 
from collections import Counter
import operator

from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords 
from nltk.stem.snowball import SnowballStemmer

import sys  

reload(sys)  
sys.setdefaultencoding('utf8')

# Import negative and positive words from General Inquirer dictionary 
positive = []
negative = []

with open("general_inquirer_dict.txt") as fin:
    reader = csv.DictReader(fin, delimiter = '\t')
    for i, line in enumerate(reader):
        if line['Negativ'] == 'Negativ':
            if line['Entry'].find('#') == -1:
                negative.append(line['Entry'].lower())
            if line['Entry'].find('#') != -1:
                negative.append(line['Entry'].lower()[:line['Entry'].index('#')])
                
        if line['Positiv'] == 'Positiv':
            if line['Positiv'].find('#') == -1:
                positive.append(line['Entry'].lower())
            if line['Entry'].find('#') != -1:
                positive.append(line['Entry'].lower()[:line['Entry'].index('#')])

fin.close()                

pvocabulary = sorted(list(set(positive)))
nvocabulary = sorted(list(set(negative)))

# Import review data
review = pd.read_csv('airline_review.csv')
review.columns.values
review['reviewcontent']

review['poswdcnt'] = 0
review['negwdcnt'] = 0
review['lsentiment'] = 0
review_index = 0

# Tokenizing text and write to list
def getWordList(text, word_proc = lambda x:x):
    word_list = []
    for sent in sent_tokenize(text):
        for word in word_tokenize(sent):
            word_list.append(word)
    return word_list 

stemmer = SnowballStemmer('english')

pcount_list = []
ncount_list = []
lsenti_list = []
for text in review['reviewcontent']:
    vocabulary = getWordList(text, lambda x:x.lower())
    
    vocabulary = [word for word in vocabulary if len(word) > 1]
    #Remove stop word
    vocabulary = [word for word in vocabulary
                  if not word in stopwords.words('english')]
    
    vocabulary= [stemmer.stem(word) for word in vocabulary]
    
    
    
    
    pcount = 0
    ncount = 0
    
    for pword in pvocabulary:
        pcount += vocabulary.count(pword)
    for nword in nvocabulary:
        ncount += vocabulary.count(nword)
    
    pcount_list.append(pcount)
    ncount_list.append(ncount)
    lsenti_list.append(pcount - ncount)
        
    review.loc[review_index, 'poswdcnt'] = pcount 
    review.loc[review_index, 'negwdcnt'] = ncount 
    review.loc[review_index, 'lsentiment'] = pcount - ncount 
    
    review_index += 1

se = pd.Series(pcount_list)
review['poswdcnt'] = se 
se = pd.Series(ncount_list)
review['negwdcnt'] = se
se = pd.Series(lsenti_list)
review['lsentiment'] = se 

review.to_csv('airline_review_out.csv')

# Run OLS regression
import statsmodels.formula.api as sm
results = sm.ols(formula = "recommended~lsentiment", data = review).fit()
print(results.summary())
