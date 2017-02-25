'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                    WORK FLOW EXPLANATION
1. The General Inquirer Dictionary is import, the dictionary contains 
    words of different level of positivity and negativity, upon which 
    the reviews' sentiment is assessed.

2. After the dictionary was imported, the positive and negative words
    of different senses are combined into definitive polars of either
    positivity or negativity.

3. The airline_review csv file which contains the content of the review
    is then imported and read. The reviewcont is then tokenized, stemmed.
    Stop words are removed. Processed words then are written into a list.
    
4. Each word in the list are then compared to positive word list and
    negative word list. The number of negative words and positive words
    in each review are counted.
    
5. The sentiment of the review are calculated by subtract the number of
    negative words from the number of positive words. A negative number
    indicate negative sentiment, a positive number indicates otherwise.
    
6. Logistic Regression analysis is done to find out the dependency of
    recommended (whether the review author is going to recommend the
    airlines) on the sentiment of the review.
    Logistic Regression is chosen, because it's used for categorical
    variable (binary)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import pandas as pd
import csv 
from collections import Counter
import operator

from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords 
from nltk.stem.snowball import SnowballStemmer

# Import negative and positive words from General Inquirer dictionary 
def sentiment_analysis(review): # Input dictionary txt file here 
    positive = []
    negative = []
    
    with open('general_inquirer_dict.txt') as fin:
        reader = csv.DictReader(fin, delimiter = '\t')
        for i, line in enumerate(reader):
            '''Combining positive and negative words of different senses into 
            definitive polars (neg or pos)'''
            if line['Negativ'] == 'Negativ':
                if line['Entry'].find('#') == -1:
                    negative.append(line['Entry'].lower())
                if line['Entry'].find('#') != -1:
                    negative.append(line['Entry'].
                                    lower()[:line['Entry'].index('#')])
                
        if line['Positiv'] == 'Positiv':
            if line['Positiv'].find('#') == -1:
                positive.append(line['Entry'].lower())
            if line['Entry'].find('#') != -1:
                positive.append(line['Entry'].
                                lower()[:line['Entry'].index('#')])

    fin.close()
        
    pvocabulary = sorted(list(set(positive)))
    nvocabulary = sorted(list(set(negative)))                  

    
    # Sort positive and negative word list
       
    
    '''Import airline reviews from csv file'''
    review.columns.values
    review['reviewcontent']
    
    review['poswdcnt'] = 0
    review['negwdcnt'] = 0
    review['lsentiment'] = 0
    review_index = 0
    
    
    ''' Breaking sentences down into individual words (tokenizing)'''
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
        
        #Removing words with only 1 letter
        vocabulary = [word for word in vocabulary if len(word) > 1]
        #Remove stop words
        vocabulary = [word for word in vocabulary
                      if not word in stopwords.words('english')]
        
        # Stemming words, i.e. grouping same words of different tenses or forms 
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
    
    return review

csv_file = 'airline_review.csv'
sentiment_analysis(dictionary, csv_file)

# Run a Logistic regression
import statsmodels.api as sm2
logit = sm2.Logit(sentiment_analysis(dictionary, csv_file)['recommended'], sentiment_analysis(dictionary, csv_file)['lsentiment'])
result = logit.fit()
print(result.summary())
