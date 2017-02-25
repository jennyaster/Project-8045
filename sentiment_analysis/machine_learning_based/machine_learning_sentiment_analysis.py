#REQUIRED PYTHON 2

import nltk 
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import os
from nltk.corpus.reader import CategorizedPlaintextCorpusReader
from nltk.util import ngrams

'''Reviews are first classified by rating_overall'''
import csv

path = 'airline_review_for_ml.csv'

count = 0
count_pos = 0
count_neg = 0

with open(path, 'r') as input:
    review_reader = csv.DictReader(input, delimiter = ",", skipinitialspace = True)
    for count,row in enumerate(review_reader):
        
        filename = "review" + str(count)
        
        if row['rating_overall'] in ("6", "7", "8", "9", "10") and count_pos < 15000:
            output = open('airline_review\\pos\\' + \
                          filename + '.txt', 'w')
            output.write(row['reviewcontent'])
            output.close()
            count_pos += 1
            
        elif row['rating_overall'] in ("1", "2", "3", "4", "5") and count_neg < 15000:
            output = open('airline_review\\neg\\' + \
                          filename + '.txt', 'w')
            output.write(row['reviewcontent'])
            output.close()
            count_neg += 1
        

os.chdir("E:/Documents/GSU/Python Development/Unstructured Data/Team Project/machine_learning_text_analysis")
reader = CategorizedPlaintextCorpusReader('./airline_review', r'.*\.txt',
                                          cat_pattern = r'(\w+)/*') # file name format

# Positive reviews file ids
pos_ids = reader.fileids('pos')

# Negative reviews file ids
neg_ids = reader.fileids('neg')

'''Generating word feature list'''
def word_feats(words):
    return dict([(word, True) for word in words])


'''Building positive and negative feature lists. Each 
item is the positive/negative word features for a review file'''
pos_feat = [(word_feats(reader.words(fileids = f)), 'pos')
            for f in pos_ids]
neg_feat = [(word_feats(reader.words(fileids = f)), 'neg')
            for f in neg_ids]

'''refining feature lists, stemming, removing punctuation and stop words from pos_feat'''
pos_feat = []
import re
for file in pos_ids[:15000]:
    # reset review variable
    review = ''
    
    # Create a string of the text in the file
    review = ' '.join(word for word in 
                     reader.words(fileids = [file]))
    
    # Remove punctuation
    review = re.sub(r'[^\w\s]', '', review)
    
    # Remove word with only 1 letter
    review = " ".join(word for word in review.split()
                      if len(word) > 1
                      )
    
    # Remove stop words
    review = " ".join(
        word for word in review.split()
        if not word in stopwords.words('english')
        )
    
    # Stemming (words in different tenses/forms carry the same meaning)
    stemmer = SnowballStemmer('english')
    review = " ".join(
        [stemmer.stem(word) for word in review.split()])

    # N-gram 
    ngramsize = 2
    if ngramsize > 1:
        review = [word for word in ngrams(
            review.split(), ngramsize)]
        review = (word_feats(review), 'pos')
    else:
        review = (word_feats(review), 'pos')
    
    pos_feat.append(list(review))
    
'''refining feature lists, stemming, removing punctuation and stop words from neg_feat'''
neg_feat = []

for file in neg_ids[:15000]:
    # reset review variable
    review = ''
    
    # Create a string of the text in the file
    review = ' '.join(word for word in 
                     reader.words(fileids = [file]))
    
    # Remove punctuation
    review = re.sub(r'[^\w\s]', '', review)
    
    # Remove word with only 1 letter
    review = " ".join(word for word in review.split()
                      if len(word) > 1
                      )
    
    # Remove stop words
    review = " ".join(
        word for word in review.split()
        if not word in stopwords.words('english')
        )
    
    # Stemming (words in different tenses/forms carry the same meaning)
    stemmer = SnowballStemmer('english')
    review = " ".join(
        [stemmer.stem(word) for word in review.split()])

    # N-gram 
    ngramsize = 2
    if ngramsize > 1:
        review = [word for word in ngrams(
            review.split(), ngramsize)]
        review = (word_feats(review), 'neg')
    else:
        review = (word_feats(review), 'neg')
    
    neg_feat.append(list(review))

# training and testing
'''Using the first 10000 positive word feature items and the first 10000 negative 
word feature items. the remaining 5000 positive and 5000 negative feature items are
used as test dataset'''
train = pos_feat[:10000] + neg_feat[:10000]
test = pos_feat[10000:] + neg_feat[10000:]

# classifying using Naive Bayes Classifier
from nltk.classify import NaiveBayesClassifier
classifier = NaiveBayesClassifier.train(train)

# Creating reference sets and test sets
import collections
refsets = collections.defaultdict(set)
testsets = collections.defaultdict(set)

# Put the predicted class in test set 
result = []
for i,(feats,label) in enumerate(test):
    refsets[label].add(i)
    observed = classifier.classify(feats)
    testsets[observed].add(i)
    result.append(observed)

# Prepare results
all_ids = pos_ids + neg_ids

import pandas as pd
se1 = pd.Series(all_ids)
df = pd.DataFrame(all_ids)
df['File_ids'] = se1
se2 = pd.Series(['pos'] * 15000 + ['neg'] * 15000)
df['Original_Class'] = se2
se3 = pd.Series([''] * 10000 + result[0:5000] + [''] * 10000 + result[5000:10000])

df.to_csv('machine_learning_sentiment_out.csv')

import nltk.metrics
import math 
from nltk.metrics import precision 
from nltk.metrics import recall

''' calculating the precision of the training, with refsets as the original 
data set, and testsets as the predicted dataset'''
pos_precision = precision(refsets['pos'], testsets['pos']) 
neg_precision = precision(refsets['neg'], testsets['neg'])

'''return the fraction of the values in refsets
that appear in the testsets'''
pos_recall = recall(refsets['pos'], testsets['pos'])
neg_recall = recall(refsets['neg'], testsets['neg'])

'''Measuring G-performance (predictive accuracy)
 of the training''' 
gperf = math.sqrt(pos_precision * neg_recall)

print("Positive Precision", pos_precision)
print("Negative Precision", neg_precision)
print("Positive Recall", pos_recall)
print("Negative Recall", neg_recall)
print("G-Performance", gperf)
