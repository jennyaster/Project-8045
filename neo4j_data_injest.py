import py2neo
from py2neo import Graph
import bson
import pandas as pd
import csv
import nltk
from multiprocess import parallel_by_function
from nltk.stem.snowball import SnowballStemmer
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from statsmodels.graphics.tukeyplot import results
from collections import Counter
import operator

py2neo.authenticate("localhost:7474", "neo4j", "classroom")
graph = Graph("http://localhost:7474/db/data/")

PATH = "E:\\Documents\\GSU\\CIS 8045 Unstructured Data\\Project\\Airline\\reviews.bson"
PATH_DICT = "E:\\Documents\\GSU\\Python Development\\Unstructured Data\\Team Project\\lex_based_text_analysis\\general_inquirer_dict.txt"

def sentiment_analysis(review): # Input dictionary txt file here 
    positive = []
    negative = []
    
    with open(PATH_DICT) as fin:
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

def getWordList(text, word_proc = lambda x:x):
    word_list = []
    for sent in sent_tokenize(text):
        for word in word_tokenize(sent):
            word_list.append(word)
    return word_list 

def analyze_text(results):
    user_text_preference = []
    stemmer = SnowballStemmer('english')
    
    entertainment_words = ["entertain","IFE","picture","music","headphones","movie","television",
                           "tv","video","wifi","wi-fi","bored","internet","entertainment"]
    seatcomfort_words = ["pain","comfortable","recline","cushion","blanket","soft","seat","chair","comfort",
                         "legroom","leg","sore","neck","room","armrest","tight"]
    value_words = ["cost","complimentary","quality","cost","expensive","free","generous","bonus",
                   "money","cheap","value","economy","low-cost","price","pay","save"]
    food_words = ["catering","food","drink","thirsty","hungry","menu","delicious",
                  "water","beverage","snack","meal","breakfast","lunch","dinner","eat","drink",
                  "alcohol","beer","wine","cocktail","salad","pasta","butter","pizza","meat","cracker"]
    staff_words = ["staff","service","polite","rude","steward","attendant","attentive","caring",
                   "impolite","warm","smile","crew","enthusiastic"]
    
    for result in results:
        string = result[1]
        
        word_list = getWordList(string, lambda x: x.lower())
        word_list = [word for word in word_list if not word in stopwords.words('english')]
        word_list = [stemmer.stem(word) for word in word_list]
        
        entertain_count = 0
        seat_count = 0
        value_count = 0
        food_count = 0
        staff_count = 0
        
        
        for word in word_list:
            if word in entertainment_words:
                entertain_count += 1
            elif word in seatcomfort_words:
                seat_count += 1
            elif word in value_words:
                value_count += 1
            elif word in food_words:
                food_count += 1
            elif word in staff_words:
                staff_count += 1
        
        grand_total = entertain_count + seat_count + value_count + food_count + staff_count
        if grand_total != 0:
            entertain_count /= grand_total
            seat_count /= grand_total
            value_count /= grand_total
            food_count /= grand_total
            staff_count /= grand_total
        else:
            entertain_count = 1/5
            seat_count = 1/5
            value_count = 1/5
            food_count = 1/5
            staff_count = 1/5
        
        user_text_preference.append([result[0],entertain_count,seat_count,value_count,food_count,staff_count])
        
    return user_text_preference

def upload_text_preference(list_):
    transaction = graph.begin()
    
    for entry in list_:
        id_ = entry[0]
        entertain = entry[1]
        seat = entry[2]
        value = entry[3]
        food = entry[4]
        staff = entry[5]
        
        query = """Match (r:Review {_id:'""" + str(id_) + """'})
                    SET r.entertain_imp = '""" + str(entertain) + """',r.seat_imp = '""" + str(seat) + """',
                    r.value_imp = '""" + str(value) + """', r.food_imp = '""" + str(food) + """', r.staff_imp = '""" + str(staff) + """'"""
        
        transaction.run(query)
    
    transaction.commit()

def load_database():
    ## Open BSON and decode it into a dictionary.
    with open(PATH, "rb") as data:
        decoded_doc = bson.decode_all(data.read())
        
    ## Upload BSON to NEO4j
    ## This step iterates through the bson once to create a unique author node for each user.
    ## It then iterates through the bson again to generate a review with the information
    ## and a relationship back to the authoring node.
    transaction = graph.begin()
    
    airline_list = []  #Used to figure out what airline nodes need to be created.
    user_list = []  #Used to figure out what authors need to be created.
    
    # First loop to find authors.
    print("Creating Author Nodes")
    for document in decoded_doc:
        
        for key,value in document.items():
            if key == "authorname":
                user_list.append(str(value).replace("'",""))
    
    user_list = list(set(user_list)) # Keep unique authors 
    
    for user in user_list:
        query = """CREATE (a:Author {authorname: '""" + str(user) + """'})"""
        transaction.run(query)
        
    transaction.commit() #commit queries to create the authors.
    graph.run("""CREATE CONSTRAINT ON (a:Author) ASSERT a.authorname IS UNIQUE""") #unique authors and index for speed.
    
    print("Creating Review Nodes")
    transaction = graph.begin()
    for document in decoded_doc:
        
        query = """CREATE (r:Review {"""
        match = """MATCH (a:Author {authorname: '"""
    
        for key,value in document.items():
            if key == "airlinename":
                airline_list.append([value,document['_id']]) #Extracts the airlines from the bson data
            elif key == 'authorname':
                match += str(value).replace("'","") + """'})""" #Added to the match part of the query
            else:
                string = key + ":'" + str(value).replace("'", "") + "'," #Creates the CREATE statements from the bson data
                query += string
        query = query[:-1] + "})"
        query = match + query + """WITH a,r CREATE (a)-[:Wrote]->(r)"""
        
        transaction.run(query)
    
    transaction.commit() #Uploads the reviews with a relationship to their authors.
    print("Creating Airline Nodes")
    transaction = graph.begin()
    
    ## Upload the unique airline names
    unique_airlines = []
    for match in airline_list:
        unique_airlines.append(match[0])
    
    unique_airlines = list(set(unique_airlines))
    
    for airline in unique_airlines:
            query = """CREATE (a:Airline {airline_name:'""" + airline + """'})"""
            transaction.run(query)
        
    transaction.commit() #Uploads the airlines
    
    # Create constraint and indexes for integrity/faster queries
    graph.run("""CREATE CONSTRAINT ON (r:Review) ASSERT r._id IS UNIQUE""")
    graph.run("""CREATE CONSTRAINT ON (a:Airline) ASSERT a.airline_name IS UNIQUE""")
    
    transaction = graph.begin()
    
    print("Running Sentiment Analysis")
    query = """MATCH (r:Review)
                RETURN r._id,r.reviewcontent,r.recommended"""
    
    results = graph.run(query)
    labels = ["_id","reviewcontent","recommended"]
    rows1 = []
    rows2 = []
    rows3 = []
    rows4 = []
    for index,result in enumerate(results):
        row = [result["r._id"],result["r.reviewcontent"],result["r.recommended"]]
        if index < 10000:
            rows1.append(row)
        elif index < 20000:
            rows2.append(row)
        elif index < 30000:
            rows3.append(row)
        else:
            rows4.append(row)
    dataframe1 = pd.DataFrame.from_records(rows1, columns=labels)
    dataframe2 = pd.DataFrame.from_records(rows2, columns=labels)
    dataframe3 = pd.DataFrame.from_records(rows3, columns=labels)
    dataframe4 = pd.DataFrame.from_records(rows4, columns=labels)
    
    functions = [sentiment_analysis]
    data_sets = [dataframe1,dataframe2,dataframe3,dataframe4]
    data_sets = parallel_by_function(data_sets, functions, cores=4, chunk=False)
    
    dataframe1 = data_sets[0]
    dataframe2 = data_sets[1]
    dataframe3 = data_sets[2]
    dataframe4 = data_sets[3]
    
    transaction = graph.begin()
    for index,row in dataframe1.iterrows():
        
        query = """MATCH (r:Review {_id: '""" + row["_id"] + """'})
                        SET r.sentiment = '""" + str(row["lsentiment"]) + "'"
                
        transaction.run(query)
    transaction.commit()
    
    transaction = graph.begin()
    for index,row in dataframe2.iterrows():
        
        query = """MATCH (r:Review {_id: '""" + row["_id"] + """'})
                        SET r.sentiment = '""" + str(row["lsentiment"]) + "'"
                
        transaction.run(query)
    transaction.commit()
    
    transaction = graph.begin()
    for index,row in dataframe3.iterrows():
        
        query = """MATCH (r:Review {_id: '""" + row["_id"] + """'})
                        SET r.sentiment = '""" + str(row["lsentiment"]) + "'"
                
        transaction.run(query)
    transaction.commit()
    
    transaction = graph.begin()
    for index,row in dataframe4.iterrows():
        
        query = """MATCH (r:Review {_id: '""" + row["_id"] + """'})
                        SET r.sentiment = '""" + str(row["lsentiment"]) + "'"
                
        transaction.run(query)
    transaction.commit()
    
    # Create relationships between reviewers and airlines
    print("Creating Review and Airline Relationships")
    transaction = graph.begin()
    
    for relationship in airline_list:
        
        airline = relationship[0]
        userid = relationship[1]
        query = """Match (a:Airline {airline_name:'""" + airline + """'}), (r:Review {_id: '""" + str(userid) + """'})
                    CREATE UNIQUE (a)<-[:Reviewed]-(r)"""
        
        transaction.run(query)
        
    transaction.commit() #Upload relationship between the reviewer and the airlines
    
    
    query = """Match p=((a:Airline)<-[]-(r1)<-[]-()-[]->(r2)-[]->(a2:Airline))
                WHERE NOT a = a2
                WITH COUNT(DISTINCT p) as total,a,a2,
                AVG(toFloat(r2.rating_overall)) as rrating,
                AVG(toFloat(r1.rating_overall)) as lrating,
                AVG(toFloat(r2.rating_inflightEnt)) as r_inflightent,
                AVG(toFloat(r1.rating_inflightEnt)) as l_inflightent,
                AVG(toFloat(r1.rating_seatcomfort)) as l_seatcomfort,
                AVG(toFloat(r2.rating_seatcomfort)) as r_seatcomfort,
                AVG(toFloat(r1.rating_valuemoney)) as l_valuemoney,
                AVG(toFloat(r2.rating_valuemoney)) as r_valuemoney,
                AVG(toFloat(r2.rating_foodbeverage)) as r_foodbev,
                AVG(toFloat(r1.rating_foodbeverage)) as l_foodbev,
                SUM(toFloat(r1.recommended))/COUNT(r1.recommended) as l_recommend,
                SUM(toFloat(r2.recommended))/COUNT(r2.recommended) as r_recommend,
                AVG(toFloat(r1.rating_cabinstaff)) as l_staff,
                AVG(toFloat(r2.rating_cabinstaff)) as r_staff,
                AVG(toFloat(r1.sentiment)) as l_sentiment,
                AVG(toFloat(r2.sentiment)) as r_sentiment
                WHERE NOT rrating IS NULL AND NOT lrating IS NULL
                CREATE UNIQUE (a)-[:Competes_with {
                shared_reviewers: total,
                focus_avg_review: lrating,
                focus_avg_inflight_ent: l_inflightent,
                focus_avg_seatcomfort: l_seatcomfort,
                focus_avg_valuemoney: l_valuemoney,
                focus_avg_foodbeverage: l_foodbev,
                focus_recommended: l_recommend,
                focus_avg_staff: l_staff,
                focus_avg_sentiment: l_sentiment,
                competitor_avg_review: rrating,
                competitor_avg_inflight_ent: r_inflightent,
                competitor_avg_seatcomfort: r_seatcomfort,
                competitor_avg_valuemoney: r_valuemoney,
                competitor_avg_foodbeverage: r_foodbev,
                competitor_recommended: r_recommend,
                competitor_avg_staff: r_staff,
                competitor_avg_sentiment: r_sentiment
                }]->(a2)"""
    
    print("Linking Airlines to Competitors")
    graph.run(query)  #Query to link all airlines with shared reviewers
    
    query = """MATCH (a:Airline)-[u:Competes_with]->(a1)
                WITH a,avg(u.focus_avg_review) as numerator, avg(u.competitor_avg_review) as divisor
                SET a.adjusted_rating = (numerator/divisor)"""
                
    graph.run(query)
    
    query = """MATCH (r:Review)
                RETURN r._id,r.reviewcontent"""
    
    print("Analyzing User Preferences") 
    result = graph.run(query)
    results = []
    for record in result:
        results.append([record['r._id'],record['r.reviewcontent']])
    
    functions = [analyze_text,upload_text_preference]
    data_sets = [results]
    parallel_by_function(data_sets, functions, cores=4, chunk=True)
    
    print("Identifying Main Airline Competitors")
    query = """MATCH (a:Airline)-[r:Competes_with]-(c:Airline)
                WITH a,max(r.shared_reviewers) as total
                MATCH (a)-[r {shared_reviewers: total}]->(c:Airline)
                WHERE total >= 5
                RETURN a.airline_name,c.airline_name
                ORDER BY a.airline_name"""
    
    results = graph.run(query)
    airline_list = []
    airlines = []
    for index1,result in enumerate(results):
        airline = result['a.airline_name']
        competitor = result['c.airline_name']
        if index1 == 0:
            airline_list.append([airline,[competitor]])
            airlines.append(airline)
        else:
            if airline in airlines:
                airline_list[airlines.index(airline)][1].append(competitor)
            else:
                airline_list.append([airline,[competitor]])
                airlines.append(airline)
    
    transaction = graph.begin()
    
    for airline in airline_list:
        count=1
        for competitor in airline[1]:
            query = """MATCH (a:Airline {airline_name: '""" + airline[0] + """'})
                    SET a.main_competitor""" + str(count) + """ = '""" + competitor + "'"
            count += 1
            
            transaction.run(query)
    
    transaction.commit()
    
    print("Finding Customer Preferences By Airlines")
    
    query = """MATCH (a:Airline)<-[:Reviewed]-(r:Review)
                with a,avg(toFloat(r.seat_imp)) as seat_imp,avg(toFloat(r.staff_imp)) as staff_imp,avg(toFloat(r.entertain_imp)) 
                as ent_imp, avg(toFloat(r.food_imp)) as food_imp, avg(toFloat(r.value_imp)) as value_imp
                SET a.avg_seat_imp = seat_imp,a.avg_staff_imp = staff_imp, a.avg_entertain_imp = ent_imp,a.avg_food_imp = food_imp,
                a.avg_value_imp = value_imp"""
                
    query = """Match p=((a:Airline)<-[]-(r1)<-[]-()-[]->(r2)-[]->(a2:Airline))
                WHERE NOT a = a2
                WITH a,a2,
                avg(toFloat(r1.seat_imp)) as seat_imp_a,
                avg(toFloat(r2.seat_imp)) as seat_imp_a2,
                avg(toFloat(r1.staff_imp)) as staff_imp_a,
                avg(toFloat(r2.staff_imp)) as staff_imp_a2,
                avg(toFloat(r1.entertain_imp)) as ent_imp_a,
                avg(toFloat(r2.entertain_imp)) as ent_imp_a2,
                avg(toFloat(r1.food_imp)) as food_imp_a,
                avg(toFloat(r2.food_imp)) as food_imp_a2,
                avg(toFloat(r1.value_imp)) as value_imp_a,
                avg(toFloat(r2.value_imp)) as value_imp_a2
                RETURN a,a2,seat_imp_a,seat_imp_a2,staff_imp_a,staff_imp_a2,ent_imp_a,ent_imp_a2,food_imp_a,food_imp_a2,value_imp_a,value_imp_a2"""
    
    graph.run(query)
    
    
    
if __name__ == "__main__":
    load_database()
