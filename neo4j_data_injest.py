import py2neo
from py2neo import Graph
import bson

py2neo.authenticate("localhost:7474", "neo4j", "classroom")
graph = Graph("http://localhost:7474/db/data/")

PATH = "C:\\Users\\Kyle\\Desktop\\Airline\\reviews.bson"

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

# Create relationships between reviewers and airlines
print("Creating Review and Airline Relationships")
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
            AVG(toFloat(r2.rating_cabinstaff)) as r_staff
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
            competitor_avg_review: rrating,
            competitor_avg_inflight_ent: r_inflightent,
            competitor_avg_seatcomfort: r_seatcomfort,
            competitor_avg_valuemoney: r_valuemoney,
            competitor_avg_foodbeverage: r_foodbev,
            competitor_recommended: r_recommend,
            competitor_avg_staff: r_staff
            }]->(a2)"""

print("Linking Airlines to Competitors")
graph.run(query)  #Query to link all airlines with shared reviewers

query = """MATCH (a:Airline)-[u:Competes_with]->(a1)
            WITH a,avg(u.focus_avg_review) as numerator, avg(u.competitor_avg_review) as divisor
            SET a.adjusted_rating = (numerator/divisor)"""
            
graph.run(query)

entertainment_words = ["entertain","IFE","picture","music","headphones","movie","television",
                       "tv","video","wifi","wi-fi","bored","internet"]
seatcomfort_words = ["pain","comfortable","recline","cushion","blanket","soft","seat","chair","comfort",
                     "legroom","leg","sore","neck","room","armrest","tight"]
value_words = ["cost","complimentary","quality","cost","expensive","free","generous","bonus",
               "money","cheap","value","economy","low-cost","price","pay","save"]
food_words = ["catering","food","drink","thirsty","hungry","menu","delicious",
              "water","beverage","snack","meal","breakfast","lunch","dinner","eat","drink",
              "alcohol","beer","wine","cocktail"]
staff_words = ["staff","service","polite","rude","steward","attendant","attentive","caring",
               "impolite","warm","smile","crew","enthusiastic"]

query = """MATCH (r:Review)
            RETURN r._id,r.reviewcontent"""

print("Analyzing User Preferences") 
results = graph.run(query)
user_text_preference = []
for result in results:
    
    user_text_preference.append([result['r._id']])
    
