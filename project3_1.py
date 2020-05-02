import csv
import json
import pymongo
import pandas as pd


# used for inserting csv file into mongo db
# mongoimport --type csv -d test -c products --headerline --drop amazon_co-ecommerce_sample.csv
# mongoimport --type csv -d test -c amazon1 --headerline --drop 7817_1.csv
# mongoimport --type csv -d test -c amazon2 --headerline --drop Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products.csv
# mongoimport --type csv -d test -c amazon3 --headerline --drop Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products_May19.csv
def cursor_print(cursor):
    for document in cursor:
        print(document)


cl = pymongo.MongoClient()
db = cl.test
print("Collections in the respective database test")
print(db.list_collection_names())
collection = db.products


def product_info():
    print("****************** product name retrives the document just displaying its unique id and name ************")
    name = input('enter product name')
    cursor = collection.find({"product_name": name}, {'_id': 1, 'product_name': 1})
    print()
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")
    print()


def manufacture_info():
    print("***************** given manufacture name displays the  products under it *********************************")
    mname = input('enter manufacture name')
    cursor = collection.find({'manufacturer': mname}, {'product_name': 1, "_id": 0}).limit(10)
    print()
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")
    print()


def product_ratings():
    print("**************** displaying the product name if it is in below ratings ***********************************")
    cursor = collection.find({"average_review_rating": {
        "$in": ['4.5 out of 5 stars', '4.6 out of 5 stars', '4.7 out of 5 stars', '4.8 out of 5 stars',
                '4.9 out of 5 stars']}}, {'product_name': 1, 'average_review_rating': 1, "_id": 0}).limit(10)
    print()
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")


def rows_products():
    print("********* displays the count of documents each product has in the collection grouping by product name ****")
    cursor = db.amazon3.aggregate([
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$limit": 10}
    ])
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")
    print()


def userreviews():
    print("*******************number of reviews of each customer ****************************************************")
    cursor = db.amazon1.aggregate([{"$group": {"_id": "$reviews.username", "count": {"$sum": 1}}}, {"$limit": 10}])
    print()
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")
    print()


def maxcust():
    print("********************** name of the customer who gave maximum reviews overall *****************************s")
    cursor = db.amazon3.aggregate([
        {"$group": {"_id": "$reviews.username", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ])
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")
    print()


def maxproduct():
    print("************************** product with maximum ratings **************************************************")
    print()
    cursor = db.amazon3.aggregate(
        [{"$unwind": "$reviews"}, {"$group": {"_id": {'name': "$name", 'ratings': "$ratings"}, "count": {"$sum": 1}}},
         {"$sort": {"count": -1}},
         {"$limit": 1}
         ])
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")
    print()


def joinquery():
    print("********************************join query result ********************************************************")
    print()
    cursor = db.trial.aggregate([
        {
            "$lookup":
                {
                    "from": "trial2",
                    "localField": "product name",
                    "foreignField": "product name",
                    "as": "test_docs"
                }
        },
        {"$limit": 1}
    ])
    cursor_print(cursor)
    print()
    print("**********************************************************************************************************")


print("**********************************************************************************************************")
print("*************Menu************")
print("1.given product name returns document")
print("2.given manufacture name returns the products under it")
print("3.number of reviews per customer")
print("4.customer who gave maximum reviews")
print("5.product with maximum ratings")
print("6.number of documents stored per product")
print("7.join query")
for i in range(9):
    inp = input('enter the number u want')
    if inp == '1':
        product_info()
    elif inp == '2':
        manufacture_info()
    elif inp == '3':
        userreviews()
    elif inp == '4':
        maxcust()
    elif inp == '5':
        maxproduct()
    elif inp == '6':
        rows_products()
    elif inp == '7':
        joinquery()
    elif inp == '0':
        exit()
