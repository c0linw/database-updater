# -*- coding: utf-8 -*-

import os
import sys
import hashlib
import getpass
import re
import mysql.connector

BLOCK_SIZE = 65536 # Higher block size allows faster hashing, but more memory usage
word_cache = set()

def get_file_hash(file: str):
    file_hash = hashlib.sha256()
    with open(file, 'rb') as f: 
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            file_hash.update(fb) # Update hash for the current block
            fb = f.read(BLOCK_SIZE)
    return file_hash

def init_word_cache(file: str):
    global word_cache
    word_cache = set()
    with open(file, 'r') as f:
        for line in f:
            word_cache.add(line.rstrip())
            
def update_word_cache(file: str) -> list:
    '''Updates the word cache and returns any new words added to the set'''
    global word_cache
    new_cache = set()
    with open(file, 'r') as f:
        for line in f:
            new_cache.add(line.rstrip())
        diff = new_cache.difference(word_cache)
        word_cache = new_cache
    return diff

def get_any_word_in_set(data: str, words: set) -> str:
    '''Returns a word in word_cache that is found in the input data.
    If there is more than one word satisfying this criteria, a random one is returned.'''
    
    for word in words:
        # account for chinese characters counting as non-word characters
        if (not word.isascii()) and (word in data):
            return word
        # only evaluate word boundaries for ascii words
        elif re.match(r"\b" + re.escape(word) + r"\b", data):
            return word
    return ""

def delete_contract_calls_by_ids(ids: list, db: any):
    count = 0
    for contract_id in ids:
        sql = "DELETE FROM `t_contract_call` WHERE `id` = %s"
        db_cursor = db.cursor()
        db_cursor.execute(sql, (contract_id,))
        count += 1
        db.commit()
    
    print("{0} row(s) deleted in table 't_contract_call'".format(count))

def update_tx_outs_by_ids(ids: list, db: any):
    count = 0
    for tx_id in ids:
        sql = "UPDATE `t_tx_out` SET `contract` = '' WHERE `id` = %s"
        db_cursor = db.cursor()
        db_cursor.execute(sql, (tx_id,))
        count += 1
        db.commit()
    
    print("{0} row(s) updated in table 't_tx_out'".format(count))

def update_for_new_words(words: list, db: any):
    # delete any contracts with sensitive words
    sql = "SELECT * FROM t_contract_call"
    db_cursor = db.cursor()
    db_cursor.execute(sql)
    ids_to_delete = []
    for row in db_cursor:
        # check the data for sensitive words
        detected_word = get_any_word_in_set(row[6], words)
        if detected_word != "":
            ids_to_delete.append(row[0])
    if len(ids_to_delete) > 0:
        delete_contract_calls_by_ids(ids_to_delete, db)
        
    # update any tx outs with sensitive words
    sql = "SELECT * FROM t_tx_out"
    db_cursor = db.cursor()
    db_cursor.execute(sql)
    ids_to_delete = []
    for row in db_cursor:
        # check the data for sensitive words
        detected_word = get_any_word_in_set(row[4], words)
        if detected_word != "":
            ids_to_delete.append(row[0])
    if len(ids_to_delete) > 0:
        update_tx_outs_by_ids(ids_to_delete, db)
    

### MAIN EXECUTION BEGINS HERE ###

db_hostname = input("Enter the hostname of the SQL database: ")
db_database = input("Enter the name of the database: ")
db_user = input("Enter your username for the database: ")
db_password = getpass.getpass("Enter your password for the database: ")
file_path = input("Enter the filepath for the sensitive words list: ")

print("Connecting to database...")
db = mysql.connector.connect(
       host=db_hostname,
       user=db_user,
       password=db_password,
       database=db_database
   )

# check all words on startup
print("Performing initial check")
try:
   last_modified = os.path.getmtime(file_path)
   prev_hash = get_file_hash(file_path)
   init_word_cache(file_path)
   update_for_new_words(word_cache, db)
except FileNotFoundError as err:
    print("File error: {0}".format(err))
    exit()
    
print("Initial check complete, listening for file updates...")
    
while(1):
    try:
        # compare modification date first to avoid unnecessary hash calculations
        update_modified = os.path.getmtime(file_path)
        if update_modified != last_modified:
            last_modified = update_modified
            # compare hash to see if it actually changed
            curr_hash = get_file_hash(file_path)
            if curr_hash.digest() != prev_hash.digest(): 
                prev_hash = curr_hash
                print("Sensitive word list updated")
                new_words = update_word_cache(file_path)
                if len(new_words) > 0:
                    update_for_new_words(new_words, db)
    except FileNotFoundError as err:
        print("File error: {0}".format(err))
        break
    except KeyboardInterrupt:
        print("Program ended by keyboard interrupt")
        break
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise