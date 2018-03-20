#!/usr/bin/env python

import sys
import os
import sqlite3
import hashlib
import datetime

def main():
    
    # Define default path as current directory
    PATH = "./"

    # Check for commnand line arguments
    if len(sys.argv) > 2:
        sys.exit("\nUsage: checkup (options: [path] | purge | help)\n")

    if len(sys.argv) == 2:
        if sys.argv[1] == "purge":
            purge()
            sys.exit(0)
        elif sys.argv[1] == "help":
            print("\nUsage\tcheckup (options: [path] | purge | help)")
            print("[path]\tDirectory to be checked. Default is the current directory")
            print('purge\tRemove all entries marked as "missing" from the database')
            print("help\tThis text file\n")
            sys.exit(0)
        else:
            PATH = sys.argv[1]

    # Ensure path is valid
    if not os.path.isdir(PATH):
        sys.exit(f"Directory: {PATH} not found.")

    # Open the database as conn and cursor as db. Create database if it doesn't exist
    try:
        conn = sqlite3.connect("./checkup.db")
        db = conn.cursor()
    except:
        print("An error occured connecting to checkup.db")

    # Create the table if one doesn't exist
    db.execute("""CREATE TABLE IF NOT EXISTS 'checkup' 
              ('filename' TEXT,'hash' INTEGER, 'flag' TEXT, 'last_checked' DATETIME DEFAULT CURRENT_TIMESTAMP)""")

    # Compare datebase against files found
    check_for_missing_files()

    # Open database. Database has: filename, hash, flag (new, mismatch, missing, ok), and date stamp
    for root, dirs, files in os.walk(PATH, topdown=True):

        for file in files:

            # Add full path to file name
            file = os.path.join(root,file)
            print(f"{file:100}", end="\r")

            # Get hash
            new_hash = get_hash(file)

            # Check to see if file is already in the database
            db.execute("SELECT * FROM checkup WHERE filename=:file", {'file': file})
            current_file = db.fetchone()

            # If the file is NOT in the database, hash the file and add it to the database
            if not current_file:
                flag = "new"
                with conn:
                    db.execute("INSERT INTO checkup ('filename', 'hash', 'flag') VALUES (:filename, :hash, :flag)",
                               {'filename':file, 'hash':new_hash, 'flag':flag})

            # The file IS in the database, so compare current hash with hash on file
            else:
                if current_file[1] == new_hash:
                    with conn:
                        db.execute("UPDATE checkup SET flag=:flag WHERE filename=:file",
                                   {'file':file, 'flag':"ok"})
                else:
                    with conn:
                        db.execute("UPDATE checkup SET hash=:hash, flag=:flag WHERE filename=:file",
                                   {'file':file, 'hash':new_hash, 'flag':"mismatch"})

    print("\n")

    # Report files that are new, missing, or have been changed
    checkup_log()

    # Close the database
    conn.close()


def check_for_missing_files():
    """Checks to see if the files in the database still exist"""

    # Open database
    conn = sqlite3.connect("checkup.db")
    db = conn.cursor()

    db.execute("SELECT filename FROM checkup")
    file_list = db.fetchall()

    for item in file_list:
        print(f"{item[0]:100}", end="\r")

        if not os.path.isfile(item[0]):
            with conn:
                db.execute("UPDATE checkup SET flag=:flag WHERE filename=:file",
                           {'file': item[0], 'flag': 'missing'})
    
    # Close the database file
    conn.close()


def get_hash(file):
    """Creates an sha256 hash for the specified file"""

    sha_hash = hashlib.sha256()

    with open(file,"rb") as in_file:
        for block in iter(lambda: in_file.read(4096),b""):
            sha_hash.update(block)

    return(sha_hash.hexdigest())


def checkup_log():
    """Writes new, missing, and changed files to checkup.log"""

    # Open database
    conn = sqlite3.connect("checkup.db")
    db = conn.cursor()

    # Log add time stamp and log any newly discovered files
    results = datetime.datetime.now().strftime("%m-%d-%Y %H:%M\n\nNew Files: ")

    # Return count of matching entries
    db.execute("SELECT count() from checkup WHERE flag=:flag", {'flag':"new"})
    count = db.fetchall()
    results += f"{count[0][0]}\n"

    # List the files if count > 0
    if count[0][0] > 0:
        db.execute("SELECT * FROM checkup WHERE flag=='new'")
        new_files = db.fetchall()
        for item in new_files:
            results += f"{item[0]}\n"

    # Log any files in which the SHA256 hash doesn't match
    results += "\nChecksum Errors: "

    # Return count of matching entries
    db.execute("SELECT count() from checkup WHERE flag=:flag", {'flag':"mismatch"})
    count = db.fetchall()
    results += f"{count[0][0]}\n"

    # List the files if count > 0
    if count[0][0] > 0:
        db.execute("SELECT * FROM checkup WHERE flag=='mismatch'")
        errors = db.fetchall()
        for item in errors:
            results += f"{item[0]}\n"

    # Log any files that are missing from the directory
    results += "\nMissing Files: "

    # Return count of matching entries
    db.execute("SELECT count() from checkup WHERE flag=:flag", {'flag':"missing"})
    count = db.fetchall()
    results += f"{count[0][0]}\n"

    # List the files if count > 0
    if count[0][0] > 0:
        db.execute("SELECT * FROM checkup WHERE flag=='missing'")
        missing_files = db.fetchall()
        for item in missing_files:
            results += f"{item[0]}\n"

    results += f"\n{'=' * 50}\n"

    # Write results to the log and screen
    try:
        log = open("checkup.log", "a+")
        log.write(results)
        log.close()
    except:
        print("Unable to update checkup.log")
 
    print(results)

    # Close database
    conn.close()


def purge():
    # open the database as db
    conn = sqlite3.connect('checkup.db')
    db = conn.cursor()

    # Get a count of entries where flag == missing
    with conn:
        db.execute("SELECT count() from checkup WHERE flag=:flag", {'flag':"missing"})
        count = db.fetchall()

        # If entries marked as missing are found, delete them from the database
        if count[0][0] > 0:
            db.execute("DELETE from checkup WHERE flag=:flag", {'flag':"missing"})
            print(f"{count[0][0]} entries deleted")
        else:    
            print("No missing files found")

    # Close the database
    conn.close()


if __name__ == "__main__":
    main()
