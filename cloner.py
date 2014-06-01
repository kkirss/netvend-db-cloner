import urllib2
import mysql.connector
from mysql.connector import errorcode
from netvend import netvend

__version__ = "1.0"
__author__ = "runekri3"

MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_HOST = "127.0.0.1"
MYSQL_DATABASE = "netvend"

AGENT_SEED = "CHANGE THIS"

DATABASE_STRUCTURE_URL = "https://raw.githubusercontent.com/Syriven/NetVend/master/database_structure"


def main():
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, buffered=True)
    c = conn.cursor()
    c.execute("DROP DATABASE IF EXISTS {}".format(MYSQL_DATABASE))
    create_db(c)
    conn.database = MYSQL_DATABASE
    create_tables(c)
    clone(c)
    conn.commit()
    conn.close()
    conn.disconnect()


def create_db(c):
    c.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'latin1'".format(MYSQL_DATABASE))


def create_tables(c):
    database_structure = urllib2.urlopen(DATABASE_STRUCTURE_URL).read()
    for _ in c.execute(database_structure, multi=True):
        pass
        # If multi=True the function returns an iterator, which must be iterated through to have an effect


def clone(c):
    total_charged = 0
    agent = netvend.Agent(AGENT_SEED, seed=True)
    r = agent.query("SHOW TABLES")
    tables = map(list.pop, r["command_result"]["rows"])
    for table in tables:
        print "Cloning table", table
        r = agent.query("SELECT * FROM " + table)
        total_charged += r["charged"]
        rows = r["command_result"]["rows"]
        for row in rows:
            values = str(row).translate(None, "[]")
            query = "INSERT INTO {table} VALUES ({values})".format(table=table, values=values)
            c.execute(query)
    print "Total charged:", total_charged


if __name__ == "__main__":
    main()