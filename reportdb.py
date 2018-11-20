#!/usr/bin/env python3
import psycopg2
import time
DBNAME = "news"

TOP_THREE = """select title, views from slug_views
order by views desc
limit 3;"""

TOP_AUTHORS = """select aut.name as author_name, sum(slug_views.views) as views
from authors aut
Join slug_views on aut.id = slug_views.author
group by author_name order by views desc;"""

ERROR_ONE = """select time::timestamp::date as date,
100
* (sum(case when status = '404 NOT FOUND' then 1 else 0 end) / count(*)::real)
from log
group by date
having 100 *
(sum(case when status = '404 NOT FOUND' then 1 else 0 end)
 / count(*)::real) > 1;"""


def connect(database_name):
    """Connect to the PostgreSQL database.  Returns a database connection."""
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        c = db.cursor()
        return db, c
    except psycopg2.Error as e:
        logger.warning("error creating database: %s", self.fmt_errmsg(e))



def top_articles(sql):
    db, c = connect(DBNAME)
    c.execute(sql)
    rows = c.fetchall()
    db.close()
    with open("top_three_articles.txt", "w") as f:
        f.writelines(
                    str(r[0]) + " -- " +
                    str("{:,}".format(r[1])) +
                    " views" +
                    "\n" for r in rows)


def top_authors(sql):
    db, c = connect(DBNAME)
    c.execute(sql)
    rows = c.fetchall()
    db.close()
    with open("top_authors.txt", "w") as f:
        f.writelines(
                    str(r[0]) + " -- " +
                    str("{:,}".format(r[1])) +
                    " views" +
                    "\n" for r in rows
                    )


def error_one(sql):
    db, c = connect(DBNAME)
    c.execute(sql)
    rows = c.fetchall()
    db.close()
    with open("error_one.txt", "w") as f:
        f.writelines(
                    str(rows[0][0]) +
                    " -- " +
                    str("{0:.2f}%".format(rows[0][1])) +
                    " errors")


if __name__ == '__main__':
    top_articles(TOP_THREE)
    top_authors(TOP_AUTHORS)
    error_one(ERROR_ONE)
