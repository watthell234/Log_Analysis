import datetime
import psycopg2

DBNAME = "news"

TOP_THREE = """select title, views from slug_views
order by views desc
limit 3;"""

TOP_AUTHORS = """select aut.name as author_name, sum(slug_views.views) as views
from authors aut
Join slug_views on aut.id = slug_views.author
group by author_name order by views desc;"""

ERROR_ONE = """select time::timestamp::date as date,
100 * (sum(case when status = '404 NOT FOUND' then 1 else 0 end) / count(*)::real)
from log
group by date
having 100 * (sum(case when status = '404 NOT FOUND' then 1 else 0 end) / count(*)::real) > 1;"""

def get_reports(SQL):
    db = psycopg2.connect(dbname=DBNAME)
    cur = db.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    db.close()
    if SQL == TOP_THREE:
        with open("top_three_articles.txt", "w") as f:
            f.writelines(str(r[0]) + " -- " + str("{:,}".format(r[1])) + " views" + "\n" for r in rows)

    elif SQL == TOP_AUTHORS:
        with open("top_authors.txt", "w") as f:
            f.writelines(str(r[0]) + " -- " + str("{:,}".format(r[1])) + " views" + "\n" for r in rows)
    elif SQL == ERROR_ONE:
        with open("error_one.txt", "w") as f:
            f.writelines(str(rows[0][0]) + " -- " + str("{0:.2f}%".format(rows[0][1])) + " errors")

get_reports(TOP_THREE)
get_reports(TOP_AUTHORS)
get_reports(ERROR_ONE)
