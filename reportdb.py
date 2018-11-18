import datetime
import psycopg2
import csv

DBNAME = "news"

TOP_THREE = """select reverse(split_part(reverse(path), '/', 1)) as article_slug,
count(*) as views
from log
where path like '%article%'
and status = '200 OK'
group by article_slug
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
        print(rows)
    elif SQL == TOP_AUTHORS:
        print(rows)
    elif SQL == ERROR_ONE:
        print(rows)

get_reports(TOP_THREE)
get_reports(TOP_AUTHORS)
get_reports(ERROR_ONE)
