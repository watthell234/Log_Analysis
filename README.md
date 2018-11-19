Hello there!

This is Project 1 for Udacity's Nanodegree Program.

I was tasked to figure out how to create a Log Analysis report.

In order for you to obtain the proper reports you will need to:

1st have the newsdata.sql file.

2nd clone this repository.

3rd pip install psycopg2.

4th Make sure you have python3 install.

Lastly, run python repordb.py and Voila! you will have all 3 text files downloaded to your active directory.


Views Created:

1. Create view slug_views as Select art.author, art.slug, views_t.views, art.title
from articles art
join (select reverse(split_part(reverse(path), '/', 1)) as article_slug,
count(*) as views
from log
where path like '%article%'
group by article_slug
order by views desc ) views_t on art.slug = views_t.article_slug;
