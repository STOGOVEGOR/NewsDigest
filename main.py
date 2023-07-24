import sqlalchemy as db
from flask import Flask
from sqlalchemy import insert, update, desc
from sqlalchemy.orm import relationship, declarative_base, Session

import time
import atexit
import random
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

engine = db.create_engine("sqlite:///digest.db", echo=True)
conn = engine.connect()
Base = declarative_base()
metadata = db.MetaData() #extracting the metadata

# TODO delete old posts every week
# TODO keywords for every user
# TODO prioritize theme of interests

########################## MODEL SECTION ############################


class User(Base):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    # subscriptions = relationship("Subscription")


class Subscription(Base):
    __tablename__ = 'subscriptions'
    id = db.Column(db.Integer, primary_key=True)
    sub_source = db.Column(db.String)
    sub_name = db.Column(db.String)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    # user = relationship("User", back_populates="subscriptions")
    # post = relationship("Post", back_populates="subscriptions")


class Post(Base):
    __tablename__ = 'posts'
    id = db.Column(db.Integer, primary_key=True)
    sub_id = db.Column(db.Integer, db.ForeignKey('subscriptions.id'), nullable=False)
    post_link = db.Column(db.String, nullable=False)
    post_date = db.Column(db.String, nullable=False)
    post_pop = db.Column(db.Integer, nullable=False)
    post_summary = db.Column(db.String, nullable=False)
    # subscription = relationship("Subscription", back_populates="post")
    # digest = relationship("Digest", back_populates="post")


class Digest(Base):
    __tablename__ = 'digests'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    posts_list = db.Column(db.String)
    # post = relationship("Post", back_populates="digest")


Base.metadata.create_all(engine)

# ######################### SCHEDULE SECTION ############################

scheduler = BackgroundScheduler()


def print_date_time():
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))


def collect_news():
    newsgrabber()


def init_scheduler():
    scheduler.add_job(func=print_date_time, trigger="interval", seconds=5)
    scheduler.add_job(func=collect_news, trigger="interval", seconds=600)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())


init_scheduler()
# ######################### RATING SECTION ############################

def rating_update():
    pass


# ######################### DIGEST SECTION ############################


def create_digest(user_id):
    session = Session(bind=engine)
    query = session.query(Subscription).filter_by(user_id=user_id).all()
    sub_list = [i.sub_name for i in query]
    digest_list = []  # collect summary from posts
    query = session.query(Post).order_by(desc(Post.post_pop)).filter(Post.sub_id.in_(sub_list)).limit(3).all()
    for i in query:
        digest_list.append('* ' + i.post_summary + ' #link')
    digest_fin = '\n\n'.join(digest_list)  # create final digest
    session.query(Digest).where(Digest.user_id == user_id).update({Digest.posts_list: digest_fin}, synchronize_session=False)
    session.commit()


# ######################### NEWSGRABBER SECTION ############################

import http.client, urllib.parse
import json

MEDIASTACK_API = 'f5657705bfc44a19f913d0045fd303e0'
apiconn = http.client.HTTPConnection('api.mediastack.com')

def newsgrabber():
    # to exclude specific news sources by prepending them with a '-' symbol.
    params = urllib.parse.urlencode({
        'access_key': MEDIASTACK_API,
        'categories': 'general,business,science,sports,entertainment,health',
        'sort': 'published_desc',
        'languages': 'en',  # ru, fr
        'sources': 'cnn,bbc',
        'limit': 10,
        })
    apiconn.request('GET', '/v1/news?{}'.format(params))
    res = apiconn.getresponse()
    getdata = res.read()
    data = json.loads(getdata)
    for i in data['data']:
        newpost = Post(
            sub_id=i['category'],
            post_link=i['url'],
            post_date=i['published_at'],
            post_pop=random.randint(1, 100),
            post_summary=i['description'],
        )
        session = Session(bind=engine)
        session.add(newpost)
        session.commit()
        print('new post added')


########################## RESPONSE SECTION ############################
@app.route('/')
def home():
    return "Ready to answer."


@app.route('/getnews/<int:user_id>')
def getnews(user_id):
    create_digest(user_id)
    session = Session(bind=engine)
    digest = session.query(Digest).where(Digest.user_id == user_id).first()
    if digest == None:
        return f'There is no data for user with id {user_id}', 404
    return digest.posts_list


# @app.route('/test/<int:user_id>')
# def test(user_id):
#     # data = User.query.get(user_id)
#     # print(data.id)
#     # print(data.name)
#     # digest = 'Waiting for a test.'
#     session = Session(bind=engine)
#     data = session.query(Digest).where(Digest.user_id == user_id).first()
#     return data.posts_list


if __name__ == "__main__":
    app.run(use_reloader=False, debug=True)