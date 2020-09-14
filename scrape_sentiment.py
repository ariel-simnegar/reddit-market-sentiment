from datetime import datetime
import pandas as pd
from praw import Reddit
from prawcore.exceptions import RequestException

REPLACE_MORE_ATTEMPTS = 5

def replace_more(submission):
    """
    Given a Reddit submission, repeatedly replaces MoreComments placeholders with
    additional comments until no more top level comments remain to be loaded.

    :param submission: The Reddit submission whose comments are to be loaded
    """
    for i in range(REPLACE_MORE_ATTEMPTS):
        try:
            submission.comments.replace_more(limit=None)
            return
        except RequestException:
            pass # Who cares?

    raise Exception(f"replace_more request failed after {REPLACE_MORE_ATTEMPTS} attempts")

def scrape_submission(submission):
    """
    Writes the given Reddit submission's top level comments to a csv file.

    :param submission: The Reddit submission to be scraped
    """
    print(f'Scraping submission "{submission.title}"...')

    submission_date = datetime.utcfromtimestamp(submission.created_utc)
    submission.comment_sort = 'new'

    replace_more(submission)

    comments_data = []

    for comment in submission.comments:
        body = comment.body
        if body == '[deleted]' or body == '[removed]':
            continue
        date = datetime.utcfromtimestamp(comment.created_utc)
        comments_data.append([date, comment.author, comment.score, body])

    comments_df = pd.DataFrame(comments_data, columns=['date', 'author', 'score', 'body'])
    comments_df.to_csv(f'data/{submission_date.strftime("%Y-%m-%d")}.csv', index=False)


def main():
    """
    Scrapes the submissions of the daily "What Are Your Moves Tomorrow" threads on
    r/wallstreetbets to csv files.
    """
    
    reddit = Reddit('RedditMarketSentiment')

    wsb = reddit.subreddit('wallstreetbets')

    for submission in wsb.search('title:"What Are Your Moves Tomorrow"', sort='new', limit=None):
        scrape_submission(submission)

if __name__ == '__main__':
    main()
