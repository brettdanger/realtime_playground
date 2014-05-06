from flask import Flask, Response
import pika
import json
import pandas

 #setup queue
connection = pika.BlockingConnection()
channel = connection.channel()


#function to get data from queue
def get_tweets(size=10):
    tweets = []
    # Get ten messages and break out
    count = 0
    for method_frame, properties, body in channel.consume('twitter_topic_feed'):

        tweets.append(json.loads(body))

        count += 1

        # Acknowledge the message
        channel.basic_ack(method_frame.delivery_tag)

        # Escape out of the loop after 10 messages
        if count == size:
            break

    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    print 'Requeued %i messages' % requeued_messages

    return tweets

app = Flask(__name__)

app.config.update(
    DEBUG=True,
    PROPAGATE_EXCEPTIONS=True
)


@app.route('/feed/raw_feed', methods=['GET'])
def get_raw_tweets():
    tweets = get_tweets(size=5)
    text = ""
    for tweet in tweets:
        tt = tweet.get('text', "")
        text = text + tt + "<br>"

    return text


@app.route('/feed/word_count', methods=['GET'])
def get_word_count():
    tweets = get_tweets(size=200)
    ignore_words = [ "rt", "nhl",'nhl', 'avs', 'wild', 'avalanche', "sterling", "i","me","my","myself","we","us",
                                "our","ours","ourselves","you","your","yours","yourself",
                                "yourselves","he","him","his","himself","she","her","hers",
                                "herself","it","its","itself","they","them","their","theirs",
                                "themselves","what","which","who","whom","whose","this",
                                "that","these","those","am","is","are","was","were","be","been",
                                "being","have","has","had","having","do","does","did","doing",
                                "will","would","should","can","could","ought","i'm","you're","he's",
                                "she's","it's","we're","they're","i've","you've","we've","they've",
                                "i'd","you'd","he'd","she'd","we'd","they'd","i'll","you'll","he'll",
                                "she'll","we'll","they'll","isn't","aren't","wasn't","weren't","hasn't",
                                "haven't","hadn't","doesn't","don't","didn't","won't","wouldn't","shan't",
                                "shouldn't","can't","cannot","couldn't","mustn't","let's","that's","who's",
                                "what's","here's","there's","when's","where's","why's","how's","a","an",
                                "the","and","but","if","or","because","as","until","while","of","at","by","for",
                                "with","about","against","between","into","through","during","before","after",
                                "above","below","to","from","up","upon","down","in","out","on","off","over",
                                "under","again","further","then","once","here","there","when","where",
                                "why","how","all","any","both","each","few","more","most","other","some",
                                "such","no","nor","not","only","own","same","so","than","too","very","say","says","said","shall"]
    words = []
    for tweet in tweets:
        tt = tweet.get('text', "").lower()
        for word in tt.split():
            if "http" in word:
                continue
            if word not in ignore_words:
                words.append(word)

    p = pandas.Series(words)
    #get the counts per word
    freq = p.value_counts()
    #how many max words do we want to give back
    freq = freq.ix[0:300]

    response = Response(freq.to_json())
    
    response.headers.add('Access-Control-Allow-Origin', "*")
    return response

if __name__ == "__main__":
    app.run()
