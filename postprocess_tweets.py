import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from wordcloud import WordCloud


def main():
    tweets = pd.read_parquet('./tweets.parquet') \
        .reset_index() \
        .drop_duplicates(subset='tweet_id') \
        .set_index('tweet_id')
    # tweets_no_dup = tweets.drop_duplicates(subset='full_text').copy()
    tweets['words'] = tweets['full_text'] \
        .str.split() \
        .apply(lambda x: np.array(x, dtype=np.unicode))

    fig, ax = plt.subplots()
    tweets['full_text'].str.len().plot(kind='hist', ax=ax, density=True)
    ax.set_xlabel('Tweet length [characters]')
    plt.show()

    text_freqs = tweets['full_text'].value_counts()

    words = np.hstack(tweets['full_text'].to_numpy())
    words = str.join(' ', words)

    hashtags = np.hstack(tweets['hashtags'].to_numpy())
    # Generate a word cloud image

    swords = set(stopwords.words(['english', 'spanish'])).union(hashtags)
    swords.add('https')
    swords.add('rechazo')
    swords.add('rechazocrece')
    swords.add('rechazotuoportunismo')
    # swords.add('kramerrechazotuodio')
    # swords.add('kramermiserable')
    swords.add('si')
    swords.add('co')
    swords.add('así')
    # swords.add('país')
    # swords.add('apoyoamañalich')
    # swords.add('apoyoalpresidente')
    swords.add('ahora')
    swords.add('solo')
    swords.add('ud')
    # swords.add('chile')
    swords.add('gente')
    # swords.add('chileno')
    # swords.add('chilenos')
    wordcloud = WordCloud(width=800, height=800, stopwords=swords) \
        .generate(words)

    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()


if __name__ == '__main__':
    main()
