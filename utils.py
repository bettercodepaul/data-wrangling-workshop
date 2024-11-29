import urllib.request
import os.path

URL_TRACK_ID_PREFIX = "https://open.spotify.com/track/"

def download_data():
    # download CSV data
    DATA_URL = "https://github.com/bettercodepaul/data2day_2023_polars/raw/main/spotify-charts-2017-2021-global-top200.csv.gz"
    LOCAL_DATA_FILE_NAME = os.path.basename(DATA_URL)
    urllib.request.urlretrieve(DATA_URL, LOCAL_DATA_FILE_NAME)
    # download track genres data
    GENRES_DATA_URL = "https://github.com/bettercodepaul/data2day_2023_polars/raw/main/track-genres.parquet"
    LOCAL_GENRES_DATA_FILE_NAME = os.path.basename(GENRES_DATA_URL)
    urllib.request.urlretrieve(GENRES_DATA_URL, LOCAL_GENRES_DATA_FILE_NAME)
    # download requirements.txt with required libraries
    REQUIREMENTS_URL = "https://github.com/bettercodepaul/data-wrangling-praktikum/raw/master/requirements.txt"
    urllib.request.urlretrieve(REQUIREMENTS_URL, os.path.basename(REQUIREMENTS_URL))
    # exercises
    EXERCISES_URL = "https://github.com/bettercodepaul/data-wrangling-praktikum/raw/master/exercises_de.py"
    urllib.request.urlretrieve(EXERCISES_URL, os.path.basename(EXERCISES_URL))
    UTILS_URL = "https://github.com/bettercodepaul/data-wrangling-praktikum/raw/master/utils.py"
    urllib.request.urlretrieve(EXERCISES_URL, os.path.basename(UTILS_URL))

def download_region_data():
    REGION_DATA_URL = "https://github.com/bettercodepaul/data2day_2023_polars/raw/main/region-info.csv"
    LOCAL_REGION_DATA_FILE_NAME = os.path.basename(REGION_DATA_URL)
    urllib.request.urlretrieve(REGION_DATA_URL, LOCAL_REGION_DATA_FILE_NAME)
    BIG_DATA_URL = "https://github.com/bettercodepaul/data2day_2023_polars/releases/download/data-parquet/spotify-charts-2017-2021.parquet"
    LOCAL_BIG_DATA_FILE_NAME = os.path.basename(BIG_DATA_URL)
    urllib.request.urlretrieve(BIG_DATA_URL, LOCAL_BIG_DATA_FILE_NAME)

def play_song(df, index=0):
    from IPython.display import IFrame
    if "trackId" in df.columns:
        trackId = df.item(index, "trackId")
    elif "url" in df.columns:
        trackId = df.item(index, "url")[len(URL_TRACK_ID_PREFIX):]
    else:
        return("Can not play a song without either column 'url' or 'trackId'")

    url = f"https://open.spotify.com/embed/track/{trackId}?utm_source=generator"
    return(IFrame(src=url, width="100%", height=152, style="border-radius:12px", frameBorder="0", allowfullscreen="", allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture", loading="lazy"))

def plot_rank(df, title="Daily Spotify Rank"):
    import plotly.express as px
    import polars as pl
    plot_df = df.with_columns(
        pl.format("{} by {} ({}, {})", "title", "artist", "region", "chart").alias("label"),
    #     (pl.col("title") + " by " + pl.col("artist") + " (" + pl.col("region") + ", " + pl.col("chart") + ")").alias("label")
    ).sort("date")
    fig = px.line(
        plot_df,
        x = "date",
        y = "rank",
        color = "label",
        title = title,
    )
    fig.update_layout(
        yaxis = dict(autorange="reversed")
    )
    return(fig)

def plot_streams(df, title="Daily Spotify Streams"):
    import plotly.express as px
    import polars as pl
    plot_df = df.with_columns(
        pl.format("{} by {} ({}, {})", "title", "artist", "region", "chart").alias("label"),
    ).sort("date")
    fig = px.line(
        plot_df,
        x = "date",
        y = "streams",
        color = "label",
        title = title,
    )
    return(fig)


def assert_approx(actual, expected, tol=0.001):
    assert abs(actual - expected) < abs(tol*expected)


def get_value(df, column=0):
    import polars as pl
    if df is pl.dataframe.frame.DataFrame:
        value = df.item(0, column)
    else:
        value = df
    return value

class HintSolution:

    def __init__(self, language, question, check, hint, solution):
        self.language = language
        self.tries = 1
        self._question = question
        self._hint = hint
        self._check = check
        self._solution = solution

    def question(self):
        print(self._question)

    def hint(self):
        print(self._hint)

    def solution(self):
        print(self._solution)

    def check(self, *args):
        if args[0] is Ellipsis:
            if self.language=="de":
                return "❓ Moment, die drei Punkte musst du mit deiner Lösung ersetzen!"
            else:
                return "❓ Hold on, you have to replace the ellipsis with your solution!"
        
        if self._check is None:
            if self.language=="de":
                return "🔎 Ich kann das Ergebnis dieser Aufgabe nicht überprüfen, aber es ist bestimmt großartig! 🎉👏"
            else:
                return "🔎 I can't check the result of this exercise, but I'm sure it's amazing! 🎉👏"
        try:
            self._check(*args)
            check_result = True
        except:
            check_result = False
        
        if check_result:
            if self.tries == 1:
                if self.language=="de":
                    return "✅ Wow, erster Versuch - du hast es voll drauf! Du bist ein geborener Problemlöser! 🎉👏"
                else:
                    return "✅ Wow, first try and you nailed it! You're a natural problem-solver! 🎉👏"
            elif self.tries == 2:
                if self.language=="de":
                    return "✅ Voll ins Schwarze getroffen! Schon beim zweiten Schuss mitten auf die Zwölf! 🎯👏"
                else:
                    return "✅ Right on target! You hit the bullseye on your second shot! 🎯👏"
            elif self.tries == 3:
                if self.language=="de":
                    return "✅ Durchhalten zahlt sich aus! Mit dem dritten Versuch hast du es geschafft! 🌟👏"
                else:
                    return "✅ Persistence pays off! Third try's a charm, and you did it! 🌟👏"
            else:
                if self.language=="de":
                    return "✅ Es hat vielleicht ein paar Versuche gebraucht, aber du bist nicht aufzuhalten! 😅👊"
                else:
                    return "✅ It might have taken a few tries, but you're unstoppable! 😅👊"
                
        else:
            self.tries = self.tries + 1
            if self.tries == 2:
                if self.language=="de":
                    return "🤔 Probier es nochmal! Du hast ja gerade erst angefangen. 🔍"
                else:
                    return "🤔 Give it another shot! You're just getting started. 🔍"
            elif self.tries == 3:
                if self.language=="de":
                    return "🤔 Zwei Versuche hinter dir, aber die Lösung ist in Reichweite. Nur Mut! 🧐"
                else:
                    return "🤔 Two tries down, but the solution is within reach. Keep going! 🧐"
            elif self.tries == 4:
                if self.language=="de":
                    return "🤔 Du bist fast am Ziel, nur noch eine letzte Anstrengung! Du schaffst das! 😬"
                else:
                    return "🤔 Almost there, just one more push! You can do it! 😬"
            else:
                if self.language=="de":
                    return "🤔 Es ist schwierig, aber verlier nicht die Hoffnung! Hast du schon die hint() Methode verwendet? 😓"
                else:
                    return "🤔 It's tough, but don't lose hope! Maybe consider using the hint() method now? 😓"