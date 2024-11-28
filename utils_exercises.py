from IPython.display import IFrame
import polars as pl
import plotly.express as px
from polars.testing import assert_frame_equal
import datetime

URL_TRACK_ID_PREFIX = "https://open.spotify.com/track/"

def play_song(df, index=0):
    if "trackId" in df.columns:
        trackId = df.item(index, "trackId")
    elif "url" in df.columns:
        trackId = df.item(index, "url")[len(URL_TRACK_ID_PREFIX):]
    else:
        return("Can not play a song without either column 'url' or 'trackId'")

    url = f"https://open.spotify.com/embed/track/{trackId}?utm_source=generator"
    return(IFrame(src=url, width="100%", height=152, style="border-radius:12px", frameBorder="0", allowfullscreen="", allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture", loading="lazy"))

def plot_rank(df, title="Daily Spotify Rank"):
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
    if type(df) == pl.dataframe.frame.DataFrame:
        value = df.item(0, column)
    else:
        value = df
    return value

class HintSolution:

    def __init__(self, question, check, hint, solution):
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
        if type(args[0]) == type(Ellipsis):
            print("❓ Moment, die drei Punkte musst du mit deiner Lösung ersetzen!")
            return
        
        if self._check is None:
            print("🔎 Ich kann das Ergebnis dieser Aufgabe nicht überprüfen, aber es ist bestimmt großartig! 🎉👏")
            return
        try:
            self._check(*args)
            check_result = True
        except:
            check_result = False
        
        if check_result:
            if self.tries == 1:
                #print("✅ Wow, first try and you nailed it! You're a natural problem-solver! 🎉👏")
                print("✅ Wow, erster Versuch - du hast es voll drauf! Du bist ein geborener Problemlöser! 🎉👏")
            elif self.tries == 2:
                #print("✅ Right on target! You hit the bullseye on your second shot! 🎯👏")
                print("✅ Voll ins Schwarze getroffen! Schon beim zweiten Schuss mitten auf die Zwölf! 🎯👏")
            elif self.tries == 3:
                #print("✅ Persistence pays off! Third try's a charm, and you did it! 🌟👏")
                print("✅ Durchhalten zahlt sich aus! Mit dem dritten Versuch hast du es geschafft! 🌟👏")
            else:
                #print("✅ It might have taken a few tries, but you're unstoppable! 😅👊")
                print("✅ Es hat vielleicht ein paar Versuche gebraucht, aber du bist nicht aufzuhalten! 😅👊")
                
        else:
            if self.tries == 1:
                #print("🤔 Give it another shot! You're just getting started. 🔍")
                print("🤔 Probier es nochmal! Du hast ja gerade erst angefangen. 🔍")
            elif self.tries == 2:
                #print("🤔 Two tries down, but the solution is within reach. Keep going! 🧐")
                print("🤔 Zwei Versuche hinter dir, aber die Lösung ist in Reichweite. Nur Mut! 🧐")
            elif self.tries == 3:
                #print("🤔 Almost there, just one more push! You can do it! 😬")
                print("🤔 Du bist fast am Ziel, nur noch eine letzte Anstrengung! Du schaffst das! 😬")
            else:
                #print("🤔 It's tough, but don't lose hope! Maybe consider using the hint() method now? 😓")
                print("🤔 Es ist schwierig, aber verlier nicht die Hoffnung! Hast du schon die hint() Methode verwendet? 😓")
            self.tries = self.tries + 1

def q1_check(df):
    value_column = [col for col in df.columns if col != "date"]
    assert df.select(pl.col(value_column).min()).item(0, 0) == 2
    assert df.select(pl.col(value_column).max()).item(0, 0) == 10

q1 = HintSolution(
    'Erstelle einen Dataframe, der die Anzahl der Künstler in den globalen Top-10 je Tag angibt.',
    q1_check,
    'Filter `df` auf `rank` und benutze dann die Methode `n_unique`.',
    'q1_df = df.filter(pl.col("rank").le(10)).group_by("date").agg(pl.col("artist").n_unique()).sort("date")'
)

q2 = HintSolution(
    'Erstelle ein Linien-Diagramm mit obigem Dataframe, mit dem Datum auf der x-Achse und der Anzahl der Künstler auf der y-Achse.',
    None,
    'Benutze die Methode `px.line`.',
    'q2_fig = px.line(q1_df, x="date", y="artist", height=300)'
)

q3 = HintSolution(
    'Füge eine neue Filter-Option hinzu, mit der auf Nr. 1 Hits eingeschränkt werden kann.',
    None,
    'Nutze `dmc.Switch` oder `dmc.Checkbox` und filtere den Dataframe mit `df.filter(pl.col("rank").eq(1))`.',
"""
all_artists = df.select(pl.col("artist").str.split(", ")).get_column("artist").explode().unique().sort().to_list()
all_titles = df.select(pl.col("title").unique()).get_column("title").sort().to_list()

app = Dash(external_stylesheets=dmc.styles.ALL)

def get_streams_chart(artist, title, rank_1_only):
    filter_expr = pl.lit(True) if artist is None else pl.col("artist").str.contains(artist)
    if title is not None:
        filter_expr = filter_expr & pl.col("title").eq(title)
    if rank_1_only:
        # alle Lieder berücksichtigen, die mindestens einmal auf Platz 1 waren
        filter_expr = filter_expr & pl.col("rank").min().over("artist", "title").eq(1)
    data = df.filter(filter_expr).group_by("date").agg(pl.col("streams").sum()).sort("date")
    return px.line(data, x="date", y="streams", height=300, title=f"Daily Streams for {artist or 'all artists'} - {title or 'all titles'}")

app.layout = dmc.MantineProvider([
    dmc.Title("Spotify Explorer", order=3, mb=20),
    dmc.Group([
        dmc.Select(id="artist-select", label="Artist", placeholder="Select one", data=all_artists, searchable=True),
        dmc.Select(id="title-select", label="Title", placeholder="Select one", data=all_titles, searchable=True),
        # Schalter für die Option, nur Lieder anzuzeigen, die mindestens einmal auf Platz 1 waren
        dmc.Switch(id="rank-1-switch", label="#1 hits only", checked=False)
    ], grow=True, mb=10),
    dcc.Graph(id="streams-chart", figure=get_streams_chart(None, None, False))
])

@app.callback(
    Output(component_id="streams-chart", component_property="figure"),
    Output(component_id="artist-select", component_property="data"),
    Output(component_id="title-select", component_property="data"),
    Input(component_id="artist-select", component_property="value"),
    Input(component_id="title-select", component_property="value"),
    Input(component_id="rank-1-switch", component_property="checked")
)
def update_streams_per_month(selected_artist, selected_title, rank_1_only):
    filter_expr = pl.lit(True) if not rank_1_only else pl.col("rank").eq(1)
    if selected_artist is not None:
        possible_titles = df.filter(filter_expr & pl.col("artist").str.contains(selected_artist)).select(pl.col("title").unique()).get_column("title").sort().to_list()
    else:
        possible_titles = df.filter(filter_expr).select(pl.col("title").unique()).get_column("title").sort().to_list()
    if selected_title is not None:
        possible_artists = df.filter(filter_expr & pl.col("title").eq(selected_title)).select(pl.col("artist").str.split(", ")).get_column("artist").explode().unique().sort().to_list()
    else:
        possible_artists = df.filter(filter_expr).select(pl.col("artist").str.split(", ")).get_column("artist").explode().unique().sort().to_list()
    return get_streams_chart(selected_artist, selected_title, rank_1_only), possible_artists, possible_titles

app.run(jupyter_mode="inline", jupyter_height=500)
"""
)