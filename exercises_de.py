import polars as pl
from polars.testing import assert_frame_equal
import datetime
from utils import HintSolution, assert_approx, get_value

def q0_check(x):
    assert x == "BettercallPaul"

q0 = HintSolution(
    'de',
    'Welche Firma hat diesen Workshop kreiert?',
    q0_check,
    'Es ist nicht BettercallSaul.',
    'coole_firma = "BettercallPaul"'
)


def q1_check(df):
    assert df.shape == (362_182, 4)
    assert df.columns == ["date", "rank", "title", "artist"]

q1 = HintSolution(
    'de',
    'Selektiere vom Dataframe "df" nur die Spalten "date", "rank", "title" und "artist".',
    q1_check,
    'Achte auf die Reihenfolge der Spalten und nutze die Methode "select".',
    'q1_df = df.select("date", "rank", "title", "artist")'
)



def q2_check(df):
    assert df.shape == (362_182, 4)
    assert df.columns == ["date", "rank", "title", "performer"]

q2 = HintSolution(
    'de',
    'Selektiere nun die Spalten "date", "rank", "title" und "artist", aber benenne die Spalte "artist" in "performer" um.',
    q2_check,
    'Du kannst die Funktion "alias" nutzen, um eine Spalte umzubenennen.',
    'q2_df = df.select("date", "rank", "title", pl.col("artist").alias("performer"))'
)



def q3_check(result):
    result = get_value(result)
    assert_approx(result, 1_212_938)

q3 = HintSolution(
    'de',
    'Was ist der Durchschnitt an Streams im Datensatz?',
    q3_check,
    'Du kannst pl.col("streams") mit der Funktion "mean" verbinden.',
    'q3_df = df.select(pl.col("streams").mean())'
)



def q4_check(rank_1, rank_200):
    rank_1 = get_value(rank_1)
    rank_200 = get_value(rank_200)

    assert_approx(rank_1, 6_452_678)
    assert_approx(rank_200, 604_534)

q4 = HintSolution(
    'de',
    'Wie oft wurden Lieder auf Platz 1 durchschnittlich pro Tag gestreamt und wie oft die Lieder auf Platz 200?',
    q4_check,
    'So ähnlich wie Frage 3 aber jeweils die Spalte "rank" entsprechend gefiltert.',
    '''
rank_1 = df.filter(pl.col("rank").eq(1)).select(pl.col("streams").mean())
rank_200 = df.filter(pl.col("rank").eq(200)).select(pl.col("streams").mean())
    '''
)


def q5_check(result):
    result = get_value(result, "title")
    assert result == "rockstar"

q5 = HintSolution(
    'de',
    'Welches Lied war am Sylvesterabend 2017 auf Platz 1? Höre es auch gerne an.',
    q5_check,
    'Filter sowohl auf den 31.12.2017 ("date") als auch auf den Platz 1 ("rank").',
    '''
q5_df = df.filter(pl.col("date").eq(pl.date(2017, 12, 31)) & pl.col("rank").eq(1))
play_song(q5_df)
    '''
)

def q6_check(result):
    expected = pl.DataFrame({
        "title": ["All I Want for Christmas Is You", "Last Christmas"],
        "artist": ["Mariah Carey", "Wham!"],
        "date_min": ["2017-11-11", "2017-11-11"],
        "date_max": ["2021-12-20", "2021-12-20"]
    }).with_columns(pl.col("date_min", "date_max").str.to_date())
    actual = result.select("title", "artist", pl.col("date").min().name.suffix("_min"), pl.col("date").max().name.suffix("_max")).unique()
    assert_frame_equal(expected, actual, check_row_order=False, check_column_order=False)

q6 = HintSolution(
    'de',
    '''
🎄🎅🏻 X-Mas-Showdown 🎅🏻🎄
"Last Christmas" von "Wham!" oder "All I Want for Christmas Is You" von "Mariah Carey"???
Filter auf die beiden Lieder und plotte dann die Streams. Was ist dein Favorit?
    ''',
    q6_check,
    'Du brauchst einen Filter in der Form (TITEL_1 und KÜNSTLER_1) oder (TITEL_2 und KÜNSTLER_2) und die Methode "plot_streams".',
    '''
q6_df = df.filter(
    (pl.col("title").eq("All I Want for Christmas Is You") & pl.col("artist").eq("Mariah Carey")) |
    (pl.col("title").eq("Last Christmas") & pl.col("artist").eq("Wham!"))
)
plot_streams(q6_df)
    '''
)

def q7_check(result):
    expected = pl.DataFrame({
        "title": ["Last Christmas"],
        "artist": ["Wham!"],
        "date_min": ["2017-12-24"],
        "date_max": ["2020-12-25"]
    }).with_columns(pl.col("date_min", "date_max").str.to_date())
    actual = result.select("title", "artist", pl.col("date").min().name.suffix("_min"), pl.col("date").max().name.suffix("_max")).unique()
    assert_frame_equal(expected, actual, check_row_order=False, check_column_order=False)

q7 = HintSolution(
    'de',
    'Filter auf alle zweitplatzierten Lieder an Weihnachten (24. und 25. Dezember)!',
    q7_check,
    'Filter sowohl auf den Tag ("dt.day()") mit "is_between", auf Dezember ("dt.month()") und auf den Platz 2 ("rank").',
    '''
q7_df = df.filter(
    pl.col("date").dt.month().eq(12) &
    pl.col("date").dt.day().is_between(24, 25) &
    pl.col("rank").eq(2)
)
    '''
)

def q8_check(monday, friday):
    monday = get_value(monday, "streams")
    friday = get_value(friday, "streams")
    assert_approx(monday, 1_180_265)
    assert_approx(friday, 1_325_348)

q8 = HintSolution(
    'de',
    'Berechne die durchschnittliche Anzahl an Streams je Song an Montagen und an Freitagen!',
    q8_check,
    'Filter auf jeweils auf den Wochentag mit "dt.weekday()", beachte "Montag==1"',
    '''
q8_monday = df.filter(pl.col("date").dt.weekday().eq(1)).select(pl.col("streams").mean())
q8_friday = df.filter(pl.col("date").dt.weekday().eq(5)).select(pl.col("streams").mean())
    '''
)

def q9_check(result):
    result = get_value(result)
    assert (result - datetime.timedelta(days=14, hours=15, minutes=39)).total_seconds() < 60

q9 = HintSolution(
    'de',
    'Wie viele Tage sind im Datensatz durchschnittlich seit dem jeweiligen Monatsbeginn vergangen?',
    q9_check,
    'Bilde den Mittelwert ("mean") über die Differenz vom Datum und Monatsbeginn ("dt.month_start()").',
    '''
q9_df = df.select((pl.col("date") - pl.col("date").dt.month_start()).mean())
    '''
)


def q10_check(result):
    expected = pl.DataFrame({
        "artist": [
            "Shawn Mendes, Zedd",
            "Zedd, Maren Morris, Grey",
            "Zedd, Jasmine Thompson",
            "Zedd, Katy Perry",
            "Hailee Steinfeld, Grey, Zedd",
            "Zedd, Alessia Cara",
            "Zedd, Elley Duhé"
        ],
    })
    assert_frame_equal(expected, result, check_row_order=False, check_column_order=False)

q10 = HintSolution(
    'de',
    'Erstelle einen Dataframe mit allen Künstler-Kooperationen bei denen "Zedd" mitgewirkt hat.',
    q10_check,
    'Filter auf alle Künstlernamen in den Zedd enthalten ist, aber die nicht genau Zedd sind. Benutze die Funktion "unique".',
    '''
q10_df = df.filter(pl.col("artist").str.contains("Zedd") & pl.col("artist").ne("Zedd")).select(pl.col("artist").unique())
    '''
)

def q11_check(ohne_zedd, mit_zedd):
    ohne_zedd = get_value(ohne_zedd)
    mit_zedd = get_value(mit_zedd)
    assert ohne_zedd == 101
    assert mit_zedd == 6

q11 = HintSolution(
    'de',
    'Was ist die höchste Chart-Platzierung, die "Maren Morris" mit "Zedd" erreicht hat? Und ohne ihn?',
    q11_check,
    'Filter auf Künstler-Namen die "Maren Morris" und/und nicht "Zedd" enthalten. Nutze den kleinsten Wert von "rank".',
    '''
q11_ohne_zedd = (
    df.filter(
        pl.col("artist").str.contains("Maren Morris") &
        ~ pl.col("artist").str.contains("Zedd")
    )
    .select(pl.col("rank").min())
)

q11_mit_zedd = (
    df.filter(
        pl.col("artist").str.contains("Maren Morris") &
        pl.col("artist").str.contains("Zedd")
    )
    .select(pl.col("rank").min())
)
    '''
)



def q12_check(df, result):
    expected_size = df.with_columns(
        pl.col("title").cast(pl.Categorical),
        pl.col("artist").cast(pl.Categorical),
        pl.col("trend").cast(pl.Categorical),
        pl.col("region").cast(pl.Categorical),
        pl.col("chart").cast(pl.Categorical),
        pl.col("rank").cast(pl.UInt8),
        pl.col("streams").cast(pl.UInt32),
        pl.col("url").str.slice(len("https://open.spotify.com/track/")).cast(pl.Categorical)
    ).estimated_size("mb")

    actual_size = result.estimated_size("mb")

    assert actual_size <= expected_size
    

q12 = HintSolution(
    'de',
    '''
Minimiere den Speicherverbrauch des Dataframes durch andere Datentypen und das Entfernen eines unnötigen Präfix.
Den Speicherverbrauch kannst du mit df.estimated_size("mb") anzeigen.
    ''',
    q12_check,
    'Entferne den Präfix aus der Spalte "url" z.B. mit "str.replace" oder "str.slice", caste zu "pl.Categorical" für alle Strings, UInt8 bzw. UInt32 für die Zahlen.',
    '''
q12_df = df.with_columns(
    pl.col("title").cast(pl.Categorical),
    pl.col("artist").cast(pl.Categorical),
    pl.col("trend").cast(pl.Categorical),
    pl.col("region").cast(pl.Categorical),
    pl.col("chart").cast(pl.Categorical),
    pl.col("rank").cast(pl.UInt8),
    pl.col("streams").cast(pl.UInt32),
    pl.col("url").str.slice(len("https://open.spotify.com/track/")).cast(pl.Categorical)
)
    '''
)


def q13_check(result):
    expected = pl.DataFrame([
        pl.Series("title", ['Sunflower - Spider-Man: Into the Spider-Verse', 'Someone You Loved', 'Dance Monkey', 'Blinding Lights', 'Shape of You'], dtype=pl.Utf8),
        pl.Series("artist", ['Post Malone, Swae Lee', 'Lewis Capaldi', 'Tones And I', 'The Weeknd', 'Ed Sheeran'], dtype=pl.Utf8),
        pl.Series("streams", [2046023015, 2111297778, 2373957880, 2623933279, 2921494072], dtype=pl.Int64),
    ])

    assert_frame_equal(expected, result, check_row_order=False, check_column_order=False)

q13 = HintSolution(
    'de',
    '''
Ermittel die 5 Songs mit den meisten Streams über den gesamten Zeitraum.
    ''',
    q13_check,
    'Gruppiere nach "title" und "artist", aggregiere "streams" als Summe und filter mit der Funktion "top_k".',
    '''
q13_df = (df
    .group_by("title", "artist")
    .agg(pl.col("streams").sum())
    .top_k(5, by="streams")
)
    '''
)


def q14_check(result):
    expected = pl.DataFrame([
        pl.Series("title", ['Shape of You', 'Blinding Lights', 'Dance Monkey', 'Someone You Loved', 'Sunflower - Spider-Man: Into the Spider-Verse'], dtype=pl.Utf8),
        pl.Series("urlCount", [1, 3, 2, 2, 5], dtype=pl.UInt32),
    ])

    assert_frame_equal(expected, result.select("title", "urlCount"), check_row_order=False, check_column_order=False)

q14 = HintSolution(
    'de',
    '''
Ermittel die 5 Songs mit den meisten Streams über den gesamten Zeitraum und auch
wie viele unterschiedliche "url"s je Song vorhanden sind ("urlCount") und eine (die erste) "url" je Song.
Höre dir die Songs mit der Funktion "play_song" an.
    ''',
    q14_check,
    'Wie Frage 13, aber zusätzlich mit "n_unique" (als "urlCount") und "first" auf der Spalte "url".',
    '''
q14_df = (df
    .group_by("title", "artist")
    .agg(pl.col("streams").sum(), pl.col("url").n_unique().alias("urlCount"), pl.col("url").first())
    .top_k(5, by="streams")
)
play_song(q14_df, 0)
    '''
)



def q15_check(result):
    expected = pl.DataFrame([
        pl.Series("title", ['Thinking out Loud', "Say You Won't Let Go", 'Shape of You', 'All of Me', 'Photograph'], dtype=pl.Utf8),
    ])

    assert_frame_equal(expected, result.select("title"), check_row_order=False, check_column_order=False)

q15 = HintSolution(
    'de',
    '''
Berechne pro Song den romantischen 💕 Valentins-Index ("valentinesIndex") als durchschnittliche Anzahl an Streams
am Valentins-Tag geteilt durch die durchschnittliche Anzahl an Streams an allen anderen Tagen.
Filter auf die 5 romantischsten Songs 😍 🎶 😍, die in jedem Jahr am Valentins-Tag in den Charts waren.
Plotte die Streams für den romantischsten Song.
    ''',
    q15_check,
    '''
Lege ein Hilfs-Spalte "isValentinesDay" an, gruppiere nach Titel und Künstler und ermittel in der Aggregation
die Anzahl der Jahre mit "n_unique" und filter die Aggregations-Ausdrücke mit der Hilfs-Spalte "isValentinesDay".
    ''',
    '''
q15_df = (df
    .with_columns((pl.col("date").dt.month().eq(2) & pl.col("date").dt.day().eq(14)).alias("isValentinesDay"))
    .group_by("title", "artist")
    .agg(
        pl.col("date").filter(pl.col("isValentinesDay")).dt.year().n_unique().alias("valentineYears"),
        (pl.col("streams").filter(pl.col("isValentinesDay")).mean()/pl.col("streams").filter(~pl.col("isValentinesDay")).mean()).alias("valentinesIndex"),
    )
    .filter(pl.col("valentineYears").eq(5))
    .top_k(5, by="valentinesIndex")
)
plot_streams(df.filter(pl.col("title").eq("Thinking out Loud")))
    '''
)


def q16_check(result):
    expected_date = pl.DataFrame([
        pl.Series("date", [datetime.date(2021, 12, 20)], dtype=pl.Date),
    ])

    expected_songs = pl.DataFrame([
        pl.Series("artist", ['Mariah Carey', 'Michael Bublé', 'Wham!'], dtype=pl.Utf8),
        pl.Series("title", ['All I Want for Christmas Is You', "It's Beginning to Look a Lot like Christmas", 'Last Christmas'], dtype=pl.Utf8),
    ])

    assert_frame_equal(expected_date, result.select(pl.col("date").max()), check_row_order=False, check_column_order=False)
    assert_frame_equal(expected_songs, result.select("artist", "title").unique(), check_row_order=False, check_column_order=False)

q16 = HintSolution(
    'de',
    '''
Erstelle eine Liste mit Weihnachts-Liedern in dem Du auf alle Titel filterst, die das Wort "Christmas" enthalten.
Gruppiere dann auf "url" und ermittel die Top-3 Songs mit den meisten Streams. Verbinde den Original-Datensatz und plotte
die Streams der drei beliebtesten Weihnachtslieder.
    ''',
    q16_check,
    '''
Nutze einen eigenen Namen für die Summe aller Streams (z.B. "totalStreams"), nutze top_k und mache einen join auf "df" mit "url" als Schlüssel.
    ''',
    '''
q16_df = (df
    .filter(pl.col("title").str.contains("Christmas"))
    .group_by("url")
    .agg(pl.col("streams").sum().alias("totalStreams"))
    .top_k(3, by="totalStreams")
    .join(df, on="url")
)
plot_streams(q16_df)
    '''
)


def q17_check(result):
    expected = pl.DataFrame([
        pl.Series("genre", ['pop music', 'hip hop music', 'contemporary R&B', 'dance-pop', 'trap music'], dtype=pl.Utf8),
    ])

    assert_frame_equal(expected, result.select("genre"), check_row_order=False, check_column_order=False)


q17 = HintSolution(
    'de',
    '''
Lese die Datei "track-genres.parquet" ein. Ergänze dann den Hauptdatensatz um diese Genres und ermittel die 5 am meisten gestreamten Musik-Genres.
    ''',
    q17_check,
    '''
Joine über die Spalte "url" und rolle die Spalte "genres" mit der Methode explode aus, bevor du gruppierst, aggregierst und auf die Top 5 filterst.
    ''',
    '''
q17_df = (df
    .join(pl.read_parquet("track-genres.parquet"), on="url")
    .explode("genre")
    .group_by("genre")
    .agg(pl.col("streams").sum())
    .top_k(5, by="streams")
)
    '''
)


def q18_check(result):
    result = get_value(result)
    assert_approx(result, 0.519)


q18 = HintSolution(
    'de',
    '''
Ermittel den Anteil der gesamten Streams für die wir ein oder mehrere Genres haben (z.B. 0.25 falls für 25% der Streams eine Genre-Angabe vorhanden ist).
    ''',
    q18_check,
    '''
Benutze einen Left-Join. Das Ergebnis sollte entweder eine Zahl kleiner als 1 mit 3 Nachkommastellen sein oder ein Dataframe mit genau einer Zeile und einer Spalte sein.
    ''',
    '''
q18_df = (df
    .join(pl.read_parquet("track-genres.parquet"), on="url", how="left")
    .group_by(pl.col("genre").is_not_null().alias("knownGenre"))
    .agg(pl.col("streams").sum())
    .with_columns(pl.col("streams")/pl.col("streams").sum())
    .select(pl.col("streams").filter(pl.col("knownGenre")))
)
    '''
)


def q19_check(result):
    expected = pl.DataFrame([
        pl.Series("artist", ['Drake', 'Taylor Swift', 'Ariana Grande', 'Mariah Carey', 'Billie Eilish', 'Post Malone'], dtype=pl.Utf8),
        pl.Series("2020", [1, 2, 2, 1, 2, 0], dtype=pl.UInt32),
        pl.Series("2018", [4, 0, 1, 1, 0, 3], dtype=pl.UInt32),
        pl.Series("2019", [0, 1, 2, 1, 2, 1], dtype=pl.UInt32),
        pl.Series("2017", [0, 1, 0, 1, 0, 0], dtype=pl.UInt32),
        pl.Series("2021", [2, 1, 0, 1, 0, 0], dtype=pl.UInt32),
        pl.Series("allYears", [7, 5, 5, 5, 4, 4], dtype=pl.UInt32),
    ])
    assert_frame_equal(expected, result, check_row_order=False, check_column_order=False)


q19 = HintSolution(
    'de',
    '''
Erstelle einen Dataframe der für jeden Künstler die Anzahl der Nr. 1 Hits je Jahr in getrennten Spalten ausweist.
Erstelle zusätzlich eine Spalte für die Gesamtanzahl an Nr. 1 Hits ("allYears") und
filtere auf die 6 Künstler mit den meisten Nr. 1 Hits ("allYears").
    ''',
    q19_check,
    '''
Filter auf die Nr. 1 Hits, zähle die eindeutigen Titelnamen je Jahr und Künstler und pivotiere dann.
    ''',
    '''
q19_df = (df
    .filter(pl.col("rank").eq(1))
    .group_by("artist", pl.col("date").dt.year().alias("year"))
    .agg(pl.col("title").n_unique().alias("numberOnes"))
    .pivot(index="artist", on="year", values="numberOnes")
    .fill_null(0)
    .with_columns(pl.sum_horizontal(pl.all().exclude("artist")).alias("allYears"))
    .top_k(6, by="allYears")
)
    '''
)


q20 = HintSolution(
    'de',
    '''
Schreibe die Musterlösung von Frage 12 so um, dass die String-Spalten nicht einzeln
mit einem Namen selektiert werden.
    ''',
    q12_check,
    'Entferne alle Casts zu Categorical aus der Musterlösung und mache den finalen Cast in einem zusätzlichen "with_columns".',
    '''
q20_df = (df
    .with_columns(
        pl.col("rank").cast(pl.UInt8),
        pl.col("streams").cast(pl.UInt32),
        pl.col("url").str.slice(len("https://open.spotify.com/track/"))
    )
    .with_columns(pl.col(pl.Utf8).cast(pl.Categorical)))
    '''
)

def q21_check(result):
    expected = pl.DataFrame([
        pl.Series("region", ['Global', 'United States', 'Brazil', 'Mexico', 'Germany', 'United Kingdom', 'Spain', 'Italy', 'France', 'Australia'], dtype=pl.Utf8),
    ])
    assert_frame_equal(expected, result.select("region"), check_row_order=False, check_column_order=False)

q21 = HintSolution(
    'de',
    '''
Ermittel die 10 Regionen mit den meisten Streams.
    ''',
    q21_check,
    'Gruppiere nach "region", aggregiere und dann top-k.',
    '''
q21_df = (df
    .group_by("region")
    .agg(pl.col("streams").sum())
    .top_k(10, by="streams")
    .collect()
)
    '''
)


def q22_check(result):
    expected = pl.DataFrame([
        pl.Series("region", ['Norway', 'Sweden', 'Iceland', 'Denmark', 'Netherlands', 'Finland', 'Chile', 'New Zealand', 'Ireland', 'Australia'], dtype=pl.Utf8),
    ])
    assert_frame_equal(expected, result.select("region"), check_row_order=False, check_column_order=False)

q22 = HintSolution(
    'de',
    '''
Lade zusätzlich die Datei region-info.csv und ermittel jetzt die 10 Regionen mit
den meisten Streams relativ zur Bevölkerung.
    ''',
    q22_check,
    'Lade "region-info.csv" mit "pl.scan_csv", teile die Summe der Streams durch "population".',
    '''
region_df = pl.scan_csv("region-info.csv")
q22_df = (df
    .group_by("region")
    .agg(pl.col("streams").sum())
    .join(region_df, on="region")
    .with_columns(pl.col("streams")/pl.col("population"))
    .top_k(10, by="streams")
    .collect()
)
    '''
)



def q23_check(result):
    expected = pl.DataFrame([
        pl.Series("chart", ['viral50', 'top200'], dtype=pl.Utf8),
    ])
    assert_frame_equal(expected, result, check_row_order=False, check_column_order=False)


q23 = HintSolution(
    'de',
    '''
Ermittel die unterschiedlichen Werte für die Spalte "chart".
    ''',
    q23_check,
    'Du kannst die Funktion "unique" verwenden.',
    '''
q23_df = df.select(pl.col("chart").unique()).collect()
    '''
)


def q24_check(result):
    expected = pl.DataFrame([
        pl.Series("continent", ['Asia,Europe', 'Europe,Asia', 'North America', 'Oceania', 'Africa', 'Earth', 'Asia', 'Europe', 'South America'], dtype=pl.Utf8),
        pl.Series("xmasYears", [4, 1, 5, 4, 3, 4, 5, 5, 5], dtype=pl.UInt32),
    ])
    assert_frame_equal(expected, result, check_row_order=False, check_column_order=False)

q24 = HintSolution(
    'de',
    '''
Berechne pro Kontinent für wie viele Weihnachten es Einträge für die "top200" gibt (sowohl
am 24. als auch 25. Dezember). Nenne die Spalte mit den Jahren "xmasYears".
    ''',
    q24_check,
    'Lade "region-info.csv" mit "pl.scan_csv" und joine, filter auf "top200" und Weihnachten und nutze "dt.year().n_unique()"',
    '''
region_df = pl.scan_csv("region-info.csv")
q24_df = (df
    .filter(
        pl.col("chart").eq("top200") &
        pl.col("date").dt.month().eq(12) &
        pl.col("date").dt.day().is_between(24, 25)
    )
    .join(region_df, on="region")
    .group_by("continent")
    .agg(pl.col("date").dt.year().n_unique().alias("xmasYears"))
    .collect()
)
    '''
)

def q25_check(result):
    expected = pl.DataFrame([
        pl.Series("continent", ['Earth', 'Earth', 'North America', 'North America', 'Europe', 'Europe', 'South America', 'South America', 'Asia,Europe', 'Asia,Europe', 'Asia', 'Asia', 'Oceania', 'Oceania', 'Europe,Asia', 'Europe,Asia', 'Africa', 'Africa'], dtype=pl.Utf8),
        pl.Series("artist", ['Mariah Carey', 'Wham!', 'Mariah Carey', 'Bobby Helms', 'Mariah Carey', 'Wham!', 'Mariah Carey', 'Bobby Helms', 'Ezhel', 'Yüzyüzeyken Konuşuruz', 'Mariah Carey', 'Ariana Grande', 'Mariah Carey', 'Wham!', 'Big Baby Tape', 'SLAVA MARLOW', 'Mariah Carey', 'Wham!'], dtype=pl.Utf8),
        pl.Series("title", ['All I Want for Christmas Is You', 'Last Christmas', 'All I Want for Christmas Is You', 'Jingle Bell Rock', 'All I Want for Christmas Is You', 'Last Christmas', 'All I Want for Christmas Is You', 'Jingle Bell Rock', 'Geceler', 'Ne Farkeder', 'All I Want for Christmas Is You', 'Santa Tell Me', 'All I Want for Christmas Is You', 'Last Christmas', 'KARI', 'Снова я напиваюсь', 'All I Want for Christmas Is You', 'Last Christmas'], dtype=pl.Utf8),
    ])
    assert_frame_equal(expected, result.select("continent", "title", "artist"), check_row_order=False, check_column_order=False)

q25 = HintSolution(
    'de',
    '''
Schwieriger Endgegner: Berechne die Top-Weihnachts-Songs je Kontinent.

- Erstelle einen Dataframe mit Kontinent und Anzahl an Weihnachten (siehe q24)
- Filter dann den Datensatz zuerst auf Songs die an jedem Weihnachten,
dass für den Kontinent im Datensatz enthalten ist, auch in den Top-200 waren.
- Ermittel dann von diesen Songs je Kontinent welche an Weihnachten am meisten gespielt wurden
- Erstelle einen Dataframe mit den Top-2 je Kontinent
    ''',
    q25_check,
    '',
    '''
region_df = pl.scan_csv("region-info.csv")
xmasYears_per_continent = (df
    .filter(
        pl.col("chart").eq("top200") &
        pl.col("date").dt.month().eq(12) &
        pl.col("date").dt.day().is_between(24, 25)
    )
    .join(region_df, on="region")
    .group_by("continent")
    .agg(pl.col("date").dt.year().n_unique().alias("xmasYears"))
)

q25_df = (df
    .filter((pl.col("date").dt.month().eq(12) & pl.col("date").dt.day().is_between(24, 25)))
    .join(region_df, on="region")    
    .group_by("title", "artist", "continent")
    .agg(
        pl.col("date").dt.year().n_unique().alias("xmasYears"),
        pl.col("streams").sum()
    )
    .join(xmasYears_per_continent, on=["continent", "xmasYears"])
    .sort("streams", descending=True)
    .group_by("continent")
    .head(2)
    .collect()
)
    '''
)

def q26_check(df):
    value_column = [col for col in df.columns if col != "date"]
    assert df.select(pl.col(value_column).min()).item(0, 0) == 2
    assert df.select(pl.col(value_column).max()).item(0, 0) == 10

q26 = HintSolution(
    'de',
    'Erstelle einen Dataframe, der die Anzahl der Künstler in den globalen Top-10 je Tag angibt.',
    q26_check,
    'Filter `df` auf `rank` und benutze dann die Methode `n_unique`.',
    'q26_df = df.filter(pl.col("rank").le(10)).group_by("date").agg(pl.col("artist").n_unique()).sort("date")'
)

q27 = HintSolution(
    'de',
    'Erstelle ein Linien-Diagramm mit obigem Dataframe, mit dem Datum auf der x-Achse und der Anzahl der Künstler auf der y-Achse.',
    None,
    'Benutze die Methode `px.line`.',
    'q27_fig = px.line(q26_df, x="date", y="artist", height=300)'
)

q28 = HintSolution(
    'de',
    'Füge eine neue Filter-Option hinzu, mit der auf Nr. 1 Hits eingeschränkt werden kann.',
    None,
    'Nutze `dmc.Switch` oder `dmc.Checkbox` und filtere den Dataframe mit `df.filter(pl.col("rank").eq(1))`.',
"""
all_artists = df.select(pl.col("artist").str.split(", ")).get_column("artist").explode().unique().sort().to_list()
all_titles = df.select(pl.col("title").unique()).get_column("title").sort().to_list()

app = Dash(external_stylesheets=dmc.styles.ALL)

def get_streams_chart(artist, title, rank_1_only):
    filter_expr = pl.col("artist").str.contains(artist) if artist else pl.lit(True)
    filter_expr = filter_expr & pl.col("title").eq(title) if title else filter_expr
    # alle Lieder berücksichtigen, die mindestens einmal auf Platz 1 waren
    filter_expr = filter_expr & pl.col("rank").min().over("artist", "title").eq(1) if rank_1_only else filter_expr
    data = df.filter(filter_expr).group_by("date").agg(pl.col("streams").sum()).sort("date")
    return px.line(data, x="date", y="streams", height=300, title=f"Daily Streams for {artist or 'all artists'} - {title or 'all titles'}")

app.layout = dmc.MantineProvider([
    dmc.Title("Spotify Explorer", order=3, mb=20),
    dmc.Group([
        dmc.Select(id="artist-select", label="Artist", placeholder="Select one", data=all_artists, searchable=True, clearable=True),
        dmc.Select(id="title-select", label="Title", placeholder="Select one", data=all_titles, searchable=True, clearable=True),
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
    # nur Lieder berücksichtigen, die mindestens einmal auf Platz 1 waren
    tmp_df = df.filter(pl.col("rank").eq(1)) if rank_1_only else df
    # mögliche Künstler für die aktuelle Filterung ermitteln
    possible_titles = tmp_df.filter(pl.col("artist").str.contains(selected_artist) if selected_artist else pl.lit(True)).select(pl.col("title").unique()).get_column("title").sort().to_list()
    # mögliche Titel für die aktuelle Filterung ermitteln
    possible_artists = tmp_df.filter(pl.col("title").eq(selected_title) if selected_title else pl.lit(True)).select(pl.col("artist").str.split(", ")).get_column("artist").explode().unique().sort().to_list()
    return get_streams_chart(selected_artist, selected_title, rank_1_only), possible_artists, possible_titles

app.run(jupyter_mode="inline", jupyter_height=500)
"""
)