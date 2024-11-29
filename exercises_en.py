import polars as pl
from polars.testing import assert_frame_equal
import datetime
from utils import HintSolution, assert_approx, get_value

def q0_check(x):
    assert x == "BettercallPaul"

q0 = HintSolution(
    'en',
    'Which company created this workshop?',
    q0_check,
    'It\'s not BettercallSaul.',
    'awesome_company = "BettercallPaul"'
)


def q1_check(df):
    assert df.shape == (362_182, 4)
    assert df.columns == ["date", "rank", "title", "artist"]

q1 = HintSolution(
    'en',
    'Select only the columns "date", "rank", "title" and "artist" from the dataframe "df".',
    q1_check,
    'Pay attention to the order of the columns and use the "select" method.',
    'q1_df = df.select("date", "rank", "title", "artist")'
)



def q2_check(df):
    assert df.shape == (362_182, 4)
    assert df.columns == ["date", "rank", "title", "performer"]

q2 = HintSolution(
    'en',
    'Now select the columns "date", "rank", "title" and "artist", but rename the column "artist" to "performer".',
    q2_check,
    'You can use the "alias" function to rename a column.',
    'q2_df = df.select("date", "rank", "title", pl.col("artist").alias("performer"))'
)



def q3_check(result):
    result = get_value(result)
    assert_approx(result, 1_212_938)

q3 = HintSolution(
    'en',
    'What is the average of streams in the dataset?',
    q3_check,
    'You can connect pl.col("streams") with the function "mean".',
    'q3_df = df.select(pl.col("streams").mean())'
)



def q4_check(rank_1, rank_200):
    rank_1 = get_value(rank_1)
    rank_200 = get_value(rank_200)

    assert_approx(rank_1, 6_452_678)
    assert_approx(rank_200, 604_534)

q4 = HintSolution(
    'en',
    'How many times were songs at #1 streamed per day on average, and how many times for the songs at #200?',
    q4_check,
    'Similar to question 3 but the column "rank" is filtered accordingly.',
    '''
rank_1 = df.filter(pl.col("rank").eq(1)).select(pl.col("streams").mean())
rank_200 = df.filter(pl.col("rank").eq(200)).select(pl.col("streams").mean())
    '''
)


def q5_check(result):
    result = get_value(result, "title")
    assert result == "rockstar"

q5 = HintSolution(
    'en',
    'What song was #1 on New Year\'s Eve 2017? Also enjoy listening to it.',
    q5_check,
    'Filter on both 12/31/2017 ("date") and #1 ("rank").',
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
    'en',
    '''
🎄🎅🏻 X-Mas-Showdown 🎅🏻🎄
"Last Christmas" by "Wham!" or "All I Want for Christmas Is You" by "Mariah Carey"???
Filter on the two songs and then plot the streams. Which is your favorite?
    ''',
    q6_check,
    'You need a filter in the form (TITLE_1 and ARTIST_1) or (TITLE_2 and ARTIST_2) and the method "plot_streams".',
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
    'en',
    'Filter on all second place songs on Christmas (December 24 and 25)!',
    q7_check,
    'Filter both on the day ("dt.day()") with "is_between", on December ("dt.month()") and on the second place ("rank").',
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
    'en',
    'Calculate the average number of streams per song on Mondays and on Fridays!',
    q8_check,
    'Filter on each day of the week with "dt.weekday()", note "Monday==1".',
    '''
q8_monday = df.filter(pl.col("date").dt.weekday().eq(1)).select(pl.col("streams").mean())
q8_friday = df.filter(pl.col("date").dt.weekday().eq(5)).select(pl.col("streams").mean())
    '''
)

def q9_check(result):
    result = get_value(result)
    assert (result - datetime.timedelta(days=14, hours=15, minutes=39)).total_seconds() < 60

q9 = HintSolution(
    'en',
    'What is the average number of days in the data set since the beginning of each month?',
    q9_check,
    'Calculate the average ("mean") over the difference of the date and the start of the month ("dt.month_start()").',
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
    'en',
    'Create a dataframe with all collaborations "Zedd" has been involved in.',
    q10_check,
    'Filter on all artist names that contain Zedd but are not exactly Zedd. Use the "unique" function.',
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
    'en',
    'What is the highest chart position achieved by "Maren Morris" together with "Zedd"? And without him?',
    q11_check,
    'Filter on artist names that contain "Maren Morris" and/or not "Zedd". Use the smallest value of "rank".',
    '''
q11_without_zedd = (
    df.filter(
        pl.col("artist").str.contains("Maren Morris") &
        ~ pl.col("artist").str.contains("Zedd")
    )
    .select(pl.col("rank").min())
)

q11_with_zedd = (
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
    'en',
    '''
Minimize the memory consumption of the dataframe by using different data types and removing an unnecessary prefix.
You can display the memory consumption with df.estimated_size("mb").
    ''',
    q12_check,
    'Remove the prefix from the "url" column e.g. with "str.replace" or "str.slice", cast to "pl.Categorical" for all strings, UInt8 or UInt32 for the numbers.',
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
    'en',
    '''
Identify the 5 songs with the most streams over the entire time period.
    ''',
    q13_check,
    'Group by "title" and "artist", aggregate "streams" as a sum and filter with the "top_k" function.',
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
    'en',
    '''
Find out the 5 songs with the most streams over the whole time period and also
how many different "url"s there are per song ("urlCount") and one "url" per song (e.g. the first).
Listen to the songs with the function "play_song".
    ''',
    q14_check,
    'Like question 13, but additionally with "n_unique" (as "urlCount") and "first" on the "url" column.',
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
    'en',
    '''
Calculate for each song the romantic 💕 Valentine's Index ("valentinesIndex") as the average number of streams
on Valentine's Day divided by the average number of streams on all other days.
Filter on the 5 most romantic songs 😍 🎶 😍 that charted on Valentine's Day in each year.
Plot the streams for the most romantic song.
    ''',
    q15_check,
    '''
Create an auxiliary column "isValentinesDay", group by title and artist and determine in the aggregation the number of years
with "n_unique" and filter the aggregation expressions with the auxiliary column "isValentinesDay".
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
    'en',
    '''
Create a list of Christmas songs by filtering on all tracks that contain the word "Christmas".
Then group on "url" and find the top 3 songs with the most streams. Merge the original dataset and plot
the streams of the three most popular Christmas songs.
    ''',
    q16_check,
    '''
Use a custom name for the sum of all streams (e.g. "totalStreams"), use top_k and make a join on "df" with "url" as key.
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
    'en',
    '''
Load the file "track-genres.parquet". Then add these genres to the main data set and determine the 5 most streamed music genres.
    ''',
    q17_check,
    '''
Joine on the "url" column and roll out the "genres" column using the "explode" method before grouping, aggregating and filtering to the top 5.
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
    'en',
    '''
Get the percentage of the total streams for which we have one or more genres (e.g. 0.25 if there is a genre specification for 25% of the streams).
    ''',
    q18_check,
    '''
Use a left join. The result should either be a number smaller than 1 with 3 decimal places or a dataframe with exactly one row and one column.
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
    'en',
    '''
Create a dataframe that shows the number of No. 1 hits per year in separate columns for each artist.
Create an additional column for the total number of No. 1 hits ("allYears") and
filter on the 6 artists with the most No. 1 hits ("allYears").
    ''',
    q19_check,
    '''
Filter on the No. 1 hits, count the unique track names per year and artist, then pivot.
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
    'en',
    '''
Rewrite the sample solution of question 12 in such a way that the string columns are not selected individually with a name.
    ''',
    q12_check,
    'Remove all casts to Categorical from the sample solution and make the final cast in an additional "with_columns".',
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
    'en',
    '''
Identify the 10 regions with the most streams.
    ''',
    q21_check,
    'Group by "region", aggregate, and then top-k.',
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
    'en',
    '''
Additionally load the file region-info.csv and determine now the 10 regions with
the most streams relative to the population.
    ''',
    q22_check,
    'Load "region-info.csv" with "pl.scan_csv", divide the sum of the streams by "population".',
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
    'en',
    '''
Determine the different values for the "chart" column.
    ''',
    q23_check,
    'You can use the "unique" function.',
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
    'en',
    '''
Calculate per continent for how many Christmases there are entries for the "top200" (on both
on December 24 and 25). Call the column with the years "xmasYears".
    ''',
    q24_check,
    'Load "region-info.csv" with "pl.scan_csv" and join, filter on "top200" and Christmas and use "dt.year().n_unique()".',
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
    'en',
    '''
Difficult final boss: Calculate the top Christmas songs per continent.

- Create a dataframe with continent and number of Christmases (see q24)
- Then filter the dataset first for songs that were played on every Christmas,
that is included in the dataset for the continent.
- Then determine from these songs per continent which were played the most at Christmas
- Create a dataframe with the top-2 per continent
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
    'en',
    'Create a dataframe that shows the number of artists in the global top 10 per day.',
    q26_check,
    'Filter `df` on `rank` and use the method `n_unique`.',
    'q26_df = df.filter(pl.col("rank").le(10)).group_by("date").agg(pl.col("artist").n_unique()).sort("date")'
)

q27 = HintSolution(
    'en',
    'Create a line chart with the above dataframe, with the date on the x-axis and the number of artists on the y-axis.',
    None,
    'Use the method `px.line`.',
    'q27_fig = px.line(q26_df, x="date", y="artist", height=300)'
)

q28 = HintSolution(
    'en',
    'Add a new filter option that restricts to #1 hits.',
    None,
    'Use a `dmc.Switch` or `dmc.Checkbox` and filter the dataframe with `df.filter(pl.col("rank").eq(1))`.',
"""
all_artists = df.select(pl.col("artist").str.split(", ")).get_column("artist").explode().unique().sort().to_list()
all_titles = df.select(pl.col("title").unique()).get_column("title").sort().to_list()

app = Dash(external_stylesheets=dmc.styles.ALL)

def get_streams_chart(artist, title, rank_1_only):
    filter_expr = pl.lit(True) if artist is None else pl.col("artist").str.contains(artist)
    if title is not None:
        filter_expr = filter_expr & pl.col("title").eq(title)
    if rank_1_only:
        # consider all songs that were at least once on #1
        filter_expr = filter_expr & pl.col("rank").min().over("artist", "title").eq(1)
    data = df.filter(filter_expr).group_by("date").agg(pl.col("streams").sum()).sort("date")
    return px.line(data, x="date", y="streams", height=300, title=f"Daily Streams for {artist or 'all artists'} - {title or 'all titles'}")

app.layout = dmc.MantineProvider([
    dmc.Title("Spotify Explorer", order=3, mb=20),
    dmc.Group([
        dmc.Select(id="artist-select", label="Artist", placeholder="Select one", data=all_artists, searchable=True),
        dmc.Select(id="title-select", label="Title", placeholder="Select one", data=all_titles, searchable=True),
        # Switch for the option to display only songs that were at least once at #1
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