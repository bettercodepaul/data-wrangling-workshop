(deutsche Version: [🇩🇪](#deutsche-Version))
# Hands-on workshop: Data Wrangling with Polars & Dash - the turbo boost for your data project

Material (notebooks and data) for the hands-on workshop "Data Wrangling with Polars & Dash" by [BettercallPaul](https://www.bcxp.de).

The data on which the workshop is based are daily charts from Spotify from 2017-2021 for different countries.

You will learn how to use [Polars](https://pola.rs) to explore tabular data and how to visualize insights in a web applicaation using [Plotly Dash](https://dash.plotly.com).

At the end you will be able to build a web application that looks like this with just a few lines of code:

![screenshot](https://github.com/bettercodepaul/data-wrangling-praktikum/raw/master/images/screenshot.png)

The ingredients that make the workshop interesting and entertaining:

- 👐 Strict focus on practical application from the very beginning
- 💪 >25 exercises of increasing difficulty to make sure you can apply the concepts in practice
- 😀 an exercise system that includes on-demand hints and solutions make exercises fun
- 😮 surprising insights: What are the most popular Christmas songs? Which songs are suitable for a romantic evening?
- 🎧 the possibility to listen to your data wrangling: every song can be played directly in the jupyter notebook

For an introduction to Polars we recommend watching this talk from PyCon/PyData Berlin 2023 first: https://www.youtube.com/watch?v=CtkMzCIXOWk

## Prerequisites

You should have basic knowledge of a structured data processing technology, e.g. Pandas, SQL or Apache Spark.

## Using the notebooks

### Colab

The easiest way is to open the notebooks in Google Colab:

- [Colab Intro](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/colab_intro_en.ipynb) - Polars: Optional, if you don't know Jupyter/Colab
- [Part 1](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Polars_Part_1.ipynb) - Polars: load, select, filter & sort
- [Part 2](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Polars_Part_2.ipynb) - Polars: aggregations, joins & reshaping
- [Part 3](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Polars_Part_3.ipynb) - Polars: custom expressions, lazy mode and streaming
- [Part 4](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Dash_Part_4.ipynb) - Dash: layout, callbacks, Dash Mantine Components, AgGrid

### Local environment

A local environment is also possible. You may have to make a few adjustments to the notebooks and install Graphviz to display the execution plans.

We recommend [uv](https://docs.astral.sh/uv/) and (Visual Studio Code)[https://code.visualstudio.com] with its (Python Extension)[https://marketplace.visualstudio.com/items?itemName=ms-python.python].

## Additional material

- technical backgrounds from Ritchie, the original developer of Polars: http://www.ritchievink.com/blog/2021/02/28/i-wrote-one-of-the-fastest-dataframe-libraries/
- the cheat sheet: https://franzdiebold.github.io/polars-cheat-sheet/Polars_cheat_sheet.pdf

## Feedback

If you like the material please leave us a star.

Bugs, suggestions or request for additional topics? [Create an issue](https://github.com/bettercodepaul/data-wrangling-praktikum/issues/new)!

Would you like support for your data analytics/data science/machine learning project? We'd love to help, you just [BettercallPaul](mailto:sayhi@bcxp.de).

---

#### deutsche Version

# Praxisworkshop: Datenaufbereitung mit Polars & Dash – der Turbo-Boost für dein Datenprojekt

Material (Notebooks und Daten) für den Praxisworkshop "Datenaufbereitung mit Polars & Dash" von [BettercallPaul](https://www.bcxp.de).

Die Daten, auf denen der Workshop basiert, sind tägliche Charts von Spotify von 2017 bis 2021 für verschiedene Länder.

Du wirst lernen, wie du mit [Polars](https://pola.rs) tabellarische Daten analysierst und wie du Erkenntnisse mithilfe von [Plotly Dash](https://dash.plotly.com) in einer Webanwendung visualisierst.

Am Ende wirst du mit wenigen Codezeilen eine Webanwendung erstellen können, die so aussieht:

![Screenshot](https://github.com/bettercodepaul/data-wrangling-praktikum/raw/master/images/screenshot.png)

Die Zutaten, die den Workshop interessant und unterhaltsam machen:

- 👐 Strikter Fokus auf die praktische Anwendung von Anfang an
- 💪 >25 Übungen mit steigendem Schwierigkeitsgrad, um sicherzustellen, dass du die Konzepte in der Praxis anwenden kannst
- 😀 ein Übungssystem mit On-Demand-Tipps und Lösungen sorgt dafür, dass die Übungen Spaß machen
- 😮 überraschende Insights: Welches sind die beliebtesten Weihnachtslieder? Welche Lieder eignen sich für einen romantischen Abend?
- 🎧 die Möglichkeit, sich die eigenen Datenanalysen anzuhören: Jedes Lied kann direkt im Jupyter-Notebook abgespielt werden

Für eine Einführung in Polars empfehlen wir, zunächst diesen Vortrag von der PyCon/PyData Berlin 2023 anzusehen: https://www.youtube.com/watch?v=CtkMzCIXOWk

## Voraussetzungen

Du solltest Grundkenntnisse in einer strukturierten Datenverarbeitungstechnologie haben, z.B. Pandas, SQL oder Apache Spark.

## Nutzung der Notebooks

### Colab

Am einfachsten ist es die Notebooks in Google Colab zu öffnen:

- [Colab Intro](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/colab_intro_de.ipynb) – Optional, falls dir Jupyter/Colab nicht bekannt ist
- [Teil 1](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Polars_Teil_1.ipynb) – Polars: Laden, Selektieren, Filtern & Sortieren
- [Teil 2](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Polars_Teil_2.ipynb) – Polars: Aggregationen, Joins & Reshaping
- [Teil 3](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Polars_Teil_3.ipynb) – Polars: Custom Expressions, Lazy Mode und Streaming
- [Teil 4](https://colab.research.google.com/github/bettercodepaul/data-wrangling-praktikum/raw/master/Dash_Teil_4.ipynb) – Dash: Layout, Callbacks, Dash Mantine Components, AgGrid


### Lokale Umgebung

Eine lokale Umgebung ist ebenfalls möglich. Gegebenenfalls musst du ein paar Anpassungen an den Notebooks vornehmen und Graphviz installieren, um Ausführungspläne anzuzeigen.

Wir empfehlen [uv](https://docs.astral.sh/uv/) und [Visual Studio Code](https://code.visualstudio.com) mit der [Python-Erweiterung](https://marketplace.visualstudio.com/items?itemName=ms-python.python).

## Zusätzliches Material

- technische Hintergründe von Ritchie, dem ursprünglichen Entwickler von Polars: http://www.ritchievink.com/blog/2021/02/28/i-wrote-one-of-the-fastest-dataframe-libraries/

- ein Cheat-Sheet: https://franzdiebold.github.io/polars-cheat-sheet/Polars_cheat_sheet.pdf

## Feedback

Wenn dir das Material gefällt, hinterlasse uns bitte einen Stern.

Bugs, Vorschläge oder Wünsche für zusätzliche Themen? [Erstelle ein Ticket](https://github.com/bettercodepaul/data-wrangling-praktikum/issues/new)!

Benötigst du Unterstützung für dein Projekt im Bereich Data Analytics/Data Science/Machine Learning? Wir würden uns freuen zu helfen, you just [BettercallPaul](mailto:sayhi@bcxp.de).
