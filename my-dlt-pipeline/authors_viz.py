import marimo

__generated_with = "0.10.14"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import ibis
    import altair as alt
    return alt, ibis, mo


@app.cell
def _(ibis):
    con = ibis.duckdb.connect("open_library_pipeline.duckdb", read_only=True)
    authors = con.table("books__isbn_0451526538__authors", database="open_library_pipeline_dataset")
    return authors, con


@app.cell
def _(authors, alt, ibis):
    author_counts = authors.group_by("name").aggregate(book_count=authors.count()).order_by(ibis.desc("book_count")).limit(10)
    df = author_counts.execute()
    
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("book_count:Q", title="Book Count"),
        y=alt.Y("name:N", title="Author", sort="-x")
    ).properties(
        title="Top 10 Authors by Book Count",
        width=600,
        height=400
    )
    
    chart
    return author_counts, chart, df


if __name__ == "__main__":
    app.run()
