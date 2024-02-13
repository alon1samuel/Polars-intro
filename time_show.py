# On polars 0.17.5

import polars as pl 

df = pl.DataFrame(
    {
        "col1": ["abc", "def", "ghi"],
        "col2": [["cat", "dog", "mouse", "bird"], ["cat", "dog"], ["snail", "worm"]],
        "col3": [["cat", "bird"], ["dog", "dog"], ["mouse", "bird", "starfish"]],
    }
)

nbr_groups = 34_000_000
df = (
    df
    .join(
        pl.DataFrame({
            'group': pl.arange(0, nbr_groups, eager=True)
        }),
        how="cross"
    )
)
print(df)

import time

start = time.perf_counter()
df.with_columns(
    pl.col("col2")
    .arr.unique()
    .arr.concat(pl.col('col3').arr.unique())
    .arr.eval(pl.element().filter(pl.element().is_duplicated()), parallel=True)
    .arr.unique()
    .alias('intersection')
)
print(time.perf_counter() - start)
