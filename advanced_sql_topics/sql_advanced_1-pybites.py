import marimo

__generated_with = "0.13.14"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Some Advanced SQL Topics

    Recommended follow up material from U Tübingen: 

      - [Advanced SQL Playlist 2020](https://www.youtube.com/watch?v=HAI5DG_l60k&list=PL1XF9qjV8kH12PTd1WfsKeUQU6e83ldfc) with [materials 2025](https://github.com/DBatUTuebingen-Teaching/asql-ss24)
      - [Database Internals (optimizations)](https://www.youtube.com/watch?v=pq8KHeqS2NU&list=PL1XF9qjV8kH0ghGRGo3_f-FWqWvAbv1dh)
      - and more (when you understand German).

    ## Prerequisites

    ### Databases
    We will run a Postgres database in a Docker container. The command used is:

    `docker run -p 5432:5432 -e POSTGRES_PASSWORD=<as set in secrets> -d --name pbpg postgres:latest`

    **Warning**: As the Postgres database is located in the container, it will disappear when the `pbpg` container is purged. This demo builds from scratch, so no problems here. But if there is more data already in...

    ### Packages to be Installed
    - **marimo** – the notebook runtime  
    - **sqlalchemy** – the query builder / ORM used for SQLite and PostgreSQL  
    - **pandas** and **polars** – dataframe libraries used for displaying tables  
    - **duckdb** – one of our databases  

    ## Flow
    This notebook starts with the required imports. We will use 3 relational DBMSs: DuckDB, SQLite, and PostgreSQL. The first two run in the Marimo process, whereas Postgres runs in a Docker container accessed over the network.

    DuckDB uses its own API directly to the DBMS. SQLite and PostgreSQL are under the control of SQLAlchemy, which is mainly used as a query builder and abstraction layer.  
    The databases are ephemeral. Postgres runs in the container and will disappear when the container is dropped. The other two are local files, which can be inspected externally but can be discarded at will.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Imports""")
    return


@app.cell
def _():
    import marimo as mo
    import os
    import sqlalchemy as sa
    import pandas as pd
    import polars as pl
    import duckdb
    import matplotlib.pyplot as plt
    return duckdb, mo, os, pl, plt, sa


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ##Create Database Engines
    Also read the tables directory and use each .csv file to create a table in each database.
    """
    )
    return


@app.cell
def _(duckdb):
    _DATABASE_URL = "demo.duckdb"
    ddb_eng = duckdb.connect(_DATABASE_URL, read_only=False)
    return (ddb_eng,)


@app.cell
def _(mo, os, sa):
    _password = os.environ.get("POSTGRES_PASSWORD", "pybites")
    _DATABASE_URL = f"postgresql://postgres:{_password}@localhost:5432/postgres"
    pg_eng = sa.create_engine(_DATABASE_URL)

    try:
        with pg_eng.connect() as connection:
            connection.execute(sa.text("SELECT 'pong'"))
    except Exception as e:
        print("No working Postgres found.", e)
        mo.stop(True, mo.md("##No Postgres!\nDid you start a local PostgreSQL on port 5432?<br>See instructions at the beginning of this Marimo notebook."))
    return (pg_eng,)


@app.cell
def _(sa):
    _DATABASE_URL = "sqlite:///demo.sqlite"
    lite_eng = sa.create_engine(_DATABASE_URL)
    return (lite_eng,)


@app.cell
def _(ddb_eng, lite_eng, os, pg_eng, sa):
    def create_test_table(name):
        """
        Create table <name> in all three databases. Drop existing table if any first. Get data from tables/<name>.csv
        Warning:
          - existing table will be dropped.
          - constructing a query as f-string does not protect against SQL injection and is to be avoided in production code.
        """
        ddb_eng.execute(
            f"DROP TABLE IF EXISTS {name};"
        )  # Drop existing table if any
        ddb_eng.execute(f"CREATE TABLE {name} AS SELECT * FROM read_csv_auto('tables/{name}.csv')")

        table_df = ddb_eng.execute(f"SELECT * FROM {name}").fetchdf()
        print("DuckDB \u2713. ", end="")

        with pg_eng.connect() as con_out:
            con_out.execute(sa.text(f"DROP TABLE IF EXISTS {name}; COMMIT;"))
        table_df.to_sql(name, pg_eng, index=False)
        print("PostgreSQL \u2713. ", end="")

        with lite_eng.connect() as con_out:
            con_out.execute(sa.text(f"DROP TABLE IF EXISTS {name};"))
        table_df.to_sql(name, lite_eng, index=False)
        print("SQLite \u2713.")


    # Get all .csv files in the tables directory and create test tables
    csv_files = [f for f in os.listdir("tables") if f.endswith(".csv")]
    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[
            0
        ]  # Get the basename without extension
        print(f"Creating table {table_name} in all three databases. ", end="")
        create_test_table(table_name)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Selecting and Carthesian Product
    We select the data from the tables T1 and T2 from each database without a WHERE clause into a Polars df to compare the results.
    """
    )
    return


@app.cell
def _(ddb_eng, mo, t1, t2):
    # Carthesian 
    _df = mo.sql(
        f"""
        -- Duckdb  (connection ddb_eng)
        SELECT * FROM t1, t2;
        """,
        engine=ddb_eng
    )
    return


@app.cell
def _(mo, pg_eng, t1, t2):
    _df = mo.sql(
        f"""
        -- Postgres  (connection pg_eng)
        SELECT * FROM t1, t2;
        """,
        engine=pg_eng
    )
    return


@app.cell
def _(lite_eng, mo, pl, t1, t2):
    # SQLite (connection lite_eng)   SQLite returns bool as their int equivalent, so the dataframe needs a cast to properly display.
    #_df = mo.sql("SELECT * FROM t1, t2;", engine=lite_eng)
    _df = mo.sql("SELECT * FROM t1, t2;", engine=lite_eng).with_columns(pl.col("c").cast(pl.Boolean))
    _df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## TABLE 

    The TABLE statment is syntactic sugar for `SELECT * FROM`.

    It is not (yet?) supported by SQLite.
    """
    )
    return


@app.cell
def _(mo, pg_eng, t1):
    _df = mo.sql(
        f"""
        TABLE t1;
        """,
        engine=pg_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Learnings
    - Order in the row-set (or bag) returned from a select statement is **undefined**. There actually may be an order, but this is defined by the engine and may change, depending on DBMS version, storage engine, data statistics, query plan.  Morale: if you need order always do an explicit sort.
    - Despite usign an ORM, there are differences between the DBMSes.
      Example: bool being a sub type of int may come out as int.
    - When you write the SQL yourself, the ORM cannot generate the syntactical differences between the dialects away.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Joining
    Selecting from two tables without a WHERE clause results in a cartesian product giving back each row from each table combined with all rows in all other tables. This carthesian product is known as **CROSS JOIN**.  In SQL you can use this as syntactic sugar to make your intention more clear.
    """
    )
    return


@app.cell
def _(ddb_eng, mo, t1, t2):
    _df = mo.sql(
        f"""
        SELECT *
        FROM t1
        CROSS JOIN t2;
        -- No ON allowed for a CROSS JOIN
        -- ON t1.t2_aa = t2.aa;
        """,
        engine=ddb_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    You bet! Here's the text with markdown formatting for improved readability and emphasis:

    We normally don't want this. We only want rows combined where one (or more) fields of each of the tables are equal. We call these **equi-joins**. We make an equi-join by just filtering out all combinations from the **Cartesian product** that we don't want. This is done by adding a `WHERE` clause to the SQL statement. For an equi-join, we just add a predicate like `t1.t2_aa = t2.aa` to the `WHERE` clause as shown below.

    ---

    ### Key Points about the `WHERE` Clause

    * The `WHERE` clause can be used to filter on much more, not only on equality between fields in the rows from the tables in the `FROM` statement.
    * There can only be one `WHERE` clause per `SELECT` statement, but you can combine predicates with `AND` and `OR` and group them with parentheses `()`.
    """
    )
    return


@app.cell
def _(lite_eng, mo, t1, t2):
    _df = mo.sql(
        f"""
        -- With * all rows will be displayed. So the columns used in the join will have identical data.
        -- Try the 3 selects here by changing the commenting.  Also try the databases with the engine combo box below.

        --SELECT *
        --SELECT t1.*, t2.*
        SELECT t1.*, t2.bb
        FROM t1, t2
        WHERE t1.t2_aa = t2.aa
        """,
        engine=lite_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Just like for the Carthesian Product (CROSS JOIN) there is also a syntactic sugar for the equi-join. You can use the `JOIN` statement. The `JOIN` statement is also called an **INNER JOIN**. It is the default join type. So you can just write `JOIN` instead of `INNER JOIN`. The syntax is:

    ```sql
    SELECT <columns>
    FROM <table1>
    JOIN <table2>
    ON <predicate>;
    ```
    Lets try one.
    """
    )
    return


@app.cell
def _(lite_eng, mo, t1, t2):
    _df = mo.sql(
        f"""
        -- BOTH INNER JOIN and just JOIN work.  The INNER is default.
        SELECT lft.*, rght.bb
        FROM t1 as lft
        -- INNER JOIN t2 as rght
        JOIN t2 as rght
        ON lft.t2_aa = rght.aa;
        """,
        engine=lite_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    I did something else too.  When writing SQL expressions you deal with ROW variables.  As operands you use the ROW variables in the SELECT statement and in the WHERE statement. These are not tables, but the ROWS in the table.  They are similar to a named tuple in Python.  Within the ROW variable you can access the individual fields with dot notation in their name.

    In the example above I have used `lft` and `rght` as the ROW variables. If you don't assign a nam there, SQL will give use a default name. This is the table_name in lower case. However, his will only work with decent table names.

    In many cases the default is ok.  But if the table name is long, you may want something shorter.  And when you need the same table twice or more in the same query, you'll need to name it explicitly too.

    But what about the `INNER` in the `INNER JOIN`?
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.image("img/joins.jpg")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    In the Venn diagrams a circle depicts a Set (or Bag) with the row-halves coming from each table.   In the `INNER JOIN` only the ones are kept that have a matching counterpart in the other table.

    In the `LEFT JOIN` also the ones from the left table are kept.  Since there is no matching row in the right table, here all fields will be `NULL`.

    The `RIGHT JOIN` is the same but mirrored.  You don't see it often, because any `RIGHT JOIN` can be rewritten as a `LEFT JOIN` by just switching the order of the tables in the `FROM / JOIN` statement.

    Finally the `FULL OUTER JOIN` is like the `LEFT JOIN` and `RIGHT JOIN` combined.
    """
    )
    return


@app.cell
def _(ddb_eng, mo, t1, t2):
    _df = mo.sql(
        f"""
        SELECT *
        FROM t1 as lft
        LEFT JOIN t2 as rgt
        ON lft.t2_aa = rgt.aa;
        """,
        engine=ddb_eng
    )
    return


@app.cell
def _(mo, pg_eng, t1, t2):
    _df = mo.sql(
        f"""
        SELECT *
        FROM t1 as lft
        RIGHT JOIN t2 as rgt
        ON lft.t2_aa = rgt.aa;
        """,
        engine=pg_eng
    )
    return


@app.cell
def _(mo, pg_eng, t1, t2):
    _df = mo.sql(
        f"""
        SELECT *
        FROM t1 as lft
        FULL OUTER JOIN t2 as rgt
        ON lft.t2_aa = rgt.aa;
        """,
        engine=pg_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""An application of the `LEFT/RIGHT/FULL JOIN` is to find rows that have no counterpart.""")
    return


@app.cell
def _(mo, pg_eng, t1, t2):
    _df = mo.sql(
        f"""
        SELECT *
        FROM t1
        FULL JOIN t2
        ON t1.t2_aa = t2.aa
        WHERE t1 is NULL OR t2 IS NULL;
        """,
        engine=pg_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    - As you can see, we test for the whole `ROW` variable being `NULL`; there's no need to check all fields separately.
    - Also, we **must** use `IS` for a comparison to `NULL`. This is because any expression with `NULL` as an operand anywhere yields `NULL`. So, the result of `NULL = NULL` is `NULL`, which is neither `TRUE` nor `FALSE`, whereas `NULL IS NULL` yields `TRUE`.
    - SQL isn't picky about `TRUE` and `FALSE`. All cases work. You can use `1` and `0` too. And **(DBMS-specific)** the strings 'Yes', 'YES', 'y', 'NO', 'No', 'n', 'on', 'off' may work too. But try first; you're on **thin ice**!
    """
    )
    return


@app.cell
def _(mo, pg_eng):
    _df = mo.sql(
        f"""
        SELECT 'Yes' = true as a, 'No' = true as b;
        """,
        engine=pg_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    OK, enough about `JOIN`.

    ## `GROUP BY`
    """
    )
    return


@app.cell
def _(lite_eng, mo, sensors):
    _df = mo.sql(
        f"""
        SELECT *
        FROM sensors AS s
        ORDER BY s.sensor_id, s.value
        LIMIT 10;
        """,
        engine=lite_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Suppose we want the average value per sensor_id.  We can do that with a ``GROUP BY` clause.""")
    return


@app.cell
def _(ddb_eng, mo, sensors):
    _df = mo.sql(
        f"""
        SELECT s.sensor_id, avg(s.value) AS avg_value, count(s.value) AS n_rows
        FROM sensors AS s
        GROUP BY s.sensor_id;
        """,
        engine=ddb_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""But we are only interested in the ones after 11:00:00.""")
    return


@app.cell
def _(ddb_eng, mo, sensors):
    _df = mo.sql(
        f"""
        SELECT s.sensor_id, AVG(s.value)AS avg_value, count(value) AS n_rows
        FROM sensors AS s
        --WHERE strftime('%H', s.timestamp) > '10'  -- OK for SQLite and DuckDB, not for postgres
        WHERE EXTRACT(HOUR FROM s.timestamp) > 10 -- OK for Postgres and DuckDB, not for SQLite
        GROUP BY s.sensor_id;
        """,
        engine=ddb_eng
    )
    return


@app.cell
def _(mo):
    mo.md(r"""And of course we only want the groups where the magic number is even.""")
    return


@app.cell
def _(ddb_eng, mo, sensors):
    _df = mo.sql(
        f"""
        SELECT s.sensor_id
            , round(n%3) AS magic
            , AVG(s.value) AS avg_value
          , count(value) AS n_rows
        FROM sensors AS s
        --WHERE strftime('%H', s.timestamp) > '10'  -- OK for SQLite and DuckDB, not for postgres
        WHERE EXTRACT(HOUR FROM s.timestamp) > 10 -- OK for Postgres and DuckDB, not for SQLite
        GROUP BY s.sensor_id, magic
        HAVING magic % 2 = 0
        ORDER BY s.sensor_id, magic
        ;
        """,
        engine=ddb_eng
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Recursion

    Consider this table:
    """
    )
    return


@app.cell
def _(employee, lite_eng, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM employee;
        """,
        engine=lite_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Every employee has their unique emp_id, bit also a boss_id, which is a *foreign key* to the *same table*.
    Lets use this to find each employee's boss.
    """
    )
    return


@app.cell
def _(employee, mo, pg_eng):
    _df = mo.sql(
        f"""
        -- SELECT *  -- Works in DuckDB but not in the others
        SELECT emp.*, boss.emp_name as boss_name -- always works and better column names.
        FROM employee AS emp
        LEFT JOIN employee as boss  -- Need a LEFT JOIN to have the boss-less CEO included.
        ON emp.boss_id = boss.emp_id
        ORDER BY 2; -- The ORDER BY can select fields by position in the SELECT.
        """,
        engine=pg_eng
    )
    return


@app.cell
def _(employee, mo, pg_eng):
    _df = mo.sql(
        f"""
        WITH RECURSIVE
            top_brass AS (
                SELECT *
                FROM employee
                WHERE boss_id IS NULL
            ),
            the_rest AS (
                SELECT *
                FROM employee
                WHERE boss_id IS NOT NULL 
            ),
            hier_emp AS ( 
                SELECT *, 1 AS level
                FROM top_brass
                    UNION ALL
                SELECT tr.*, he.level + 1
                FROM the_rest AS tr
                JOIN hier_emp AS he
                ON tr.boss_id = he.emp_id
            )
        --SELECT * FROM hier_emp;
        SELECT he.emp_id, he.emp_name, bn.emp_name as boss_name, he.level
        FROM hier_emp AS he
        LEFT JOIN employee AS bn
        ON he.boss_id = bn.emp_id
        ORDER BY he.level, boss_name;
        """,
        engine=pg_eng
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Window Functions
    * A **window function** yields a scalar value, computed over one or more rows in a bag (or set). The bag is defined in the `OVER` clause.
    * The syntax is:<br> `WIN_FUNCTION(...) OVER( [PARTITION BY ...]  [ORDER BY ...]  [<frame/window\>] )`
    * The selected rows are optionally first **partitioned**, or grouped with the optional `PARTITION BY` clause.
    * In many cases order in the partition matters, so a sort can be done using the optional `ORDER BY` clause.
    * Next a **window or frame**, anchored by the **current row**, slides over the data, as specified in the optional window/frame clause.
    * A **window function** is then applied to each row in the selection, returning a scalar computed over the window anchored by that row.
    * Examples of window functions include:
        * relative row position functions: 
            * `FIRST_VALUE()`
            * `LAG()`
            * `LAST_VALUE()`
            * `LEAD()`
            * ...
        * row ranking:
            * `RANK()`
            * `DENSE_RANK()`
            * ...
        * and the usual aggregate functions like:
            * `SUM()`
            * `AVG()`
            * `COUNT()`
            * ...
    """
    )
    return


@app.cell
def _(ddb_eng, mo, sensors):
    sens_df1 = mo.sql(
        f"""
        SELECT s.sensor_id, s.timestamp, s.value FROM sensors s;
        """,
        engine=ddb_eng
    )
    return (sens_df1,)


@app.cell
def _(plt, sens_df1):
    _sensor_1_data = sens_df1.filter(sens_df1['sensor_id'] == 1)
    _sensor_2_data = sens_df1.filter(sens_df1['sensor_id'] == 2)

    # Create the plot
    plt.figure(figsize=(10, 5))
    plt.plot(_sensor_1_data['timestamp'].to_numpy(), _sensor_1_data['value'].to_numpy(), color='blue', marker='o', linestyle='-', label='Sensor 1 value')
    plt.plot(_sensor_2_data['timestamp'].to_numpy(), _sensor_2_data['value'].to_numpy(), color='red', marker='o', linestyle='-', label='Sensor 2 value')
    plt.title('Sensor Values Over Time')
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.xticks(rotation=30)
    plt.grid()
    plt.legend()
    plt.tight_layout()
    plt.show()
    return


@app.cell
def _(mo, pg_eng, sensors):
    sens_df2 = mo.sql(
        f"""
        SELECT s.sensor_id, s.timestamp, s.value,
            avg(s.value) OVER
            (
                PARTITION BY s.sensor_id
                ORDER BY s.timestamp
                ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
            ) AS mv_avg_val
        FROM sensors as s;
        """,
        engine=pg_eng
    )
    return (sens_df2,)


@app.cell
def _(plt, sens_df1, sens_df2):
    _sensor_1_data = sens_df2.filter(sens_df1['sensor_id'] == 1)
    _sensor_2_data = sens_df2.filter(sens_df1['sensor_id'] == 2)

    # Create the plot
    plt.figure(figsize=(10, 5))
    plt.plot(_sensor_1_data['timestamp'].to_numpy(), _sensor_1_data['mv_avg_val'].to_numpy(), color='blue', marker='', linestyle='-', label='Sensor 1 mv_avg_val')
    plt.plot(_sensor_2_data['timestamp'].to_numpy(), _sensor_2_data['mv_avg_val'].to_numpy(), color='red', marker='', linestyle='-', label='Sensor 2 mv_avg_val')
    plt.plot(_sensor_1_data['timestamp'].to_numpy(), _sensor_1_data['value'].to_numpy(), color='lightblue', marker='o', linestyle='dotted', label='Sensor 1 value')
    plt.plot(_sensor_2_data['timestamp'].to_numpy(), _sensor_2_data['value'].to_numpy(), color='orange', marker='o', linestyle='dotted', label='Sensor 1 value')
    plt.title('Sensor Values Over Time')
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.xticks(rotation=30)
    plt.grid()
    plt.legend()
    plt.tight_layout()
    plt.show()
    return


@app.cell
def _(ddb_eng, mo, sensors):
    sens_df3 = mo.sql(
        f"""
        SELECT s.sensor_id, s.timestamp, s.value,
            ROUND(
                (s.value - LAG(s.value, 1, s.value)
                          OVER (
                              PARTITION BY s.sensor_id
                              ORDER BY s.timestamp
                          )
                ), 3 -- Error for Postgres, optional for DuckDB, mandatory for SQLite.
        --        )::numeric, 3   -- Mandatory for Postgres, optional for DuckDB, error for SQLite.
            ) AS value_difference
        FROM sensors AS s
        WHERE sensor_id = 1;
        """,
        engine=ddb_eng
    )
    return (sens_df3,)


@app.cell
def _(plt, sens_df3):
    # Create the plot for value_difference over timestamp
    plt.figure(figsize=(10, 5))
    plt.plot(sens_df3['timestamp'].to_numpy(), sens_df3['value_difference'].to_numpy(), color='purple', marker='o', linestyle='-', label='Value Difference')
    plt.title('Δ Value Over Time')
    plt.xlabel('Time')
    plt.ylabel('Δ Value')
    plt.xticks(rotation=30)
    plt.grid()
    plt.legend()
    plt.tight_layout()
    plt.show()
    return


if __name__ == "__main__":
    app.run()
