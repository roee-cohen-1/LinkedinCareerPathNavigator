from pyspark.ml import Pipeline as _Pipeline
from pyspark.sql.functions import expr as _expr, concat_ws as _concat_ws, col as _col
from pyspark.sql.functions import coalesce as _coalesce, lit as _lit

_experience_expr = """
concat_ws('\n', transform(
        filter(
            flatten(
                transform(
                    experience, 
                    x -> flatten(
                        array(
                            array((x.title, x.subtitle, x.start_date, x.end_date, x.description)),
                            transform(
                                coalesce(x.positions, array()), 
                                y -> (y.title, y.subtitle, y.start_date, y.end_date, y.description)
                            )
                        )
                    )
                )
            ), 
            z -> z.subtitle is not null
        ),
    w -> concat_ws(' ', w.title, w.start_date, '-', w.end_date, ': ', w.description))
)
"""

_education_expr = """
concat_ws('\n', transform(
        transform(
            coalesce(education, array()), 
            y -> (y.degree, y.field, y.start_year, y.end_year)
        ),
    w -> concat_ws(' ', w.degree, w.field, w.start_year, '-', w.end_year))
)
"""

_certifications_expr = """
concat_ws('\n', transform(certifications, x -> x.title))
"""

_сourses_expr = """
concat_ws('\n', transform(fix, x -> x.title))
"""


def preprocess(df):
    return df \
        .withColumn('experience', _expr(_experience_expr)) \
        .withColumn('education', _expr(_education_expr)) \
        .withColumn('certifications', _expr(_certifications_expr)) \
        .withColumnRenamed('сourses', 'fix') \
        .withColumn('fix', _expr(_сourses_expr)) \
        .withColumn('profile_document',
                    _concat_ws(
                        '\n',
                        _col('experience'),
                        _col('education'),
                        _col('certifications'),
                        _col('fix'),
                        # some weird bug when trying to transform a column named 'сourses'... had to rename column
                        _coalesce(_col('about'), _lit(''))
                    )
                    )


# for testing
if __name__ == '__main__':
    import sparknlp

    spark = sparknlp.start()

    profiles = spark.read.parquet('/linkedin/people')
    preprocess(profiles).select('id', 'profile_document').printSchema()

