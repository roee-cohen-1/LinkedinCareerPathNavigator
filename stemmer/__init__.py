from pyspark.ml import Pipeline as _Pipeline
from sparknlp.annotator import Normalizer as _Normalizer
from sparknlp.annotator import StopWordsCleaner as _StopWordsCleaner
from sparknlp.annotator import Stemmer as _Stemmer
from sparknlp.annotator import Tokenizer as _Tokenizer
from sparknlp.base import DocumentAssembler as _DocumentAssembler
from pyspark.sql.functions import expr as _expr


class StemmingPipeline(_Pipeline):

    def __init__(self, col_to_stem):
        super().__init__(stages=[
            _DocumentAssembler() \
                .setInputCol(col_to_stem) \
                .setOutputCol('document'),
            _Tokenizer() \
                .setInputCols(['document'])
                .setOutputCol('token'),
            _Normalizer() \
                .setInputCols(['token']) \
                .setOutputCol('normalized') \
                .setLowercase(True).setCleanupPatterns(['[^\w\d\s]']),
            _StopWordsCleaner() \
                .setInputCols('normalized') \
                .setOutputCol('removed_stopwords') \
                .setCaseSensitive(False),
            _Stemmer() \
                .setInputCols(['removed_stopwords']) \
                .setOutputCol('stem')
        ])

    def transform(self, df):
        return super().fit(df).transform(df) \
            .withColumn('stem', _expr('transform(stem, x -> x.result)').alias('stem')) \
            .drop('document', 'token', 'normalized', 'removed_stopwords')

