import pyspark
import os
import pandas as pd
from pprint import pprint
from event_detection.extractor import EventExtractor
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import window
from pyspark.sql.types import (StructField, StructType, StringType,
                               TimestampType, IntegerType, ArrayType,
                               FloatType)

schema = StructType([
    StructField('date', TimestampType(), True),
    StructField('place', StringType(), True),
    StructField('keywords', ArrayType(StringType()), False),
    StructField('doc_ids', ArrayType(IntegerType()), False),
    StructField('doc_scores', ArrayType(FloatType()), False),
    StructField('doc_texts', ArrayType(StringType()), False),
    StructField('kw_scores', ArrayType(FloatType()), False)
])


def apply_event_extraction(df: DataFrame):
    import pynndescent
    pynndescent.rp_trees.FlatTree.__module__ = "pynndescent.rp_trees"

    rows = list(map(lambda x: x[1], df.iterrows()))

    extractor = EventExtractor(embedding_model='doc2vec')
    events = extractor.extract_events(rows)

    # pprint(events[0])

    res_df = pd.DataFrame(events)

    return res_df


input_schema = StructType([
    StructField('date', TimestampType(), False),
    StructField('id', IntegerType(), False),
    StructField('title', StringType(), False),
    StructField('text', StringType(), False)
])

if __name__ == "__main__":
    # Workaround
    # https://github.com/lmcinnes/umap/issues/477#issuecomment-862017068
    import pynndescent
    pynndescent.rp_trees.FlatTree.__module__ = "pynndescent.rp_trees"

    input_file = os.environ.get('SPARK_JOB_INPUT_PATH')

    context = pyspark.SparkContext()
    session = SparkSession.builder.appName('EventMiner').getOrCreate()

    df = session.readStream.csv(input_file,
                                header=True,
                                schema=input_schema,
                                timestampFormat="%Y-%m-%d %H:%M:%S")
    grouped = df.groupBy(window("date", "1 day"))

    result = grouped.applyInPandas(apply_event_extraction, schema=schema)

    query = result.writeStream.outputMode('append').format('console').start()
    query.awaitTermination()

    session.stop()
