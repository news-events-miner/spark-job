import requests
import pyspark
import os
import sys
import pandas as pd
from event_detection.extractor import EventExtractor
from time import sleep
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import window
from pyspark.sql.types import (StructField, StructType, StringType,
                               TimestampType, IntegerType, BooleanType)
from requests.auth import HTTPBasicAuth

schema = StructType([
    StructField('ok', BooleanType(), False),
])


def apply_event_extraction(df: DataFrame):
    import pynndescent
    pynndescent.rp_trees.FlatTree.__module__ = "pynndescent.rp_trees"

    rows = list(map(lambda x: x[1], df.iterrows()))

    extractor = EventExtractor(embedding_model='doc2vec')
    events = extractor.extract_events(rows)
    statuses = []

    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASS')
    db_name = os.environ.get('DB_NAME')

    base_url = f'http://{db_host}:{db_port}/{db_name}'

    for i, event in enumerate(events):
        event['date'] = str(event['date'])

        succeed = False
        attempts = 10

        while attempts > 10 or not succeed:
            try:
                req = requests.post(base_url,
                                    json=event,
                                    verify=False,
                                    auth=HTTPBasicAuth(username=db_user,
                                                       password=db_password))
                succeed = True
            except Exception as e:
                print(f"Attempt to write to DB failed: {e}", file=sys.stderr)
                attempts -= 1
                sleep(10)
        ok = True
        if attempts == 0 or req.status_code != 201 or req.status_code != 202:
            ok = False
        statuses.append({'ok': ok})

    # pprint(events[0])

    res_df = pd.DataFrame(statuses)

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

    s3_path = os.environ.get('S3_PATH')

    context = pyspark.SparkContext()
    session = SparkSession.builder\
        .appName('EventMiner')\
        .config('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('fs.s3a.access.key', os.environ.get('S3_ACCESS_KEY'))\
        .config('fs.s3a.secret.key', os.environ.get('S3_SECRET_KEY'))\
        .config('fs.s3a.endpoint', os.environ.get('S3_ENDPOINT'))\
        .getOrCreate()

    conf = pyspark.SparkConf().getAll()
    print(*conf, sep='\n')

    df = session.readStream.csv(s3_path,
                                header=True,
                                schema=input_schema,
                                timestampFormat="%Y-%m-%d %H:%M:%S")

    grouped = df.groupBy(window("date", "1 day"))

    result = grouped.applyInPandas(apply_event_extraction, schema=schema)

    query = result.writeStream.outputMode('append').format('console').start()
    query.awaitTermination()

    session.stop()
