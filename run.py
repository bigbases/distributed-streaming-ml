import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

!export PYSPARK_PYTHON=python3.6.9

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, Dense, Conv2D
from tensorflow.keras.layers import MaxPooling2D, Dropout,Flatten
from tensorflow.keras import backend as K
from tensorflow.keras.models import Model
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras import layers
from tensorflow.keras.layers import Embedding
import subprocess
import tensorflow as tf
from sklearn.feature_extraction import _stop_words
from sklearn.feature_extraction.text import CountVectorizer
from collections import Counter
from nltk.corpus import brown
from tensorflow.keras.layers.experimental.preprocessing import TextVectorization
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from pyspark.streaming.kafka import KafkaUtils
import pyspark.sql.functions as f
import json
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import sys
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import lit
import pickle
import tensorflow.keras
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from numpy import zeros
from keras.models import model_from_json
from tensorflow import keras
from pyspark.sql.functions import udf
import pandas as pd
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from pyspark.streaming.kafka import KafkaUtils
import pyspark.sql.functions as f
import json
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import sys
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import lit

import pickle
import keras
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from numpy import zeros
from keras.models import model_from_json
import keras
from keras.models import Sequential
from keras.layers import Input, Dense, Conv2D
from keras.layers import MaxPooling2D, Dropout,Flatten
from keras import backend as K
from keras.models import Model
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras import layers
from tensorflow.keras.layers import Embedding
import subprocess
import nltk

spark.sparkContext.getConf().getAll()

import re

def regex_(text):
    # 영어, 숫자, 특수만문자 제외 삭제.  
    text = text.dropna(axis=0)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+/(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    text['total_text_list'] = text['total_text_list'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['total_text_list'] = text['total_text_list'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https):// (?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['total_text_list'] = text['total_text_list'].str.replace(pat=pattern,repl=r'',regex=True)
    text['total_text_list'] = text['total_text_list'].str.replace(pat=r'(\@\w+.*?) ',repl=r'',regex=True)
    text['total_text_list'] = text['total_text_list'].str.replace(pat=r'[^ a-zA-Z]',repl=r'',regex=True)
    text['total_text_list'] = text['total_text_list'].str.lower()
    text=text[((text['total_text_list'].str.len()>= 10)) & ((text['total_text_list'].eq(' ')==False) & (text['total_text_list'].eq('')==False))]
    return text
def regex2_(text):
    # 영어, 숫자, 특수만문자 제외 삭제.  
    text = text.dropna(axis=0)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+/(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    text['text'] = text['text'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['text'] = text['text'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https):// (?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['text'] = text['text'].str.replace(pat=pattern,repl=r'',regex=True)
    text['text'] = text['text'].str.replace(pat=r'(\@\w+.*?) ',repl=r'',regex=True)
    text['text'] = text['text'].str.replace(pat=r'[^ a-zA-Z]',repl=r'',regex=True)
    text['text'] = text['text'].str.lower()
    text=text[((text['text'].str.len()>= 10)) & ((text['text'].eq(' ')==False) & (text['text'].eq('')==False))]
    return text

def regex_3(text):
    # 영어, 숫자, 특수만문자 제외 삭제.  
    text = text.dropna(axis=0)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+/(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    text['full_text'] = text['full_text'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['full_text'] = text['full_text'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https):// (?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['full_text'] = text['full_text'].str.replace(pat=pattern,repl=r'',regex=True)
    text['full_text'] = text['full_text'].str.replace(pat=r'[^ a-zA-Z]',repl=r'',regex=True)
    # @로 시작하는 애들도 정규식 이용해서 없애기 @[^ ]+
    text['full_text'] = text['full_text'].str.replace(pat=r'(\@\w+.*?) ',repl=r'',regex=True)
    text['full_text'] = text['full_text'].str.lower()
    text=text[((text['full_text'].str.len()>= 10)) & ((text['full_text'].str.split(" ").str.len()>= 10)) & ((text['full_text'].eq(' ')==False) & (text['full_text'].eq('')==False))]
    return text

def regex_4(text):
    # 영어, 숫자, 특수만문자 제외 삭제.  
    text = text.dropna(axis=0)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+/(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    text['full_text'] = text['full_text'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['full_text'] = text['full_text'].str.replace(pat=pattern,repl=r'',regex=True)
    pattern = '(http|ftp|https):// (?:[-\w.]|(?:%[\da-fA-F]{2}))+'  # URL제거
    text['full_text'] = text['full_text'].str.replace(pat=pattern,repl=r'',regex=True)
    text['full_text'] = text['full_text'].str.replace(pat=r'[^ a-zA-Z]',repl=r'',regex=True)
    # @로 시작하는 애들도 정규식 이용해서 없애기 @[^ ]+
    text['full_text'] = text['full_text'].str.replace(pat=r'(\@\w+.*?) ',repl=r'',regex=True)
    text['full_text'] = text['full_text'].str.lower()
    #text=text[((text['full_text'].str.len()>= 10)) & ((text['full_text'].str.split(" ").str.len()>= 10)) & ((text['full_text'].eq(' ')==False) & (text['full_text'].eq('')==False))]
    return text



if __name__ == "__main__":
    general = pd.read_csv('general.csv', sep=',',  lineterminator='\n')
    general = general.drop_duplicates()
    general = spark.createDataFrame(general[['total_text_list']])
    general = general.select(col('total_text_list').alias('text')) 
    general = general.withColumn('label', lit(0))
    general.show(5)
    print(general.count())
    general = general.withColumn("label",col("label").cast("integer"))
    
    # 보안 관련 계정들 트윗
    security = pd.read_csv('dataset.csv', sep=',',  lineterminator='\n')
    security = security[["text","datetime"]]
    security = security.sort_values(by='datetime' ,ascending=True)
    security = security.reset_index(drop=True)
    security = regex2_(security)
    security = security[~security['text'].str.contains("rt", na=False, case=False)]
    security = security.drop_duplicates()
    
    security = spark.createDataFrame(security[['text']])
    security = security.withColumn('label', lit(1))
    security = security.withColumn("label",col("label").cast("integer"))

    pretrained1 = security.collect()[:200000]
    pretrained2 = general.collect()[:200000]


    pretrained1 = spark.createDataFrame(pretrained1)
    pretrained2 = spark.createDataFrame(pretrained2)
    pre = pretrained1.union(pretrained2)
    t1 = security.collect()[200000:250000]
    t2 = security.collect()[250000:300000]
    t3 = security.collect()[300000:350000]
    t4 = security.collect()[350000:400000]

    t5 = general.collect()[200000:250000]
    t6 = general.collect()[250000:300000]
    t7 = general.collect()[300000:350000]
    t8 = general.collect()[350000:400000]


    t1 = spark.createDataFrame(t1)
    t2 = spark.createDataFrame(t2)
    t3 = spark.createDataFrame(t3)
    t4 = spark.createDataFrame(t4)
    t5 = spark.createDataFrame(t5)
    t6 = spark.createDataFrame(t6)
    t7 = spark.createDataFrame(t7)
    t8 = spark.createDataFrame(t8)

    df1 = t1.union(t5)
    df2 = t2.union(t6)
    df3 = t3.union(t7)
    df4 = t4.union(t8)

    word1 = security.collect()[:250000]
    word2 = general.collect()[:250000]

    word3 = security.collect()[:300000]
    word4 = general.collect()[:300000]

    word5 = security.collect()[:350000]
    word6 = general.collect()[:350000]

    word7 = security.collect()[:400000]
    word8 = general.collect()[:400000]
    word1 = spark.createDataFrame(word1)
    word2= spark.createDataFrame(word2)

    word3 = spark.createDataFrame(word3)
    word4= spark.createDataFrame(word4)

    word5 = spark.createDataFrame(word5)
    word6= spark.createDataFrame(word6)

    word7 = spark.createDataFrame(word7)
    word8= spark.createDataFrame(word8)
    first = t1.union(t5)
    second = t1.union(t2).union(t5).union(t6)
    third = t1.union(t2).union(t3).union(t5).union(t6).union(t7)
    fourth = t1.union(t2).union(t3).union(t4).union(t5).union(t6).union(t7).union(t8)
    from tensorflow.keras.preprocessing.text import Tokenizer
    def background_keyword(df): # input : spark dataframe을 pandas로 변환한 후, dataframe column
        background = df.values.tolist()
        token = Tokenizer()         # 토큰화 함수 지정
        token.fit_on_texts(background)    # 토큰화 함수에 문장 적용
        sorted_dic4 = sorted(token.word_counts.items(), key=lambda x: x[1], reverse=True)
        word_counts_1 = {}
        for i in sorted_dic4:
            word_counts_1.update({i[0]:i[1]})
        return word_counts_1 # dictionary 임
    def back_keyword(pos,neg): # pos : event-related keyword, neg : event-unrelated keyword
        stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
        stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')
        word1 = stage_1.transform(pos) #나는 first_train 부터 함!
        word1 = stage_2.transform(word1)
        word2 = stage_1.transform(neg) #나는 first_train 부터 함!
        word2 = stage_2.transform(word2)
        from pyspark.sql.functions import col, concat_ws
        df1 = word1.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word1 = df1.select("filtered_words").toPandas()
        df2 = word2.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word2 = df2.select("filtered_words").toPandas()
        word_counts_1 = background_keyword(word1["filtered_words"])
        word_counts_2 = background_keyword(word2["filtered_words"])
        return word_counts_1, word_counts_2




    # Author : Minseon Kim (2021)
    class BroadcastWrapper(object):
        def __init__(self, data, token_list):
            self.broadcast_var = sc.broadcast(data)
    #         self.last_updated_time = datetime.now()
            self.token_list = token_list

    #     def is_should_be_updated(self, data):
    #         cur_time = datetime.now()
    #         diff_sec = (cur_time - self.last_updated_time).total_seconds()
    #         return self.broadcast_var is None or diff_sec> 1

        def update_and_get_data(self, spark):
            a = self.broadcast_var.value
            self.broadcast_var.unpersist()
            for i in self.token_list:
                for j in i:
                    if j not in a.keys():
                        a[j] = 1
                    else:
                        a[j] += 1
            new_data = a
            self.broadcast_var = spark.broadcast(new_data)
    #         self.last_updated_time = datetime.now()
    #         return len(self.token_list)
            return self.broadcast_var

    # 바꾼 버전
    from pyspark.ml import Transformer
    from keras.preprocessing.text import Tokenizer
    from keras.preprocessing.sequence import pad_sequences
    import pandas as pd
    from typing import Dict
    from pyspark.sql import DataFrame
    from pyspark.sql import Row
    from pyspark.sql import types   

    def series_to_list(x):
        seri = x.values.tolist()
        for i in range(len(seri)):
            seri[i] = seri[i].tolist()
        return seri
    class TextToSequence(Transformer):
        w1 = dict()
        w2 = dict()
        pos_t = Tokenizer(lower=False)
        neg_t = Tokenizer(lower=False)
        pos_vocab = []
        neg_vocab = []

        def __init__(self, w1: Dict[str, int], w2:Dict[str, int]):
            super(TextToSequence, self).__init__()
            self.w1 = w1
            self.w2 = w2


        def _transform(self, df: DataFrame):
            result_pdf = df.select("filtered_words").toPandas()
            pos_df = result_pdf[:200000] # 24939 99756
            neg_df = result_pdf[200000:]
            print(len(result_pdf))

            broadcast_wrapper1 = BroadcastWrapper(self.w1, pos_df["filtered_words"])
            ww1 = broadcast_wrapper1.update_and_get_data(sc).value

            broadcast_wrapper2 = BroadcastWrapper(self.w2, neg_df["filtered_words"])
            ww2 = broadcast_wrapper2.update_and_get_data(sc).value

            www1 = {k: v for k, v in sorted(ww1.items(), key=lambda item: item[1], reverse=True)}
            www2 = {k: v for k, v in sorted(ww2.items(), key=lambda item: item[1], reverse=True)}
            print(www1['allow'])
            print(www2['like'])
            aa = [w for i, w in enumerate(www1) if i < 5000]
            bb = [w for i, w in enumerate(www2) if i < 5000]

            self.pos_t.fit_on_texts(aa)
            self.neg_t.fit_on_texts(bb)
    #         encoded_docs_pos = self.pos_t.texts_to_sequences(df.select("filtered_words1").toPandas())
    #         encoded_docs_neg = self.neg_t.texts_to_sequences(df.select("filtered_words1").toPandas())
            result_list = series_to_list(result_pdf["filtered_words"])
            encoded_docs_pos = self.pos_t.texts_to_sequences(result_list) # 상위 event keyword 5k가 쓰인 tokenizer
            encoded_docs_neg = self.neg_t.texts_to_sequences(result_list)

            X_p = pad_sequences(encoded_docs_pos, maxlen=100, padding='post').tolist()
            X_n = pad_sequences(encoded_docs_neg, maxlen=100, padding='post').tolist()

            text_array = [str(row['text']) for row in df.select('text').collect()]
            label_array = [int(row['label']) for row in df.select('label').collect()]


            zip_array = list(zip(text_array, label_array, X_p, X_n))
            rdd = sc.parallelize(zip_array, numSlices=306)
            fdf = rdd.toDF(['text','label','feature1','feature2'])


            print(type(fdf))

            return fdf, www1, aa


    word_counts_1, word_counts_2 = back_keyword(word1, word2)
    word_counts_1 = sc.broadcast(word_counts_1)
    word_counts_2 = sc.broadcast(word_counts_2)

    stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
    stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')

    r1 = stage_1.transform(first)
    r2 = stage_2.transform(r1)


    # USE THE TRANSFORMER WITHOUT PIPELINE
    text_sequence = TextToSequence(w1 = word_counts_1.value, w2 = word_counts_2.value)
    df_example, www2, bb = text_sequence.transform(r2)
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import udf
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_vectors = df_example.select(
        df_example["label"], 
        list_to_vector_udf(df_example["feature1"]).alias("feature1"), 
        list_to_vector_udf(df_example["feature2"]).alias("feature2")
    )
    label_str_index = StringIndexer(inputCol='label', outputCol='label_index') # transformer의 마지막 단계!!
    pre_df = label_str_index.fit(df_with_vectors).transform(df_with_vectors)

    from tensorflow.keras.preprocessing.text import Tokenizer
    def background_keyword(df): # input : spark dataframe을 pandas로 변환한 후, dataframe column
        background = df.values.tolist()
        token = Tokenizer()         # 토큰화 함수 지정
        token.fit_on_texts(background)    # 토큰화 함수에 문장 적용
        sorted_dic4 = sorted(token.word_counts.items(), key=lambda x: x[1], reverse=True)
        word_counts_1 = {}
        for i in sorted_dic4:
            word_counts_1.update({i[0]:i[1]})
        return word_counts_1 # dictionary 임
    def back_keyword(pos,neg): # pos : event-related keyword, neg : event-unrelated keyword
        stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
        stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')
        word1 = stage_1.transform(pos) #나는 first_train 부터 함!
        word1 = stage_2.transform(word1)
        word2 = stage_1.transform(neg) #나는 first_train 부터 함!
        word2 = stage_2.transform(word2)
        from pyspark.sql.functions import col, concat_ws
        df1 = word1.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word1 = df1.select("filtered_words").toPandas()
        df2 = word2.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word2 = df2.select("filtered_words").toPandas()
        word_counts_1 = background_keyword(word1["filtered_words"])
        word_counts_2 = background_keyword(word2["filtered_words"])
        return word_counts_1, word_counts_2




    # Author : Minseon Kim (2021)
    class BroadcastWrapper(object):
        def __init__(self, data, token_list):
            self.broadcast_var = sc.broadcast(data)
    #         self.last_updated_time = datetime.now()
            self.token_list = token_list

    #     def is_should_be_updated(self, data):
    #         cur_time = datetime.now()
    #         diff_sec = (cur_time - self.last_updated_time).total_seconds()
    #         return self.broadcast_var is None or diff_sec> 1

        def update_and_get_data(self, spark):
            a = self.broadcast_var.value
            self.broadcast_var.unpersist()
            for i in self.token_list:
                for j in i:
                    if j not in a.keys():
                        a[j] = 1
                    else:
                        a[j] += 1
            new_data = a
            self.broadcast_var = spark.broadcast(new_data)
    #         self.last_updated_time = datetime.now()
    #         return len(self.token_list)
            return self.broadcast_var

    # 바꾼 버전
    from pyspark.ml import Transformer
    from keras.preprocessing.text import Tokenizer
    from keras.preprocessing.sequence import pad_sequences
    import pandas as pd
    from typing import Dict
    from pyspark.sql import DataFrame
    from pyspark.sql import Row
    from pyspark.sql import types   

    def series_to_list(x):
        seri = x.values.tolist()
        for i in range(len(seri)):
            seri[i] = seri[i].tolist()
        return seri
    class TextToSequence(Transformer):
        w1 = dict()
        w2 = dict()
        pos_t = Tokenizer(lower=False)
        neg_t = Tokenizer(lower=False)
        pos_vocab = []
        neg_vocab = []

        def __init__(self, w1: Dict[str, int], w2:Dict[str, int]):
            super(TextToSequence, self).__init__()
            self.w1 = w1
            self.w2 = w2


        def _transform(self, df: DataFrame):
            result_pdf = df.select("filtered_words").toPandas()
            pos_df = result_pdf[:200000] # 24939 99756
            neg_df = result_pdf[200000:]
            print(len(result_pdf))

            broadcast_wrapper1 = BroadcastWrapper(self.w1, pos_df["filtered_words"])
            ww1 = broadcast_wrapper1.update_and_get_data(sc).value

            broadcast_wrapper2 = BroadcastWrapper(self.w2, neg_df["filtered_words"])
            ww2 = broadcast_wrapper2.update_and_get_data(sc).value

            www1 = {k: v for k, v in sorted(ww1.items(), key=lambda item: item[1], reverse=True)}
            www2 = {k: v for k, v in sorted(ww2.items(), key=lambda item: item[1], reverse=True)}
            print(www1['allow'])
            print(www2['like'])
            aa = [w for i, w in enumerate(www1) if i < 5000]
            bb = [w for i, w in enumerate(www2) if i < 5000]

            self.pos_t.fit_on_texts(aa)
            self.neg_t.fit_on_texts(bb)
    #         encoded_docs_pos = self.pos_t.texts_to_sequences(df.select("filtered_words1").toPandas())
    #         encoded_docs_neg = self.neg_t.texts_to_sequences(df.select("filtered_words1").toPandas())
            result_list = series_to_list(result_pdf["filtered_words"])
            encoded_docs_pos = self.pos_t.texts_to_sequences(result_list) # 상위 event keyword 5k가 쓰인 tokenizer
            encoded_docs_neg = self.neg_t.texts_to_sequences(result_list)

            X_p = pad_sequences(encoded_docs_pos, maxlen=100, padding='post').tolist()
            X_n = pad_sequences(encoded_docs_neg, maxlen=100, padding='post').tolist()

            text_array = [str(row['text']) for row in df.select('text').collect()]
            label_array = [int(row['label']) for row in df.select('label').collect()]
            X = np.array(X_p)
            y = np.array(label_array)


            zip_array = list(zip(text_array, label_array, X_p, X_n))
            rdd = sc.parallelize(zip_array, numSlices=306)
            fdf = rdd.toDF(['text','label','feature1','feature2'])


            print(type(fdf))

            return fdf, www1, aa, X, y


    word_counts_1, word_counts_2 = back_keyword(pretrained1, pretrained2)
    word_counts_1 = sc.broadcast(word_counts_1)
    word_counts_2 = sc.broadcast(word_counts_2)

    stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
    stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')

    r1 = stage_1.transform(pre)
    r2 = stage_2.transform(r1)


    # USE THE TRANSFORMER WITHOUT PIPELINE
    text_sequence = TextToSequence(w1 = word_counts_1.value, w2 = word_counts_2.value)
    df_example, www2, bb, X, y = text_sequence.transform(r2)
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import udf
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_vectors = df_example.select(
        df_example["label"], 
        list_to_vector_udf(df_example["feature1"]).alias("feature1"), 
        list_to_vector_udf(df_example["feature2"]).alias("feature2")
    )
    label_str_index = StringIndexer(inputCol='label', outputCol='label_index') # transformer의 마지막 단계!!
    pre_df = label_str_index.fit(df_with_vectors).transform(df_with_vectors)

    from tensorflow.keras.preprocessing.text import Tokenizer
    def background_keyword(df): # input : spark dataframe을 pandas로 변환한 후, dataframe column
        background = df.values.tolist()
        token = Tokenizer()         # 토큰화 함수 지정
        token.fit_on_texts(background)    # 토큰화 함수에 문장 적용
        sorted_dic4 = sorted(token.word_counts.items(), key=lambda x: x[1], reverse=True)
        word_counts_1 = {}
        for i in sorted_dic4:
            word_counts_1.update({i[0]:i[1]})
        return word_counts_1 # dictionary 임
    def back_keyword(pos,neg): # pos : event-related keyword, neg : event-unrelated keyword
        stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
        stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')
        word1 = stage_1.transform(pos) #나는 first_train 부터 함!
        word1 = stage_2.transform(word1)
        word2 = stage_1.transform(neg) #나는 first_train 부터 함!
        word2 = stage_2.transform(word2)
        from pyspark.sql.functions import col, concat_ws
        df1 = word1.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word1 = df1.select("filtered_words").toPandas()
        df2 = word2.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word2 = df2.select("filtered_words").toPandas()
        word_counts_1 = background_keyword(word1["filtered_words"])
        word_counts_2 = background_keyword(word2["filtered_words"])
        return word_counts_1, word_counts_2




    # Author : Minseon Kim (2021)
    class BroadcastWrapper(object):
        def __init__(self, data, token_list):
            self.broadcast_var = sc.broadcast(data)
    #         self.last_updated_time = datetime.now()
            self.token_list = token_list

    #     def is_should_be_updated(self, data):
    #         cur_time = datetime.now()
    #         diff_sec = (cur_time - self.last_updated_time).total_seconds()
    #         return self.broadcast_var is None or diff_sec> 1

        def update_and_get_data(self, spark):
            a = self.broadcast_var.value
            self.broadcast_var.unpersist()
            for i in self.token_list:
                for j in i:
                    if j not in a.keys():
                        a[j] = 1
                    else:
                        a[j] += 1
            new_data = a
            self.broadcast_var = spark.broadcast(new_data)
    #         self.last_updated_time = datetime.now()
    #         return len(self.token_list)
            return self.broadcast_var

    # 바꾼 버전
    from pyspark.ml import Transformer
    from keras.preprocessing.text import Tokenizer
    from keras.preprocessing.sequence import pad_sequences
    import pandas as pd
    from typing import Dict
    from pyspark.sql import DataFrame
    from pyspark.sql import Row
    from pyspark.sql import types   

    def series_to_list(x):
        seri = x.values.tolist()
        for i in range(len(seri)):
            seri[i] = seri[i].tolist()
        return seri
    class TextToSequence(Transformer):
        w1 = dict()
        w2 = dict()
        pos_t = Tokenizer(lower=False)
        neg_t = Tokenizer(lower=False)
        pos_vocab = []
        neg_vocab = []

        def __init__(self, w1: Dict[str, int], w2:Dict[str, int]):
            super(TextToSequence, self).__init__()
            self.w1 = w1
            self.w2 = w2


        def _transform(self, df: DataFrame):
            result_pdf = df.select("filtered_words").toPandas()
            pos_df = result_pdf[:100000] # 24939 99756
            neg_df = result_pdf[100000:]
            print(len(result_pdf))

            broadcast_wrapper1 = BroadcastWrapper(self.w1, pos_df["filtered_words"])
            ww1 = broadcast_wrapper1.update_and_get_data(sc).value

            broadcast_wrapper2 = BroadcastWrapper(self.w2, neg_df["filtered_words"])
            ww2 = broadcast_wrapper2.update_and_get_data(sc).value

            www1 = {k: v for k, v in sorted(ww1.items(), key=lambda item: item[1], reverse=True)}
            www2 = {k: v for k, v in sorted(ww2.items(), key=lambda item: item[1], reverse=True)}
            print(www1['allow'])
            print(www2['like'])
            aa = [w for i, w in enumerate(www1) if i < 5000]
            bb = [w for i, w in enumerate(www2) if i < 5000]

            self.pos_t.fit_on_texts(aa)
            self.neg_t.fit_on_texts(bb)
    #         encoded_docs_pos = self.pos_t.texts_to_sequences(df.select("filtered_words1").toPandas())
    #         encoded_docs_neg = self.neg_t.texts_to_sequences(df.select("filtered_words1").toPandas())
            result_list = series_to_list(result_pdf["filtered_words"])
            encoded_docs_pos = self.pos_t.texts_to_sequences(result_list) # 상위 event keyword 5k가 쓰인 tokenizer
            encoded_docs_neg = self.neg_t.texts_to_sequences(result_list)

            X_p = pad_sequences(encoded_docs_pos, maxlen=100, padding='post').tolist()
            X_n = pad_sequences(encoded_docs_neg, maxlen=100, padding='post').tolist()

            text_array = [str(row['text']) for row in df.select('text').collect()]
            label_array = [int(row['label']) for row in df.select('label').collect()]


            zip_array = list(zip(text_array, label_array, X_p, X_n))
            rdd = sc.parallelize(zip_array, numSlices=306)
            fdf = rdd.toDF(['text','label','feature1','feature2'])


            print(type(fdf))

            return fdf, www1, aa


    word_counts_1, word_counts_2 = back_keyword(word3, word4)
    word_counts_1 = sc.broadcast(word_counts_1)
    word_counts_2 = sc.broadcast(word_counts_2)

    stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
    stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')

    r1 = stage_1.transform(second)
    r2 = stage_2.transform(r1)


    # USE THE TRANSFORMER WITHOUT PIPELINE
    text_sequence = TextToSequence(w1 = word_counts_1.value, w2 = word_counts_2.value)
    df_example, www2, bb = text_sequence.transform(r2)
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import udf
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_vectors = df_example.select(
        df_example["label"], 
        list_to_vector_udf(df_example["feature1"]).alias("feature1"), 
        list_to_vector_udf(df_example["feature2"]).alias("feature2")
    )
    label_str_index = StringIndexer(inputCol='label', outputCol='label_index') # transformer의 마지막 단계!!
    label_df1 = label_str_index.fit(df_with_vectors).transform(df_with_vectors)

    t1 = security.collect()[400000:450000]
    t2 = security.collect()[450000:500000]

    t3 = general.collect()[400000:450000]
    t4 = general.collect()[450000:500000]

    t1 = spark.createDataFrame(t1)
    t2 = spark.createDataFrame(t2)
    t3 = spark.createDataFrame(t3)
    t4 = spark.createDataFrame(t4)


    fine1 = t1.union(t3)
    fine2 = t2.union(t4)

    df1 = t1.union(t3)
    df2 = t1.union(t2).union(t3).union(t4)

    word1 = security.collect()[200000:450000]
    word2 = general.collect()[200000:450000]

    word3 = security.collect()[200000:500000]
    word4 = general.collect()[200000:500000]


    word1 = spark.createDataFrame(word1)
    word2= spark.createDataFrame(word2)

    word3 = spark.createDataFrame(word3)
    word4= spark.createDataFrame(word4)
    word1 = security.collect()[200000:450000]
    word2 = general.collect()[200000:450000]

    word3 = security.collect()[200000:500000]
    word4 = general.collect()[200000:500000]


    word1 = spark.createDataFrame(word1)
    word2= spark.createDataFrame(word2)

    word3 = spark.createDataFrame(word3)
    word4= spark.createDataFrame(word4)
    dff1 = word1.union(word2)
    dff2 = word3.union(word4)
    dff2 = word3.union(word4)
    from tensorflow.keras.preprocessing.text import Tokenizer
    def background_keyword(df): # input : spark dataframe을 pandas로 변환한 후, dataframe column
        background = df.values.tolist()
        token = Tokenizer()         # 토큰화 함수 지정
        token.fit_on_texts(background)    # 토큰화 함수에 문장 적용
        sorted_dic4 = sorted(token.word_counts.items(), key=lambda x: x[1], reverse=True)
        word_counts_1 = {}
        for i in sorted_dic4:
            word_counts_1.update({i[0]:i[1]})
        return word_counts_1 # dictionary 임
    def back_keyword(pos,neg): # pos : event-related keyword, neg : event-unrelated keyword
        stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
        stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')
        word1 = stage_1.transform(pos) #나는 first_train 부터 함!
        word1 = stage_2.transform(word1)
        word2 = stage_1.transform(neg) #나는 first_train 부터 함!
        word2 = stage_2.transform(word2)
        from pyspark.sql.functions import col, concat_ws
        df1 = word1.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word1 = df1.select("filtered_words").toPandas()
        df2 = word2.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word2 = df2.select("filtered_words").toPandas()
        word_counts_1 = background_keyword(word1["filtered_words"])
        word_counts_2 = background_keyword(word2["filtered_words"])
        return word_counts_1, word_counts_2




    # Author : Minseon Kim (2021)
    class BroadcastWrapper(object):
        def __init__(self, data, token_list):
            self.broadcast_var = sc.broadcast(data)
    #         self.last_updated_time = datetime.now()
            self.token_list = token_list

    #     def is_should_be_updated(self, data):
    #         cur_time = datetime.now()
    #         diff_sec = (cur_time - self.last_updated_time).total_seconds()
    #         return self.broadcast_var is None or diff_sec> 1

        def update_and_get_data(self, spark):
            a = self.broadcast_var.value
            self.broadcast_var.unpersist()
            for i in self.token_list:
                for j in i:
                    if j not in a.keys():
                        a[j] = 1
                    else:
                        a[j] += 1
            new_data = a
            self.broadcast_var = spark.broadcast(new_data)
    #         self.last_updated_time = datetime.now()
    #         return len(self.token_list)
            return self.broadcast_var

    # 바꾼 버전
    from pyspark.ml import Transformer
    from keras.preprocessing.text import Tokenizer
    from keras.preprocessing.sequence import pad_sequences
    import pandas as pd
    from typing import Dict
    from pyspark.sql import DataFrame
    from pyspark.sql import Row
    from pyspark.sql import types   

    def series_to_list(x):
        seri = x.values.tolist()
        for i in range(len(seri)):
            seri[i] = seri[i].tolist()
        return seri
    class TextToSequence(Transformer):
        w1 = dict()
        w2 = dict()
        pos_t = Tokenizer(lower=False)
        neg_t = Tokenizer(lower=False)
        pos_vocab = []
        neg_vocab = []

        def __init__(self, w1: Dict[str, int], w2:Dict[str, int]):
            super(TextToSequence, self).__init__()
            self.w1 = w1
            self.w2 = w2


        def _transform(self, df: DataFrame):
            result_pdf = df.select("filtered_words").toPandas()
            pos_df = result_pdf[:250000] # 24939 99756
            neg_df = result_pdf[250000:]
            print(len(result_pdf))

            broadcast_wrapper1 = BroadcastWrapper(self.w1, pos_df["filtered_words"])
            ww1 = broadcast_wrapper1.update_and_get_data(sc).value

            broadcast_wrapper2 = BroadcastWrapper(self.w2, neg_df["filtered_words"])
            ww2 = broadcast_wrapper2.update_and_get_data(sc).value

            www1 = {k: v for k, v in sorted(ww1.items(), key=lambda item: item[1], reverse=True)}
            www2 = {k: v for k, v in sorted(ww2.items(), key=lambda item: item[1], reverse=True)}
            print(www1['allow'])
            print(www2['like'])
            aa = [w for i, w in enumerate(www1) if i < 5000]
            bb = [w for i, w in enumerate(www2) if i < 5000]

            self.pos_t.fit_on_texts(aa)
            self.neg_t.fit_on_texts(bb)
    #         encoded_docs_pos = self.pos_t.texts_to_sequences(df.select("filtered_words1").toPandas())
    #         encoded_docs_neg = self.neg_t.texts_to_sequences(df.select("filtered_words1").toPandas())
            result_list = series_to_list(result_pdf["filtered_words"])
            encoded_docs_pos = self.pos_t.texts_to_sequences(result_list) # 상위 event keyword 5k가 쓰인 tokenizer
            encoded_docs_neg = self.neg_t.texts_to_sequences(result_list)

            X_p = pad_sequences(encoded_docs_pos, maxlen=100, padding='post').tolist()
            X_n = pad_sequences(encoded_docs_neg, maxlen=100, padding='post').tolist()

            text_array = [str(row['text']) for row in df.select('text').collect()]
            label_array = [int(row['label']) for row in df.select('label').collect()]


            zip_array = list(zip(text_array, label_array, X_p, X_n))
            rdd = sc.parallelize(zip_array, numSlices=306)
            fdf = rdd.toDF(['text','label','feature1','feature2'])


            print(type(fdf))

            return fdf, www1, aa


    word_counts_1, word_counts_2 = back_keyword(word1, word2)
    word_counts_1 = sc.broadcast(word_counts_1)
    word_counts_2 = sc.broadcast(word_counts_2)

    stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
    stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')

    r1 = stage_1.transform(dff1)
    r2 = stage_2.transform(r1)


    # USE THE TRANSFORMER WITHOUT PIPELINE
    text_sequence = TextToSequence(w1 = word_counts_1.value, w2 = word_counts_2.value)
    df_example, www2, bb = text_sequence.transform(r2)
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import udf
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_vectors = df_example.select(
        df_example["label"], 
        list_to_vector_udf(df_example["feature1"]).alias("feature1"), 
        list_to_vector_udf(df_example["feature2"]).alias("feature2")
    )
    label_str_index = StringIndexer(inputCol='label', outputCol='label_index') # transformer의 마지막 단계!!
    label_df = label_str_index.fit(df_with_vectors).transform(df_with_vectors)

    from tensorflow.keras.preprocessing.text import Tokenizer
    def background_keyword(df): # input : spark dataframe을 pandas로 변환한 후, dataframe column
        background = df.values.tolist()
        token = Tokenizer()         # 토큰화 함수 지정
        token.fit_on_texts(background)    # 토큰화 함수에 문장 적용
        sorted_dic4 = sorted(token.word_counts.items(), key=lambda x: x[1], reverse=True)
        word_counts_1 = {}
        for i in sorted_dic4:
            word_counts_1.update({i[0]:i[1]})
        return word_counts_1 # dictionary 임
    def back_keyword(pos,neg): # pos : event-related keyword, neg : event-unrelated keyword
        stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
        stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')
        word1 = stage_1.transform(pos) #나는 first_train 부터 함!
        word1 = stage_2.transform(word1)
        word2 = stage_1.transform(neg) #나는 first_train 부터 함!
        word2 = stage_2.transform(word2)
        from pyspark.sql.functions import col, concat_ws
        df1 = word1.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word1 = df1.select("filtered_words").toPandas()
        df2 = word2.withColumn("filtered_words",
           concat_ws(",",col("filtered_words")))
        word2 = df2.select("filtered_words").toPandas()
        word_counts_1 = background_keyword(word1["filtered_words"])
        word_counts_2 = background_keyword(word2["filtered_words"])
        return word_counts_1, word_counts_2




    # Author : Minseon Kim (2021)
    class BroadcastWrapper(object):
        def __init__(self, data, token_list):
            self.broadcast_var = sc.broadcast(data)
    #         self.last_updated_time = datetime.now()
            self.token_list = token_list

    #     def is_should_be_updated(self, data):
    #         cur_time = datetime.now()
    #         diff_sec = (cur_time - self.last_updated_time).total_seconds()
    #         return self.broadcast_var is None or diff_sec> 1

        def update_and_get_data(self, spark):
            a = self.broadcast_var.value
            self.broadcast_var.unpersist()
            for i in self.token_list:
                for j in i:
                    if j not in a.keys():
                        a[j] = 1
                    else:
                        a[j] += 1
            new_data = a
            self.broadcast_var = spark.broadcast(new_data)
    #         self.last_updated_time = datetime.now()
    #         return len(self.token_list)
            return self.broadcast_var

    # 바꾼 버전
    from pyspark.ml import Transformer
    from keras.preprocessing.text import Tokenizer
    from keras.preprocessing.sequence import pad_sequences
    import pandas as pd
    from typing import Dict
    from pyspark.sql import DataFrame
    from pyspark.sql import Row
    from pyspark.sql import types   

    def series_to_list(x):
        seri = x.values.tolist()
        for i in range(len(seri)):
            seri[i] = seri[i].tolist()
        return seri
    class TextToSequence(Transformer):
        w1 = dict()
        w2 = dict()
        pos_t = Tokenizer(lower=False)
        neg_t = Tokenizer(lower=False)
        pos_vocab = []
        neg_vocab = []

        def __init__(self, w1: Dict[str, int], w2:Dict[str, int]):
            super(TextToSequence, self).__init__()
            self.w1 = w1
            self.w2 = w2


        def _transform(self, df: DataFrame):
            result_pdf = df.select("filtered_words").toPandas()
            pos_df = result_pdf[:300000] # 24939 99756
            neg_df = result_pdf[300000:]
            print(len(result_pdf))

            broadcast_wrapper1 = BroadcastWrapper(self.w1, pos_df["filtered_words"])
            ww1 = broadcast_wrapper1.update_and_get_data(sc).value

            broadcast_wrapper2 = BroadcastWrapper(self.w2, neg_df["filtered_words"])
            ww2 = broadcast_wrapper2.update_and_get_data(sc).value

            www1 = {k: v for k, v in sorted(ww1.items(), key=lambda item: item[1], reverse=True)}
            www2 = {k: v for k, v in sorted(ww2.items(), key=lambda item: item[1], reverse=True)}
            print(www1['allow'])
            print(www2['like'])
            aa = [w for i, w in enumerate(www1) if i < 5000]
            bb = [w for i, w in enumerate(www2) if i < 5000]

            self.pos_t.fit_on_texts(aa)
            self.neg_t.fit_on_texts(bb)
    #         encoded_docs_pos = self.pos_t.texts_to_sequences(df.select("filtered_words1").toPandas())
    #         encoded_docs_neg = self.neg_t.texts_to_sequences(df.select("filtered_words1").toPandas())
            result_list = series_to_list(result_pdf["filtered_words"])
            encoded_docs_pos = self.pos_t.texts_to_sequences(result_list) # 상위 event keyword 5k가 쓰인 tokenizer
            encoded_docs_neg = self.neg_t.texts_to_sequences(result_list)

            X_p = pad_sequences(encoded_docs_pos, maxlen=100, padding='post').tolist()
            X_n = pad_sequences(encoded_docs_neg, maxlen=100, padding='post').tolist()

            text_array = [str(row['text']) for row in df.select('text').collect()]
            label_array = [int(row['label']) for row in df.select('label').collect()]


            zip_array = list(zip(text_array, label_array, X_p, X_n))
            rdd = sc.parallelize(zip_array, numSlices=306)
            fdf = rdd.toDF(['text','label','feature1','feature2'])


            print(type(fdf))

            return fdf, www1, aa


    word_counts_1, word_counts_2 = back_keyword(word3, word4)
    word_counts_1 = sc.broadcast(word_counts_1)
    word_counts_2 = sc.broadcast(word_counts_2)

    stage_1 = RegexTokenizer(inputCol= 'text', outputCol='pos_t', pattern= '\\W')
    stage_2 = StopWordsRemover(inputCol= stage_1.getOutputCol(), outputCol= 'filtered_words')

    r1 = stage_1.transform(dff2) #나는 first_train 부터 함!
    r2 = stage_2.transform(r1)

    # USE THE TRANSFORMER WITHOUT PIPELINE
    text_sequence = TextToSequence(w1 = word_counts_1.value, w2 = word_counts_2.value)
    df_example, www2, bb = text_sequence.transform(r2)
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import udf
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_vectors = df_example.select(
        df_example["label"], 
        list_to_vector_udf(df_example["feature1"]).alias("feature1"), 
        list_to_vector_udf(df_example["feature2"]).alias("feature2")
    )
    label_str_index = StringIndexer(inputCol='label', outputCol='label_index') # transformer의 마지막 단계!!
    label_df1 = label_str_index.fit(df_with_vectors).transform(df_with_vectors)

    pretr, prets = pre_df.randomSplit(weights=[0.8,0.2])

    trainqq, testqq = label_df.randomSplit(weights=[0.8,0.2])
    trainqq1, testqq1 = label_df1.randomSplit(weights=[0.8,0.2])
    trainqq.toPandas().to_csv("fifth_train.csv",index=False)
    testqq.toPandas().to_csv("fifth_test.csv",index=False)
    trainqq1.toPandas().to_csv(sixth_train.csv",index=False)

    testqq1.toPandas().to_csv("six_test.csv",index=False)
    from tensorflow.python.keras.models import Model, Sequential
    from tensorflow.python.keras.layers import Input, Dense, concatenate
    from tensorflow.python.keras.layers import LSTM, SimpleRNN
    from tensorflow.python.keras.layers.embeddings import Embedding
    from tensorflow.python.keras.layers import Conv1D, Flatten, GlobalMaxPooling1D, Dropout, MaxPooling1D, Activation, Bidirectional, Conv2D, GlobalMaxPooling2D
    nb_classes=2
    vocab_sizes = 5001
    neg_vocab_sizes = 5001
    embedding_vector_length = 100
    hidden_dims = 250
    max_length = 100
    dropout_ratio = 0.5
    from tensorflow.keras.callbacks import ModelCheckpoint

    model = Sequential()
    model.add(Embedding(vocab_sizes, embedding_vector_length, input_length=max_length))

                               model.add(Bidirectional(LSTM(128)))
    # model.add(GlobalMaxPooling1D())
    model.add(Activation('relu')) # 한 층 더 쌓을까..? relu 함수는 정류된 함수로 은닉층에서 많이 사용된다. -를 차단함!
    # 은닉층으로 많이 사용되는 이유는 음수는 0으로 반환하므로 특정 양수값으로 수렴하지 않음. 그래서 기울기 소실이 발생하지 않음!
    # 시그모이드 함수는 기울기 소실이 발생한다는 단점이 존재. 그래서 relu를 은닉층으로 쓰는 거임!
    # 또한, 학습 속도가 매우 빠르다.
    model.add(Dense(nb_classes)) # stringIndexer 클래스 수만큼 쌓는 이유가 뭘까,,,
    model.add(Activation('sigmoid'))
    # model.add(Dense(1, activation='sigmoid'))
    model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])


    from keras import optimizers, regularizers
    from tensorflow.keras import optimizers

    optimizer_conf = optimizers.Adam(lr=0.01)
    opt_conf = optimizers.serialize(optimizer_conf)

    from tensorflow.keras.models import model_from_json
    from elephas import spark_model, ml_model
    from elephas.ml_model import ElephasEstimator

    estimator = ElephasEstimator()
    estimator.setFeaturesCol("feature1")
    estimator.setLabelCol("label_index")
    estimator.set_keras_model_config(trans_model.to_json())
    estimator.set_categorical_labels(True) # dense 1이면 False로 설정해야함
    estimator.set_nb_classes(nb_classes)
    estimator.set_num_workers(3)
    estimator.set_epochs(10)
    estimator.set_batch_size(128)
    estimator.set_verbosity(1)
    estimator.set_validation_split(0.10)
    estimator.set_optimizer_config(opt_conf)
    estimator.set_mode("asynchronous")
    estimator.set_loss("binary_crossentropy")
    estimator.set_metrics(['acc'])
    dl_pipeline = Pipeline(stages=[estimator])

    import time
    time1 = time.time()
    my_dl1 = dl_pipeline.fit(trainqq) # lstm finetuning
    time3 = time.time() - time1
    print(time3)

                           
