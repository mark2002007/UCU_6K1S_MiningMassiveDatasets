import os
import pyspark.sql.functions as F

from typing import Optional

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from sparknlp.base import EmbeddingsFinisher
from sparknlp.annotator import DocumentAssembler, Tokenizer, Word2VecModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.linalg import DenseVector, VectorUDT



@F.udf(VectorUDT())
def compute_average_vector(vectors):
    if not vectors:
        return None  # Handle empty list
    n = len(vectors)
    summed_vector = vectors[0]
    for vec in vectors[1:]:
        
        summed_vector += vec
    return DenseVector([x / n for x in summed_vector])


class EmbeddingsLogReg:
    _word2vec_model = None  # Static attribute for shared Word2Vec model

    def __init__(self, maxIter: int, regParam: float, elasticNetParam: float, weightType: Optional[str] = None):
        assert weightType is None or weightType in ["sqrt", "balanced"], "Unacceptable weight type"

        self.weightType = weightType

        self.lr = LogisticRegression(maxIter=maxIter, regParam=regParam, elasticNetParam=elasticNetParam, weightCol='weight') \
            .setFeaturesCol("sentence_embeddings") \
            .setLabelCol("label") \
            .setPredictionCol("prediction")
        
        documentAssembler = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("document")

        tokenizer = Tokenizer() \
            .setInputCols(["document"]) \
            .setOutputCol("token")

        if EmbeddingsLogReg._word2vec_model is None:
            EmbeddingsLogReg._word2vec_model = Word2VecModel.pretrained() \
                .setInputCols(["token"]) \
                .setOutputCol("embeddings")

        self.embeddingsFinisher = EmbeddingsFinisher() \
            .setInputCols(["embeddings"]) \
            .setOutputCols("finished_embeddings") \
            .setOutputAsVector(True) \
            .setCleanAnnotations(False)

        self.preparation_pipeline = Pipeline() \
            .setStages([
            documentAssembler,
            tokenizer
            ])
        
        self.pipelineModel = None
        self.logRegModel = None
        self.trained = False

    def _add_weights(self, train_df):
        if self.weightType == None:
            return train_df.withColumn("weight", F.lit(1.0))
        
        y_collect = train_df.select('label').groupBy('label').count().collect()
        bin_counts = {y['label']: y['count'] for y in y_collect}
        total = sum(bin_counts.values())
        n_labels = len(bin_counts)

        if self.weightType == 'balanced':
            weights = {bin_: total/(n_labels*count) for bin_, count in bin_counts.items()}
            return train_df.withColumn('weight', F.when(F.col('label')==1.0, weights[1]).otherwise(weights[0]))
        else:
            weights = {bin_: (total/(n_labels*count))**0.5 for bin_, count in bin_counts.items()}
            return train_df.withColumn('weight', F.when(F.col('label')==1.0, weights[1]).otherwise(weights[0]))

    def fit(self, train_df):
        self.pipelineModel = self.preparation_pipeline.fit(train_df)

        train_df_tokens = self.pipelineModel.transform(train_df)

        train_df_wembs = EmbeddingsLogReg._word2vec_model.transform(train_df_tokens)
        train_df_wembs = self.embeddingsFinisher.transform(train_df_wembs)

        train_df_wembs = train_df_wembs.withColumn("sentence_embeddings", compute_average_vector(F.col("finished_embeddings")))

        train_df_wembs = self._add_weights(train_df_wembs)

        self.logRegModel = self.lr.fit(train_df_wembs)
        self.trained = True

    def fitMultiple(self, dataset, paramMaps):
        """Method needed for a parameter search, to train multiple models"""

        for i, paramMap in enumerate(paramMaps):
            paramMap = {key.name: val for key, val in paramMap.items()}
            embLR = EmbeddingsLogReg(paramMap["maxIter"], paramMap["regParam"], paramMap["elasticNetParam"])

            embLR.fit(dataset)
            
            yield i, embLR

    def predict(self, df, output_pred_col: str = "prediction"):
        if not self.trained:
            raise ValueError("Model is not trained")
        
        df_tokens = self.pipelineModel.transform(df)
        df_wembs = EmbeddingsLogReg._word2vec_model.transform(df_tokens)
        df_wembs = self.embeddingsFinisher.transform(df_wembs)

        df_wembs = df_wembs.withColumn("sentence_embeddings", compute_average_vector(F.col("finished_embeddings")))

        self.logRegModel.setPredictionCol(output_pred_col)
        predictions = self.logRegModel.transform(df_wembs)
        return predictions
    
    def transform(self, df, _):
        return self.predict(df)
    
    def save(self, path):
        if not self.trained:
            raise ValueError("Model is not trained")
        if os.path.exists(path) and len(os.listdir(path)) > 0:
            raise ValueError("Target folder should be empty or nonexistent")

        os.makedirs(path, exist_ok=True)

        self.pipelineModel.save(os.path.join(path, "embedding_pipeline"))
        self.lr.save(os.path.join(path, "lr"))
        self.logRegModel.save(os.path.join(path, "lr_model"))

    def _load(self, path):
        assert os.path.exists(path)

        self.lr = LogisticRegression.load(os.path.join(path, "lr"))
        self.logRegModel = LogisticRegressionModel.load(os.path.join(path, "lr_model"))
        self.pipelineModel = PipelineModel.load(os.path.join(path, "embedding_pipeline"))

        self.trained = True

    @staticmethod
    def load(path):
        embLR = EmbeddingsLogReg(0, 0, 0)
        embLR._load(path)

        return embLR
