# Effective Model Update for Adaptive Classification of Text Streams in a Distributed Learning Environment
The paper was published in [Sensors 2022](https://www.mdpi.com/1424-8220/22/23/9298). 
> In this study, we propose dynamic model update methods for the adaptive classification model of text streams in a distributed learning environment. In particular, we present two model update strategies: (1) the entire model update and (2) the partial model update. The former aims to maximize the model accuracy by periodically rebuilding the model based on the accumulated datasets including recent datasets. Its learning time incrementally increases as the datasets increase, but we alleviate the learning overhead by the distributed learning of the model. The latter fine-tunes the model only with a limited number of recent datasets, noting that the data streams are dependent on a recent event. Therefore, it accelerates the learning speed while maintaining a certain level of accuracy. To verify the proposed update strategies, we extensively apply them to not only fully trainable language models based on CNN, RNN, and Bi-LSTM, but also a pre-trained embedding model based on BERT. Through extensive experiments using two real tweet streaming datasets, we show that the entire model update improves the classification accuracy of the pre-trained offline model; the partial model update also improves it, which shows comparable accuracy with the entire model update, while significantly increasing the learning speed. We also validate the scalability of the proposed distributed learning architecture by showing that the model learning and inference time decrease as the number of worker nodes increases.

# Dependencies
- Elephas
- Tensorflow
- Pyspark
  
# Run
```python run.py```
