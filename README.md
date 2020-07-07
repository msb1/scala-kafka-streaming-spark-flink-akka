<h2> scala-kafka-streaming-spark-flink-akka<h2/>
<h3>Data streaming to/from three major event/processing frameworks</h3>
<ol>
    <li><h6>Spark Structured Streaming</h6>
    <ul>
        <li>Includes homogeneous data processed through multimodel:
            <ol type='a'>
                <li>fours models are trained with 10000 data records stored in MongoDB</li>
                <li>Spark MLib logistic regression, support vector classifier, gradient boosted trees, random forest</li>
                <li>Both training and streaming apps are launched in scala with a SparkLauncher app </li>
            </ol>
        </li>
        <li>Includes heterogeneous data processed through multi-pipelines (one of hardest problems in spark structured streaming):
            <ol type='a'>
                <li>heterogeneous data with pipeline for each data stream is one of most difficult spark streaming problems</li>
                <li>dynamically configure schema for each model-pipeline to be included</li>
                <li>can have arbitrary number of data streams once a model and pipeline have been trained for data stream</li>
                <li>cannot use spark sql writestream due to checkpoint issues that arise after dataframe union or with separating dataframes into separate streams</li>
                <li>implement Kafka Producer directly in foreach after writestream in process method</li>
            </ol>
        </li>
    </ul>
    </li>
    <li><h6>Apache Flink</h6>
    <ul>
        <li>fours models are trained with 10000 data records stored in MongoDB</li> 
        <li>logistic regression from deeplearning4j, support vector classifier from libSVM (source), gradient boosted trees and random forest from XGBoost</li>
        <li>Models are incorporated into map functions within flink streaming event framework</li>
        <li>Data events are filtered prior to the map functions to direct which model should be used for a given event</li>
    </ul>
    </li>
    <li><h6>Akka Streams with Alpakka Kafka</h6>
    <ul>
        <li>Same models are used from Apache Flink case</li>
        <li>models are incorporated through map functions similarly</li>
        <li>data simulator (EpdGen) is also included in this file for testing trained application</li>
        <li>In each case, the original stream is branched four times after some initial preprocessing.</li>
        <li>Each branch filters on the kafka consumer key to determine which of four models is used for a given data record event</li>
    </ul>
</ol>
<h6>The Kafka Streams and Apache Flink frameworks enable the direct application of tensorflow/keras models in the JVM framework with Scala (or Java). The saved weight from a tensorflow/keras model can be directly imported into DeepLearning4j and used in streaming data applications.</h6>


