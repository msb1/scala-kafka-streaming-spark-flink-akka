<h4> scala-kafka-streaming-spark-flink-akka<h4/>
<h5>Data streaming to/from three major event/processing frameworks</h5>
<ol>
<li><h6>Spark Structured Streaming</h6>
<ul>
<li>fours models are trained with 10000 data records stored in MongoDB</li>
<li>Spark MLib logistic regression, support vector classifier, gradient boosted trees, random forest</li>
<li>Both training and streaming apps are launched in scala with a SparkLauncer app </li>
</ul>
</li>
<li><h6>Apache Flink</h6>
<ul>
<li>fours models are trained with 10000 data records stored in MongoDB</li> 
<li>logistic regression from deeplearning4j, support vector classifier from libSVM (source), gradient boosted trees and random forest from XGBoost</li>
<li>Models are incorporated into map functions within flink streaming event framework</li>
</ul>
</li>
<li><h6>Akka Streams with Alpakka Kafka</h6>
<ul>
<li>Same models are used from Apache Flink case</li>
<li>models are incorporated through map functions similarly</li>
<li>data simulator (EpdGen) is also included in this file for testing trained application
</ul>
</li>
<li>In each case, the original stream is branched four times after some initial preprocessing.<li>
<li>Each branch filters on the kafka consumer key to determine which of four models is used for a given data record event<li>
<li><h6>These frameworks enable the direct application of tensorflow/keras models in the JVM framework with Scala (or Java). The saved weight from a tensorflow/keras model can be directly imported into dl4j and used in streaming data applications.</h6></li>
</ol>

