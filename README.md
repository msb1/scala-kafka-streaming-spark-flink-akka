<h2> scala-kafka-streaming-spark-flink-akka<h2/>
<h3>Data streaming to/from three major event/processing frameworks</h3>
<ol>
<li><h4>Spark Structured Streaming</h4>
<ul>
<li>fours models are trained with 10000 data records stored in MongoDB</li>
<li>Spark MLib logistic regression, support vector classifier, gradient boosted trees, random forest</li>
<li>Both training and streaming apps are launched in scala with a SparkLauncer app </li>
</ul>
</li>
<li><h4>Apache Flink</h4>
<ul>
<li>fours models are trained with 10000 data records stored in MongoDB</li> 
<li>logistic regression from deeplearning4j, support vector classifier from libSVM (source), gradient boosted trees and random forest from XGBoost</li>
<li>Models are incorporated into map functions within flink streaming event framework</li>
</ul>
</li>
<li><h4>Akka Streams with Alpakka Kafka</h4>
<ul>
<li>Same models are used from Apache Flink case</li>
<li>models are incorporated through map functions similarly</li>
<li>data simulator (EpdGen) is also included in this file for testing trained application. this simulator is use for all tesing purposes</li>
</ul>
</li>
<li>In each case, the original stream is branched four times after some initial preprocessing.</li>
<li>Each branch filters on the kafka consumer key to determine which of four models is used for a given data record event</li>
<li><h4>These frameworks enable the direct application of tensorflow/keras models in the JVM framework with Scala (or Java). The saved weight from a tensorflow/keras model can be directly imported into dl4j and used in streaming data applications.</h4></li>
</ol>

