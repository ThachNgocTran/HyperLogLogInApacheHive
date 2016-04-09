# HyperLogLog In Apache Hive
Integrate HyperLogLog into Apache Hive under the form of User-Defined Aggregate Function (UDAF). Written in JAVA.

There are many blogs explaining how HyperLogLog works. We even have a few open-source implementations of HyperLogLog (in Java, Python, C++). Documentation on writing UDAF for Apache Hive is readily available. My contribution here is an attempt to integrate HyperLogLog into Apache Hive under the form of UDAF. The beauty is that HyperLogLog can surprisingly fit in Distributed Computing. Let's say: One mapper digests its data part, making one HyperLogLog instance; then at Reducer, all of these instances are merged into one single HyperLogLog instance that represents all of the (large) original data.

Please see [How to write an User-Defined Aggregate Function (UDAF) in Apache Hive?](https://thachtranerc.wordpress.com/2016/01/02/how-to-write-an-user-defined-aggregate-function-udaf-in-apache-hive/) and [Cardinality Estimation with HyperLogLog – An empirical evaluation] (https://thachtranerc.wordpress.com/2016/01/23/cardinality-estimation-with-hyperloglog-an-empirical-evaluation/) and [Here is how HyperLogLog fits in Distributed Computing] (https://thachtranerc.wordpress.com/2016/01/25/here-is-how-hyperloglog-fits-in-distributed-computing/) for further information.

Notes:

1. The original HyperLogLog implementation here is from Stream-Lib.
2. To build a runnable JAR in Apache Hive, it must be built in compatible library references. See the "dependencies" part in pom.xml for my case.
3. The IDE I used to build is IDEA IntelliJ, Java SDK 8. The project itself is of Maven. In Intellij, open the project by pointing to the "pom.xml".
4. I tested the UDAF by first uploading the JAR into a place where Apache Hive can find, then using the HiveQL script as specified in the source code.
5. Important caveat: watch out if the total number of elements digested by an HyperLogLog instance has its total size less than the size of the HyperLogLog instance. Let's say: one HyperLogLog 16-bit occupies about 44 KB, one IP address is maximum 15 bytes (the string "182.100.101.102"). How many IP do we need to surpass the size of an HyperLogLog instance?

Simple/Naive scenario where these UDAFs can be helpful:

* Supposed that we have a large Hive table "myiptable", which has two fields: dt and ip. "dt" is datetime (String), "ip" is IP Address (String). The table is partitioned on "dt". When your website serves a http web request, it keeps track of the IP Address of the visitor. At the end of that day, you push all recorded IPs into a new partition of the table.
* The traditional approach is you keep raw data as they are. Here, we use HyperLogLog technique - a synopsis technique, to make a "compact" representation of all IPs encountered during a specific day. That is, one day - one HyperLogLog synopsis. You have 30 days, you have 30 HyperLogLog synopses. These synopses are so small in size that you push them into a single table "hllSynopsis". This hllSynopsis table has two fields: dt and hll. "dt" is datetime (String), "hll" is an instance of HyperLogLog being serialized into a byte array. Each record of hllSynopsis is representing all 'distinct/unique' IPs of a specific day.
* After, for example, two months, you have 60 days; thus, 60 HyperLogLog synopses. Now you merge all of these synopses into one single HyperLogLog synopsis. From there, you execute the "count" to get "the number of unique IPs during the specified two months" (aka: the cardinality).
If January-February, your site manages to attract 10 million unique visitors; but March-April, the number is 5 millions. Well, your site is no longer appealing to visitors...

