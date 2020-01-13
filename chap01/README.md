# Chapter 01: Secondary Sorting
二次排序指的是在归约阶段对某个key关联的value进行排序，本章的目标是用MapReduce和Spark实现二次排序方案。

## MapReduce解决方案
在map和reduce之间，MR还有两个默认的中间处理过程：partition和sort。我们知道reduce是多个同时执行的，为了提高读取性能，map后的数据被划分成多个区块来存储，这样不同的reduce可以同时读取不同的区块而不互相干扰，这就要保证相同的key必须分在同一个分区文件里

MapReduce框架会自动对映射器(Mapper)生成的键排序，这证明，在启动归约器(Reducer)之前，映射器生成的中间键是有序的，但值未必是有序的。如果在归约阶段进行排序，MapReduce有两种解决方案：
1. 让归约器(Reducer)读取和缓存给定键的所有值，然后将这些值在Reduce进行排序，**缺点：** 会导致内存溢出，不建议使用；

2. 使用MapReduce框架对Reducer值排序，为自然键增加部分或整个值来创建一个组合键实现排序目标；利用适当的Mapper输出分区器来实现。

## MapReduce实现细节
- 定义组合键 
- 如何对Reducer键排序 
- 如何对传入Reducer的键分区 
- 如何对到达各个Reduce的数据分组 

### 中间键的排序顺序
要控制中间键排序顺序和Reducer的处理顺序。首先我们需要组合键中注入一个值，然后控制中间键的排序顺序，关系如下图所示：

![](https://github.com/mahmoudparsian/data-algorithms-book/raw/master/src/main/java/org/dataalgorithms/chap01/secondary_sorting.png)
### 制定分区器和定制比较器
分区器会根据Mapper的输出键来决定Mapper和Reducer的映射关系。需要两个插件类：
1. 定制分区器控制Reducer处理的键，确保拥有相同键（自然键，而非组合键）的数据发送给同一个Reducer；
2. 定制比较器对自然键排序，保证数据一旦到达Reducer就会按照自然键对数据分组

![](https://img-blog.csdnimg.cn/2019031815420853.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2xpdXFpYW4xMTA0,size_16,color_FFFFFF,t_70)

Each input record has the following format:
### Weather Sample Input
~~~
<year><,><month><,><day><,><temperature>
~~~

~~~
$ cat time_series.txt 
2019,12,04,10
2019,11,05,9
2019,11,04,12
2019,12,07,5
2019,12,10,3
2019,12,25,-1
2019,11,01,-10
2019,10,04,10
2019,10,14,22
2019,10,28,13
2019,10,01,26
2019,12,23,-8
2019,12,19,3
2019,11,11,-11
2019,12,12,-12
2019,11,10,-1
2019,12,01,5
~~~
### 代码实现
见 mapreduce

| 类名                              | 描述               |
| --------------------------------- | ------------------ |
| SecondarySortDriver               | 主程序入口类       |
| SecondarySortMapper               | Map函数            |
| SecondarySortReducer              | Reduce函数         |
| DateTemperaturePair               | 组合键类           |
| DateTemperaturePartitioner        | 定义自然键分区     |
| DateTemperatureGroupingComparator | 定义自然键如何分组 |

### MapReduce实现总结
在Map输出阶段，自定义分区，根据业务需求，将键相同的元素映射在同一个分区，在分区内对组合键排序，为Reducer准备。
## Spark解决方案
1. 内存中实现，**缺点：** 会导致内存溢出，不建议使用；

2. 使用Spark框架对Reducer值排序，同样采取对自然键增加部分或全部值形成一个组合键（RepartitionAndSortWithinPartitions）。
### Spark实现细节
1. 读取并校验输入参数（文件路径）
2. 连接Spark master，需要创建一个JavaSparkContext对象来处理RDD
3. 创建RDD，并读取数据
4. 从RDD中创建键值对
5. 对第四步进行验证
6. 按键对RDD元素分组
7. 在内存中对Spark归约后的值在内存中排序，使用mapValues()将(K_1, V_1)转换为(K_1, V_2)，由于Spark RDD本身不可变，因此复制到一个临时列表中
### Spark运行方式
1. 独立模式 spark-shell --master local[*]

2. Yarn客户端模式 spark-shell --master yarn --deploy-mode client

3. Yarn集群模式 spark-shell --master yarn --deploy-mode cluster
### 代码中ReduceByKey, GroupByKey与CombineByKey区别
~~~
Spark RDD有三种不同的聚合方式：reduceByKey、groupByKey和combineByKey

groupByKey：groupByKey can cause out of disk problems as data is sent over the network and collected on the reduce workers.

reduceByKey：Data are combined at each partition, only one output for one key at each partition to send over the network. reduceByKey required combining all your values into another value with the exact same type.

combineByKey：3 parameters as input
             1. Initial value: unlike aggregateByKey, need not pass constant always, we can pass a function that will return a new value.
             2. merging function
             3. combine function
~~~
我理解代码中三种方式的执行效率：SecondarySortUsingGroupByKey < SecondarySortUsingCombineByKey < RepartitionAndSortWithinPartitions
## 第一章总结
第一章使用MapReduce和Spark在内存中实现二次排序，第二章将会使用MR框架和Spark框架实现二次排序。
       

