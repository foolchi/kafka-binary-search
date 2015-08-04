##Kafka Binary Search
Kafka二分查找算法实现

###原理
Kafka的每个topic都是按照发送时间严格排序的，所以如果我们知道我们所要查找的消息的顺序，那么就可以按照二分查找的方法快速定位到该消息在某个特定Kafka topic中是否存在。

使用的是Kafka的
[High Level Consumer API](http://kafka.apache.org/documentation.html#highlevelconsumerapi)
和
[Kafka Consumer Group Example](https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example)

###用法
```scala

val topic = "test" // kafka topic name
val broker = "localhost" // kafka broker name
val port = 9092 // kafka broker port
val dest = "xxx" //查找目标
val binarySearch = new KafkaBinarySearch(topic, broker, port)
val offset = binarySearch.search(new MyBinaryComparator(dest)) // 返回offset，-1代表不存在

```

其中`MyBinaryComparator`是一个实现了`BinaryComparator`特质的类，这个类只有一个方法:`compare(msg : String)`,用于自定义比较方法。比如消息可能是一个带时间戳的复杂`json`格式，需要在`compare`方法中提取时间戳再进行比较。或则消息内并不含时间戳，但是含有一个单调递增的事件`id`，都可以在`compare`中定义具体的比较行为。

###模糊查询
如果时间戳并不是严格单调递增，但是不同时间戳误差在一定范围内，那么我们可以使用模糊查询。模糊查询首先使用二分查找的方法找到某个跟查找目标误差在一定范围内的消息，然后以这个消息为中心分别向左右按顺序查找，直到查找到该消息或则左右的误差已经超过我们定义的范围。使用方法与正常查找基本一致:

```scala

val topic = "test" // kafka topic name
val broker = "localhost" // kafka broker name
val port = 9092 // kafka broker port
val dest = "xxx" //查找目标
val binarySearch = new KafkaBinarySearch(topic, broker, port)
val offset = binarySearch.fuzzySearch(new MyFuzzyBinaryComparator(dest)) // 返回offset，-1代表不存在

```


其中`MyFuzzyBinaryComparator`是一个实现了`FuzzyBinaryComparator`特质的类，这个接口有两个方法`compare`和`exactCompare`，前一个是带误差的比较，后面一个是不带误差的准确比较。

###Test
我写了一个BinarySearchTest的测试类，里面测试了二分查找和模糊查找，测试步骤:

* 使用当前系统时间新建一个topic，这样保证每次测试的时候数据不会相互干扰
* 生成一堆只含时间戳的消息(对于模糊查询，生成的时间戳加上一个随机小扰动)
* 因为当前的topic是全新的，所以消息的发送顺序就是offset(从0开始)，保存`时间戳-offset`的映射
* 对于每个时间戳使用二分查找/模糊查找获得对应的offset，比较是否与保存的一致

###TODO
* ~~因为Kafka的API文档都是Java为主，所以就先用Java写了，后面可以考虑改成Scala~~
* ~~`BinaryComparator`和`FuzzyBinaryComparator`可以把要查找的`dest`作为一个类的变量，这样就不用每次`compare`的时候都要`parse`~~
* `fuzzySearch`中调用的`sequenceSearch`函数还可以优化速度，方法是一次读取多条消息然后依次比较