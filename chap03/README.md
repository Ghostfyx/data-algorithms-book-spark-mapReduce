## TopN设计模式形式化描述
TopN是一种设计模式，可以用来解决大数据场景下过滤出最符合要求的topN的数据。令N是一个整数，令L是一个`List<Tuple<T, Integer>>`，其中T可以是任意类型的数据，`L.size() = s, s > N`，L的元素为：
`{(K_i,V_i), 1<= i <= S}`。

为了实现Top N，需要一个散列表数据结构，例如Java中的SortedMap，TreeMap，如果SortedMap或TreeMap的容量大于N，则删除头或尾元素（取决去获取TopN还是BoomN）。
## TopN的实现
下面我们分为两种情况讨论TopN的实现：1. 唯一键，数据集中所有数据唯一，即T值唯一；2. 重复键，数据集中存在大量重复数据键，重复键更加贴近实际生产场景。