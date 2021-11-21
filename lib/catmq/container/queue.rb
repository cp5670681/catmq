module Catmq
  class Container::Queue
    attr_accessor :array, :max_size, :size, :tail

    def initialize(max_size = 10000)
      # 用来存放数据的数组
      self.array = []
      # 队列最大长度
      self.max_size = max_size
      # 队列已存放元素个数（队列长度）
      self.size = 0
      # 队尾位置
      self.tail = 0
    end

    alias :length :size

    # 队头位置
    # @return [Integer] 队头下标
    def head
      (self.tail - self.size) % self.max_size
    end

    # 入队
    # @param [Object] obj 入队的元素
    # @return [Integer] 入队后队列的长度
    def push(obj)
      raise ::Catmq::QueueFullError if self.size == self.max_size
      self.array[self.tail] = obj
      self.tail = (self.tail + 1) % self.max_size
      self.size += 1
    end

    # 出队
    # 为了防止并发时，先查后pop出现前后不一致问题，可以在pop时指定队头位置，一致则pop，否则不做任何事
    # @param [Integer] head 队头位置
    # @return [Object] 出队的元素
    def pop(head: nil)
      return if head && (self.head != head)
      raise ::Catmq::QueueEmptyError if self.size == 0
      result = self.array[self.head]
      self.size -= 1
      result
    end

    # 查看队头元素
    def head_obj
      raise ::Catmq::QueueEmptyError if self.size == 0
      self.array[self.head]
    end

  end
end
