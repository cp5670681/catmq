module Catmq
  class Container::Queue
    attr_accessor :array, :max_size, :size, :tail

    def initialize(max_size = 10000)
      self.array = []
      self.max_size = max_size
      self.size = 0
      self.tail = 0
    end

    alias :length :size

    def head
      (self.tail - self.size) % self.max_size
    end

    def push(obj)
      if self.size == self.max_size
        raise ::Catmq::QueueFullError
      end
      self.array[self.tail] = obj
      self.tail = (self.tail + 1) % self.max_size
      self.size += 1
    end

    def pop
      if self.size == 0
        raise ::Catmq::QueueEmptyError
      end
      result = self.array[self.head]
      self.size -= 1
      result
    end
  end
end
