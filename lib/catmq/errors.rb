module Catmq
  class Error < RuntimeError
  end

  # 队列满
  class QueueFullError < Error
  end

  # 队列空
  class QueueEmptyError < Error
  end

  # 无此交换机
  class ExchangeNotFoundError < Error
  end
end
