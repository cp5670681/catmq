module Catmq
  class Error < RuntimeError
  end

  class QueueFullError < Error
  end

  class QueueEmptyError < Error
  end
end
