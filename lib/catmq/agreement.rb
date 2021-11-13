require 'uuid'

module Catmq
  class Agreement
    # 两条消息之间的分隔符
    SPLIT = "\n\n"

    def initialize(socket)
      @socket = socket
    end

    def send_message(payload, router: '')
      data = {
        # uuid有bug，偶尔会报错，转而使用时间戳
        uuid: (::UUID.new.generate rescue Time.now.to_f.to_s),
        router: router,
        body: payload
      }.to_json
      @socket.write("#{data}#{SPLIT}")
      'ok'
    rescue Errno::EPIPE, IOError
      # todo: 消息可能会丢失
      @socket.close
    end

    # 由于tcp的特性，接收的数据可能不完整：或者是一条消息只取到前面部分，或者是一次取到好几条消息的合并
    # 利用生成器模式，每次yield一个完整的消息出去
    # 外部用块遍历，每次得到的就是一个完整的消息了
    def receive
      buf = ''
      while true
        while index = buf.index(SPLIT)
          yield buf[0...index]
          buf = buf[index + SPLIT.length..-1].to_s
        end
        chunk = @socket.readpartial(1024)
        buf += chunk
      end
    rescue EOFError
      puts "client eof"
      @socket.close
      raise
    end
  end
end
