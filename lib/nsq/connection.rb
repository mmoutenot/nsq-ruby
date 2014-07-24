require 'json'
require 'socket'

require_relative 'frames/error'
require_relative 'frames/message'
require_relative 'frames/response'

module Nsq
  class Connection

    attr_reader :socket
    attr_accessor :max_in_flight
    attr_reader :presumed_in_flight

    NSQ_MESSAGE_TIMEOUT_IN_SECONDS = 60
    NSQ_USER_AGENT = "nsq-ruby-client/#{Nsq::Version::STRING}"

    RESPONSE_HEARTBEAT = '_heartbeat_'
    RESPONSE_OK = 'OK'

    def initialize(host, port)
      @presumed_in_flight = 0
      @max_in_flight = 0
      @socket = TCPSocket.new(host, port)
      write '  V2'
      identify
      at_exit{close}
    end


    def subscribe_and_listen(topic, channel, queue, max_in_flight)
      @max_in_flight = max_in_flight
      sub(topic, channel)
      re_up_ready
      @message_thread = Thread.new { listen_for_messages(queue) }
    end


    # closes the connection and stops listening for messages
    def close
      @stop_listening_for_messages = true
      @message_thread.join if @message_thread
      @socket && cls
      @socket = nil
    end


    def fin(message_id)
      write "FIN #{message_id}\n"
      decrement_in_flight
    end


    def req(message_id, timeout)
      write "REQ #{message_id} #{timeout}\n"
      decrement_in_flight
    end


    def touch(message_id)
      write "TOUCH #{message_id}\n"
    end


    def pub(topic, message)
      write ["PUB #{topic}\n", message.length, message].pack('a*l>a*')
    end


    def mpub(topic, messages)
      body = messages.map do |message|
        [message.length, message].pack('l>a*')
      end.join

      write ["MPUB #{topic}\n", body.length, messages.size, body].pack('a*l>l>a*')
    end


    private
    def sub(topic, channel)
      write "SUB #{topic} #{channel}\n"
    end


    def rdy(count)
      write "RDY #{count}\n"
    end


    def cls
      write "CLS\n"
    end


    def nop
      write "NOP\n"
    end


    def listen_for_messages(queue)
      @stop_listening_for_messages = false
      loop do
        frame = receive_frame
        if frame.is_a?(Response)
          handle_response(frame)
        elsif frame.is_a?(Error)
          puts "error: #{frame.data}"
        elsif frame.is_a?(Message)
          queue.push(frame)
        end
        break if @stop_listening_for_messages
      end
    end


    def write(raw)
      @socket.write(raw)
    end


    def identify
      hostname = Socket.gethostname
      metadata = {
        client_id: Socket.gethostbyname(hostname).flatten.compact.first,
        hostname: hostname,
        feature_negotiation: false,
        heartbeat_interval: 30_000, # 30 seconds
        output_buffer: 16_000, # 16kb
        output_buffer_timeout: 250, # 250ms
        tls_v1: false,
        snappy: false,
        deflate: false,
        sample_rate: 0, # disable sampling
        user_agent: NSQ_USER_AGENT,
        msg_timeout: NSQ_MESSAGE_TIMEOUT_IN_SECONDS * 1000,
      }.to_json
      write ['IDENTIFY', "\n", metadata.length, metadata].pack('a*a*l>a*')
    end


    def handle_response(frame)
      if frame.data == RESPONSE_HEARTBEAT
        nop
      elsif frame.data == RESPONSE_OK
      else
        raise "Received response we don't know how to handle: #{frame.data}"
      end
    end


    def receive_frame
      if buffer = @socket.read(8)
        size, type = buffer.unpack('l>l>')
        size -= 4 # we want the size of the data part and type already took up 4 bytes
        data = @socket.read(size)
        frame_class = frame_class_for_type(type)
        frame_class.new(data, self)
      end
    end


    FRAME_CLASSES = [Response, Error, Message]
    def frame_class_for_type(type)
      raise "Bad frame type specified: #{type}" if type > FRAME_CLASSES.length - 1
      [Response, Error, Message][type]
    end


    def decrement_in_flight
      @presumed_in_flight -= 1

      # now that we're less than @max_in_flight we might need to re-up our RDY
      # state
      threshold = (@max_in_flight * 0.2).ceil
      re_up_ready if @presumed_in_flight <= threshold
    end


    def re_up_ready
      rdy(@max_in_flight)
      # assume these messages are coming our way. yes, this might not be the
      # case, but it's much easier to manage our RDY state with the server if
      # we treat things this way.
      @presumed_in_flight = @max_in_flight
    end

  end
end
