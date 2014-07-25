require 'json'
require 'socket'
require 'timeout'

require_relative 'frames/error'
require_relative 'frames/message'
require_relative 'frames/response'
require_relative 'logger'

module Nsq
  class Connection
    include Nsq::AttributeLogger
    @@log_attributes = [:host, :port]

    attr_reader :host
    attr_reader :port
    attr_reader :socket

    USER_AGENT = "nsq-ruby-client/#{Nsq::Version::STRING}"
    RESPONSE_HEARTBEAT = '_heartbeat_'
    RESPONSE_OK = 'OK'


    def initialize(host, port)
      # for outgoing communication
      @write_queue = Queue.new

      # for indicating that the connection has died
      # Use a queue so we don't have to poll it, even though we only care about
      # the first item
      @death_queue = Queue.new

      @host = host
      @port = port
      @connected = false

      start_connection_loop
    end


    def connected?
      @connected
    end


    # close the connection and don't try to re-open it
    def close
      stop_connection_loop
      close_connection
    end


    def sub(topic, channel)
      write "SUB #{topic} #{channel}\n"
    end


    def rdy(count)
      write "RDY #{count}\n"
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


    def cls
      write "CLS\n"
    end


    def nop
      write "NOP\n"
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
    def write(raw)
      @write_queue.push(raw)
    end


    def write_to_socket(raw)
      @socket.write raw
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
        user_agent: USER_AGENT,
        msg_timeout: 60_000, # 60 seconds
      }.to_json
      write_to_socket ['IDENTIFY', "\n", metadata.length, metadata].pack('a*a*l>a*')
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
      Timeout::timeout(0.1) do
        # Loop until we get a frame
        loop do
          if buffer = @socket.read(8)
            size, type = buffer.unpack('l>l>')
            size -= 4 # we want the size of the data part and type already took up 4 bytes
            data = @socket.read(size)
            frame_class = frame_class_for_type(type)
            return frame_class.new(data, self)
          end
        end
      end
    rescue Errno::ECONNRESET => ex
      warn "#{@port} Died receiving: #{ex}"
      died(ex)
    rescue Timeout::Error
      nop # If connection is broken, this will blow it up
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


    def start_read_loop
      @read_loop_thread ||= Thread.new{read_loop}
    end


    def stop_read_loop
      @read_loop_thread.kill if @read_loop_thread
      @read_loop_thread = nil
    end


    def read_loop
      loop do
        frame = receive_frame
        if frame.is_a?(Response)
          handle_response(frame)
        elsif frame.is_a?(Error)
          error "Error received: #{frame.data}"
        elsif frame.is_a?(Message)
          @queue.push(frame) if @queue
        end
      end
    end


    def start_write_loop
      @write_loop_thread ||= Thread.new{write_loop}
    end


    def stop_write_loop
      @stop_write_loop = true
      @write_loop_thread.join(1) if @write_loop_thread
      @write_loop_thread = nil
    end


    def write_loop
      @stop_write_loop = false
      loop do
        data = @write_queue.pop
        info "Writing: #{data}"
        @socket.write(data)
        break if @stop_write_loop && @write_queue.size == 0
      end
    rescue Errno::EPIPE, Errno::ECONNRESET => ex
      warn "#{@port} Died writing"
      died(ex)
    rescue Exception => ex
      warn "Another write exception: #{ex}"
      died(ex)
    end


    # Waits for death of connection
    def start_connection_loop
      @connection_loop_thread ||= Thread.new{connect_and_monitor}
    end


    def stop_connection_loop
      @connection_loop_thread.kill if @connection_loop_thread
      @connection_loop = nil
    end


    def open_connection
      # Block of stuff we want to write sequentially so that nothing can get
      # added in between entries in a write queue. Ideally we'd have a separate
      # command queue, but that seemed like overkill for now.
      with_retries do
        @socket = TCPSocket.new(@host, @port)
        write_to_socket '  V2'
        identify
      end
      start_read_loop
      start_write_loop
      @connected = true
      after_connect_hook
    end


    def after_connect_hook
      # for ConsumerConnection
    end


    def connect_and_monitor
      open_connection

      loop do
        # wait for death, hopefully it never comes
        cause_of_death = @death_queue.pop
        warn "Died from: #{cause_of_death}"

        warn "#{@port} Reconnecting..."
        close_connection
        open_connection
        warn "#{@port} Reconnected!"

        sleep(0.1)

        # clear all death messages
        @death_queue.clear
      end
    end

    # closes the connection and stops listening for messages
    def close_connection
      cls if connected?
      stop_read_loop
      stop_write_loop
      @write_queue.clear
      @socket = nil
      @connected = false
    end


    def died(reason)
      @connected = false
      @death_queue.push(reason)
    end


    # Retry the supplied block with exponential backoff.
    #
    # Borrowed liberally from:
    # https://github.com/ooyala/retries/blob/master/lib/retries.rb
    def with_retries(&block)
      base_sleep_seconds = 0.5
      max_sleep_seconds = 300 # 5 minutes

      # Let's do this thing
      attempts = 0
      start_time = Time.now
      begin
        attempts += 1
        return block.call(attempts)
      rescue Exception => ex
        raise exception if attempts >= 100

        # The sleep time is an exponentially-increasing function of base_sleep_seconds.
        # But, it never exceeds max_sleep_seconds.
        sleep_seconds = [base_sleep_seconds * (2 ** (attempts - 1)), max_sleep_seconds].min
        # Randomize to a random value in the range sleep_seconds/2 .. sleep_seconds
        sleep_seconds = sleep_seconds * (0.5 * (1 + rand()))
        # But never sleep less than base_sleep_seconds
        sleep_seconds = [base_sleep_seconds, sleep_seconds].max

        warn "Failed to connect: #{ex}. Retrying in #{sleep_seconds.round(1)} seconds."

        snooze sleep_seconds

        retry
      end
    end


    # Se we can stub for testing and reconnect in a tight loop
    def snooze(t)
      sleep(t)
    end
  end
end
