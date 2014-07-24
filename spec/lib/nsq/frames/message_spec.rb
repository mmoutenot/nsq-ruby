require_relative '../../../spec_helper'

require 'timecop'

describe Nsq::Message do
  before do
    @cluster = NsqCluster.new(nsqd_count: 1)
    @cluster.block_until_running
    @connection = Nsq::Connection.new(@cluster.nsqd[0].host, @cluster.nsqd[0].tcp_port)
  end
  after do
    @connection.close
    @cluster.destroy
  end

  %w(finish requeue touch).each do |method|
    describe "##{method}" do
      before do
        @msg_time = Time.new(2014, 7, 24, 13, 22, 19.363434740)
        msg_timestamp = @msg_time.to_i * 10**9 + @msg_time.nsec
        msg_data = [msg_timestamp, 17, 'msgid', 'messagebody'].pack('Q>s>a16a*')
        @msg = described_class.new(msg_data, @connection)
      end
      it 'raises CannotModifyExpiredMessageError if message timed out' do
        Timecop.freeze(
          @msg_time + Nsq::Connection::NSQ_MESSAGE_TIMEOUT_IN_SECONDS
        ) do
          expect{@msg.send(method.to_sym)}.to raise_error(
            Nsq::Message::CannotModifyExpiredMessageError
          )
        end
      end
      it 'does not raise an error if the message has not timed out' do
        puts ''
        puts ''
        puts Time.now
        Timecop.freeze(
          @msg_time + Nsq::Connection::NSQ_MESSAGE_TIMEOUT_IN_SECONDS - 100
        ) do
          puts Time.now
          expect{
            puts Time.now
            @msg.send(method.to_sym)
          }.not_to raise_error
          puts Time.now
          puts ''
          puts ''
        end
      end
    end
  end


  describe 'private method' do
    describe '#has_message_expired?' do
      it 'returns true when message has expired' do
        msg_data = [1406222539363434696, 17, 'msgid', 'messagebody'].pack('Q>s>a6a*')
        msg = described_class.new(msg_data, @connection)
        nsq_time = 1406222539363434696
        ruby_time = Time.at(
          nsq_time / (10**9),
          (nsq_time % (10**9).to_f / 1000)
        )
        Timecop.freeze(
          ruby_time + Nsq::Connection::NSQ_MESSAGE_TIMEOUT_IN_SECONDS
        ) do
          expect(msg.send(:has_message_expired?)).to be_truthy
        end
      end
      it 'returns false when message has not expired' do
        msg_data = [1406222539363434696, 17, 'msgid', 'messagebody'].pack('Q>s>a6a*')
        msg = described_class.new(msg_data, @connection)
        nsq_time = 1406222539363434696
        ruby_time = Time.at(
          nsq_time / (10**9),
          (nsq_time % (10**9).to_f / 1000)
        )
        Timecop.freeze(
          ruby_time + Nsq::Connection::NSQ_MESSAGE_TIMEOUT_IN_SECONDS - 1
        ) do
          expect(msg.send(:has_message_expired?)).to be_falsey
        end
      end
    end
  end
end
