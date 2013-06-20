require "logstash/inputs/base"
require "logstash/namespace"
require 'poseidon'

# Read events through Kafka.
#
class LogStash::Inputs::Kafka < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "kafka"
  plugin_status "beta"
  # The address to connect to.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect to.
  config :port, :validate => :number, :required => true
  config :topic, :validate => :string, :default => "kafka"
  public
  def register
    @consumer = Poseidon::PartitionConsumer.new("logstash", @host ,@port,@topic, 0, :earliest_offset)
  end # def register
  public
  def run(output_queue)
    loop do
      messages = @consumer.fetch
      messages.each do |m|
        output_queue << m.value
      end
    end
    finished
  end # def run
end # class LogStash::Inputs::Kafka