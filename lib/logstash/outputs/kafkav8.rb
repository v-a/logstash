require "logstash/outputs/base"
require "logstash/namespace"
require "poseidon"

# Write events through Kafka.
#
class LogStash::Outputs::Kafka < LogStash::Outputs::Base
  config_name "kafka"
  plugin_status "beta"
  # The address to connect to.
  config :host, :validate => :string, :required => true
  # The port to connect to.
  config :port, :validate => :number, :required => true
  # The Kafka Topic.
  config :topic, :validate => :string, :default => "kafka"
  config :message_format, :validate => :string
  public
  def register
    @producer = Poseidon::Producer.new(["#{@host}:{@port}"], "logstash")
  end
  public
  def receive(event)
    @producer << Poseidon::MessageToSend.new(@topic, event.to_hash.to_json)
  end # def receive
end # class LogStash::Outputs::Kafka

