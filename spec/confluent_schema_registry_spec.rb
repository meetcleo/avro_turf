require 'webmock/rspec'
require 'avro_turf/confluent_schema_registry'
require 'avro_turf/test/fake_confluent_schema_registry_server'
require 'avro_turf/test/fake_confluent_schema_registry_server_with_oauth'

describe AvroTurf::ConfluentSchemaRegistry do
  let(:user) { "abc" }
  let(:password) { "xxyyzz" }
  let(:client_cert) { "test client cert" }
  let(:client_key) { "test client key" }
  let(:client_key_pass) { "test client key password" }
  let(:oauth_client_id) { "test oauth_client_id" }
  let(:oauth_client_secret) { "test oauth_client_secret" }

  it_behaves_like "a confluent schema registry client", "cert" do
    let(:registry) {
      described_class.new(
        registry_url,
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass
      )
    }
  end

  it_behaves_like "a confluent schema registry client", "password" do
    let(:registry) {
      described_class.new(
        registry_url,
        user: user,
        password: password,
      )
    }
  end

  it_behaves_like "a confluent schema registry client", "oauth" do
    let(:registry) {
      described_class.new(
        registry_url,
        logger: logger,
        oauth_url: oauth_url,
        oauth_client_id: oauth_client_id,
        oauth_client_secret: oauth_client_secret
      )
    }
  end
end
