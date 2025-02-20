# This shared example expects a registry variable to be defined
# with an instance of the registry class being tested.
shared_examples_for "a confluent schema registry client" do |auth_mechanism|
  let(:logger) { Logger.new(StringIO.new) }
  let(:registry_url) { "http://registry.example.com" }
  let(:oauth_url) { "https://oauth.example.com/auth" }
  let(:oauth_token) { "test_token" }
  let(:oauth_response) { <<-RESP
      {
        "access_token":"#{oauth_token}",
        "expires_in":3600,
        "token_type":"Bearer"
      }
    RESP
  }
  let(:subject_name) { "some-subject" }
  let(:schema) do
    {
      type: "record",
      name: "person",
      fields: [
        { name: "name", type: "string" }
      ]
    }.to_json
  end

  before do
    fake_registry_server_klass = nil
    if auth_mechanism == 'oauth'
      fake_registry_server_klass = FakeConfluentSchemaRegistryServerWithOauth

      stub_request(:post, oauth_url).
        with(
          body: {"grant_type"=>"client_credentials"},
          headers: {
          'Authorization'=>'Basic dGVzdCBvYXV0aF9jbGllbnRfaWQ6dGVzdCBvYXV0aF9jbGllbnRfc2VjcmV0',
          'Content-Type'=>'application/x-www-form-urlencoded',
          'Host'=>'oauth.example.com:443'
          }).
        to_return(status: 200, body: oauth_response, headers: {})
    else
      fake_registry_server_klass = FakeConfluentSchemaRegistryServer
    end

    stub_request(:any, /^#{registry_url}/).to_rack(fake_registry_server_klass)
    fake_registry_server_klass.clear
  end

  describe "#register and #fetch" do
    it "allows registering a schema" do
      id = registry.register(subject_name, schema)
      fetched_schema = registry.fetch(id)

      expect(fetched_schema).to eq(schema)
    end

    context "with an Avro::Schema" do
      let(:avro_schema) { Avro::Schema.parse(schema) }

      it "allows registration using an Avro::Schema" do
        id = registry.register(subject_name, avro_schema)
        expect(registry.fetch(id)).to eq(avro_schema.to_s)
      end

      context "with ActiveSupport present" do
        before do
          break_to_json(avro_schema)
        end

        it "allows registering an Avro schema" do
          id = registry.register(subject_name, avro_schema)
          expect(registry.fetch(id)).to eq(avro_schema.to_s)
        end
      end
    end
  end

  describe "#fetch" do
    context "when the schema does not exist" do
      it "raises an error" do
        expect do
          registry.fetch(-1)
        end.to raise_error(Excon::Errors::NotFound)
      end
    end
  end

  describe "#subjects" do
    it "lists the subjects in the registry" do
      subjects = Array.new(2) { |n| "subject#{n}" }
      subjects.each { |subject| registry.register(subject, schema) }
      expect(registry.subjects).to be_json_eql(subjects.to_json)
    end
  end

  describe "#subject_versions" do
    it "lists all the versions for the subject" do
      2.times do |n|
        registry.register(subject_name,
                          { type: :record, name: "r#{n}", fields: [] }.to_json)
      end
      expect(registry.subject_versions(subject_name))
        .to be_json_eql((1..2).to_a.to_json)
    end

    context "when the subject does not exist" do
      let(:subject_name) { 'missing' }

      it "raises an error" do
        expect do
          registry.subject_versions(subject_name).inspect
        end.to raise_error(Excon::Errors::NotFound)
      end
    end
  end

  describe "#subject_version" do
    let!(:schema_id1) do
      registry.register(subject_name, { type: :record, name: "r0", fields: [] }.to_json)
    end
    let!(:schema_id2) do
      registry.register(subject_name, { type: :record, name: "r1", fields: [] }.to_json)
    end

    let(:expected) do
      {
        name: subject_name,
        version: 1,
        id: schema_id1,
        schema: { type: :record, name: "r0", fields: [] }.to_json
      }.to_json
    end

    it "returns a specific version of a schema" do
      expect(registry.subject_version(subject_name, 1))
        .to eq(JSON.parse(expected))
    end

    context "when the version is not specified" do
      let(:expected) do
        {
          name: subject_name,
          version: 2,
          id: schema_id2,
          schema: { type: :record, name: "r1", fields: [] }.to_json
        }.to_json
      end

      it "returns the latest version" do
        expect(registry.subject_version(subject_name))
          .to eq(JSON.parse(expected))
      end
    end

    context "when the subject does not exist" do
      it "raises an error" do
        expect do
          registry.subject_version('missing')
        end.to raise_error(Excon::Errors::NotFound)
      end
    end

    context "when the version does not exist" do
      it "raises an error" do
        expect do
          registry.subject_version(subject_name, 3)
        end.to raise_error(Excon::Errors::NotFound)
      end
    end
  end

  describe "#check" do
    context "when the schema exists" do
      let!(:schema_id) { registry.register(subject_name, schema) }
      let(:expected) do
        {
          subject: subject_name,
          id: schema_id,
          version: 1,
          schema: schema
        }.to_json
      end
      it "returns the schema details" do
        expect(registry.check(subject_name, schema)).to eq(JSON.parse(expected))
      end

      context "with an Avro::Schema" do
        let(:avro_schema) { Avro::Schema.parse(schema) }

        it "supports a check using an Avro schema" do
          expect(registry.check(subject_name, avro_schema)).to eq(JSON.parse(expected))
        end

        context "with ActiveSupport present" do
          before { break_to_json(avro_schema) }

          it "supports a check using an Avro schema" do
            expect(registry.check(subject_name, avro_schema)).to eq(JSON.parse(expected))
          end
        end
      end
    end

    context "when the schema is not registered" do
      it "returns nil" do
        expect(registry.check("missing", schema)).to be_nil
      end
    end
  end

  describe "#global_config" do
    let(:expected) do
      { compatibility: 'BACKWARD' }.to_json
    end

    it "returns the global configuration" do
      expect(registry.global_config).to eq(maybe_include_auth_header_in_expected_config(JSON.parse(expected), auth_mechanism))
    end
  end

  describe "#update_global_config" do
    let(:config) do
      { compatibility: 'FORWARD' }
    end
    let(:expected) { config.to_json }

    it "updates the global configuration and returns it" do
      expect(registry.update_global_config(config)).to eq(maybe_include_auth_header_in_expected_config(JSON.parse(expected), auth_mechanism))
      expect(registry.global_config).to eq(maybe_include_auth_header_in_expected_config(JSON.parse(expected), auth_mechanism))
    end
  end

  describe "#subject_config" do
    let(:expected) do
      { compatibility: 'BACKWARD' }.to_json
    end

    context "when the subject config is not set" do
      it "returns the global configuration" do
        expect(registry.subject_config(subject_name)).to eq(maybe_include_auth_header_in_expected_config(JSON.parse(expected), auth_mechanism))
      end
    end

    context "when the subject config is set" do
      let(:config) do
        { compatibility: 'FULL' }
      end
      let(:expected) { config.to_json }

      before do
        registry.update_subject_config(subject_name, config)
      end

      it "returns the subject config" do
        expect(registry.subject_config(subject_name)).to eq(JSON.parse(expected))
      end
    end
  end

  describe "#update_subject_config" do
    let(:config) do
      { compatibility: 'NONE' }
    end
    let(:expected) { config.to_json }

    it "updates the subject config and returns it" do
      expect(registry.update_subject_config(subject_name, config)).to eq(JSON.parse(expected))
      expect(registry.subject_config(subject_name)).to eq(JSON.parse(expected))
    end
  end

  describe "token refresh" do
    let(:expected) do
      { compatibility: 'BACKWARD' }.to_json
    end

    context "when the response is unauthorized" do
      it "tries to fetch a new authorization token 3 times" do
        skip('Not using oauth') if auth_mechanism != 'oauth'

        allow(logger).to receive(:info)
        expect{registry.subject_config('unauthorized')}.to raise_error(Excon::Error::Unauthorized)

        expect(logger).to have_received(:info).with('Encountered unauthorised response, will retry with fresh token (3 retries remaining)...').exactly(1).times
        expect(logger).to have_received(:info).with('Encountered unauthorised response, will retry with fresh token (2 retries remaining)...').exactly(1).times
        expect(logger).to have_received(:info).with('Encountered unauthorised response, will retry with fresh token (1 retries remaining)...').exactly(1).times

      end
    end
  end

  # Monkey patch an Avro::Schema to simulate the presence of
  # active_support/core_ext.
  def break_to_json(avro_schema)
    def avro_schema.to_json(*args)
      instance_variables.each_with_object(Hash.new) do |ivar, result|
        result[ivar.to_s.sub('@', '')] = instance_variable_get(ivar)
      end.to_json(*args)
    end
  end

  # Cheeky way to assert if the right auth header was passed when using oauth
  def maybe_include_auth_header_in_expected_config(config, auth_mechanism)
    config = config.merge("HTTP_AUTHORIZATION" => "Bearer #{oauth_token}") if auth_mechanism == "oauth"
    config
  end
end
