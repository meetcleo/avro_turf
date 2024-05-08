require 'avro_turf/connection_manager'

class AvroTurf::ConfluentSchemaRegistry
  def initialize(
    url,
    logger: Logger.new($stdout),
    proxy: nil,
    user: nil,
    password: nil,
    ssl_ca_file: nil,
    client_cert: nil,
    client_key: nil,
    client_key_pass: nil,
    client_cert_data: nil,
    client_key_data: nil,
    oauth_url: nil,
    oauth_client_id: nil,
    oauth_client_secret: nil,
    path_prefix: nil,
    connection_pool_size: nil,
    tcp_nodelay: nil,
    persistent_connection: nil
  )
    @path_prefix = path_prefix
    @logger = logger
    @connection_manager = ::AvroTurf::ConnectionManager.new(
      url,
      logger: logger,
      proxy: proxy,
      user: user,
      password: password,
      ssl_ca_file: ssl_ca_file,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass,
      client_cert_data: client_cert_data,
      client_key_data: client_key_data,
      oauth_url: oauth_url,
      oauth_client_id: oauth_client_id,
      oauth_client_secret: oauth_client_secret,
      connection_pool_size: connection_pool_size,
      tcp_nodelay: tcp_nodelay,
      persistent_connection: persistent_connection
    )
  end

  RETRY_ERRORS = [
    Excon::Error::Timeout,
    Excon::Error::Socket,
    Excon::Error::BadGateway,
    Excon::Error::ServiceUnavailable,
    Excon::Error::GatewayTimeout
  ].freeze
  private_constant :RETRY_ERRORS

  def retry_options
    {
      idempotent: true,
      retry_errors: RETRY_ERRORS
    }
  end

  def fetch(id)
    @logger.info "Fetching schema with id #{id}"
    data = get("/schemas/ids/#{id}")
    data.fetch('schema')
  end

  def register(subject, schema)
    data = post("/subjects/#{subject}/versions", body: { schema: schema.to_s }.to_json)

    id = data.fetch('id')

    @logger.info "Registered schema for subject `#{subject}`; id = #{id}"

    id
  end

  # List all subjects
  def subjects
    get('/subjects')
  end

  # List all versions for a subject
  def subject_versions(subject)
    get("/subjects/#{subject}/versions")
  end

  # Get a specific version for a subject
  def subject_version(subject, version = 'latest')
    get("/subjects/#{subject}/versions/#{version}")
  end

  # Check if a schema exists. Returns nil if not found.
  def check(subject, schema)
    data = post("/subjects/#{subject}",
                expects: [200, 404],
                body: { schema: schema.to_s }.to_json)
    data unless data.has_key?('error_code')
  end

  # Check if a schema is compatible with the stored version.
  # Returns:
  # - true if compatible
  # - nil if the subject or version does not exist
  # - false if incompatible
  # http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#compatibility
  def compatible?(subject, schema, version = 'latest')
    data = post("/compatibility/subjects/#{subject}/versions/#{version}",
                expects: [200, 404], body: { schema: schema.to_s }.to_json, **retry_options)
    data.fetch('is_compatible', false) unless data.has_key?('error_code')
  end

  # Get global config
  def global_config
    get('/config')
  end

  # Update global config
  def update_global_config(config)
    put('/config', body: config.to_json)
  end

  # Get config for subject
  def subject_config(subject)
    get("/config/#{subject}")
  end

  # Update config for subject
  def update_subject_config(subject, config)
    put("/config/#{subject}", body: config.to_json)
  end

  # Delete all versions for a subject
  def delete_subject(subject)
    delete("/subjects/#{subject}")
  end

  private

  def get(path, **options)
    options.merge!(retry_options)
    request(path, method: :get, **options)
  end

  def put(path, **options)
    request(path, method: :put, **options)
  end

  def post(path, **options)
    request(path, method: :post, **options)
  end

  def delete(path, **options)
    request(path, method: :delete, **options)
  end

  def request(path, **options)
    @connection_manager.with_connection do |connection|
      options = { expects: 200 }.merge!(options)
      path = File.join(@path_prefix, path) unless @path_prefix.nil?
      response = connection.request(path: path, **options)
      JSON.parse(response.body)
    end
  end
end
