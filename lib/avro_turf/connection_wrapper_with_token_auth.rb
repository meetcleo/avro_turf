# frozen_string_literal: true

require 'connection_pool'
class AvroTurf::ConnectionWrapperWithAuthToken < AvroTurf::ConnectionWrapper
  REFRESH_TOKEN_TRIES = 4
  CONNECTION_POOL_SIZE = 1

  def initialize(url,
                 logger:,
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
                 connection_pool_size: nil)
    @oauth_url = oauth_url
    @oauth_client_id = oauth_client_id
    @oauth_client_secret = oauth_client_secret
    @semaphore = Mutex.new
    @logger = logger
    @refresh_token_retries_remaining = REFRESH_TOKEN_TRIES
    @connection_pool_size = connection_pool_size || CONNECTION_POOL_SIZE

    super(url,
      logger: logger,
      proxy: proxy,
      user: user,
      password: password,
      ssl_ca_file: ssl_ca_file,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass,
      client_cert_data: client_cert_data,
      client_key_data: client_key_data)
  end

  def with_connection
    refresh_token if token_needs_refresh?
    result = connection_pool.with do |conn|
      yield conn
    end
    @refresh_token_retries_remaining = REFRESH_TOKEN_TRIES
    result
  rescue Excon::Error::Unauthorized
    raise if refresh_token_retries_remaining < 1

    logger.info("Encountered unauthorised response, will retry with fresh token (#{refresh_token_retries_remaining} retries remaining)...")
    refresh_token
    retry
  end

  private

  def refresh_token
    logger.info('Waiting to refresh auth token...')
    semaphore.synchronize do
      @refresh_token_retries_remaining -= 1
      logger.info("Checking if auth token needs refresh (current token set to expire at #{token_expires_at})...")
      return unless token_needs_refresh?

      logger.info('Auth token needs refresh. Refreshing...')
      current_time_utc = Time.now.utc
      refresh_uri = URI.parse(oauth_url)
      refresh_connection = Excon.new(
        refresh_uri.to_s.chomp(refresh_uri.path),
        headers: refresh_token_header
      )
      response = refresh_connection.post(path: refresh_uri.path, expects: 200, body: 'grant_type=client_credentials')
      json_response = JSON.parse(response.body)
      @token = json_response['access_token']
      @token_expires_at = current_time_utc + json_response['expires_in'].to_i
      remove_instance_variable(:@connection) if instance_variable_defined?(:@connection)
      logger.info("Auth token refreshed (new token set to expire at #{token_expires_at}).")
    end
  rescue StandardError => e
    logger.error('Exception whilst refreshing token:')
    logger.error(e)
  end

  def token_needs_refresh?
    token.nil? || token_expires_at.nil? || Time.now.utc > token_expires_at
  end

  def headers
    super.merge('Authorization' => "Bearer #{token}")
  end

  def base64_refresh_auth
    Base64.strict_encode64("#{oauth_client_id}:#{oauth_client_secret}")
  end

  def refresh_token_header
    {
      'Authorization' => "Basic #{base64_refresh_auth}",
      'Content-Type' => 'application/x-www-form-urlencoded'
    }
  end

  def connection_pool
    @connection_pool ||= ConnectionPool.new(size: connection_pool_size) do
      connection
    end
  end

  attr_reader :oauth_url, :oauth_client_id, :oauth_client_secret, :token_expires_at, :semaphore, :token,
              :refresh_token_retries_remaining, :connection_pool_size
end
