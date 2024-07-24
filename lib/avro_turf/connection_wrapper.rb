# frozen_string_literal: true

require 'excon'
require 'connection_pool'

class AvroTurf::ConnectionWrapper
  CONTENT_TYPE = 'application/vnd.schemaregistry.v1+json'
  CONNECTION_POOL_SIZE = 1
  TCP_NODELAY = true
  PERSISTENT_CONNECTION = true

  def initialize(
    url,
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
    connection_pool_size: nil,
    tcp_nodelay: nil,
    persistent_connection: nil,
    connect_timeout: nil,
    read_timeout: nil,
    write_timeout: nil,
    instrumentor: nil
  )
    @logger = logger
    @proxy = proxy
    @url = url
    @user = user
    @password = password
    @ssl_ca_file = ssl_ca_file
    @client_cert = client_cert
    @client_key = client_key
    @client_key_pass = client_key_pass
    @client_cert_data = client_cert_data
    @client_key_data = client_key_data
    @connection_pool_size = connection_pool_size || CONNECTION_POOL_SIZE
    @tcp_nodelay = tcp_nodelay.nil? ? TCP_NODELAY : tcp_nodelay
    @persistent_connection = persistent_connection.nil? ? PERSISTENT_CONNECTION : persistent_connection
    @connect_timeout = connect_timeout
    @read_timeout = read_timeout
    @write_timeout = write_timeout
    @instrumentor = instrumentor
  end

  def with_connection
    connection_pool.with do |conn|
      yield conn
    end
  end

  def request(path:, **options)
    with_connection do |conn|
      options = { headers: headers }.merge!(options)
      conn.request(path: path, **options)
    end
  end

  attr_reader :connection_pool

  private

  def headers
    headers = {
      'Content-Type' => CONTENT_TYPE
    }
    headers[:proxy] = proxy unless proxy.nil?
    headers
  end

  def connection
    options = {
      headers: headers,
      user: user,
      tcp_nodelay: tcp_nodelay,
      persistent: persistent_connection,
      password: password,
      ssl_ca_file: ssl_ca_file,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass,
      client_cert_data: client_cert_data,
      client_key_data: client_key_data,
      connect_timeout: connect_timeout,
      read_timeout: read_timeout,
      write_timeout: write_timeout
    }
    options[:instrumentor] = instrumentor if instrumentor
    Excon.new(
      url,
      options
    )
  end

  def connection_pool
    @connection_pool ||= ConnectionPool.new(size: connection_pool_size) do
      connection
    end
  end
  attr_reader :logger, :proxy, :url, :user, :password, :ssl_ca_file, :client_cert, :client_key, :client_key_pass,
              :client_cert_data, :client_key_data, :connection_pool_size, :tcp_nodelay, :persistent_connection,
              :connect_timeout, :read_timeout, :write_timeout, :instrumentor
end
