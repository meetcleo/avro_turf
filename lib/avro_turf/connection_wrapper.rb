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
    persistent_connection: nil
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
    @tcp_nodelay = tcp_nodelay || TCP_NODELAY
    @persistent_connection = persistent_connection || PERSISTENT_CONNECTION
  end

  def with_connection
    connection_pool.with do |conn|
      yield conn
    end
  end

  attr_reader :connection

  private

  def headers
    headers = {
      'Content-Type' => CONTENT_TYPE
    }
    headers[:proxy] = proxy unless proxy.nil?
    headers
  end

  def connection
    Excon.new(
      url,
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
      client_key_data: client_key_data
    )
  end

  def connection_pool
    @connection_pool ||= ConnectionPool.new(size: connection_pool_size) do
      connection
    end
  end
  attr_reader :logger, :proxy, :url, :user, :password, :ssl_ca_file, :client_cert, :client_key, :client_key_pass,
              :client_cert_data, :client_key_data, :connection_pool_size
end
