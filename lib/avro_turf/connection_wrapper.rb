# frozen_string_literal: true

require 'excon'

class AvroTurf::ConnectionWrapper
  CONTENT_TYPE = 'application/vnd.schemaregistry.v1+json'

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
    client_key_data: nil
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
  end

  def with_connection
    yield connection
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
    @connection ||= Excon.new(
      url,
      headers: headers,
      user: user,
      tcp_nodelay: true,
      persistent: true,
      password: password,
      ssl_ca_file: ssl_ca_file,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass,
      client_cert_data: client_cert_data,
      client_key_data: client_key_data
    )
  end

  attr_reader :logger, :proxy, :url, :user, :password, :ssl_ca_file, :client_cert, :client_key, :client_key_pass,
              :client_cert_data, :client_key_data
end
