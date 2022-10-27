require 'avro_turf/connection_wrapper'
require 'avro_turf/connection_wrapper_with_token_auth'

class AvroTurf::ConnectionManager
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
                 oauth_client_secret: nil)
    @logger = logger

    @connection_wrapper = if [oauth_client_id, oauth_client_secret].none?(&:nil?)
                            ::AvroTurf::ConnectionWrapperWithAuthToken.new(
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
                              oauth_client_secret: oauth_client_secret
                            )
                          else
                            ::AvroTurf::ConnectionWrapper.new(
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
                              client_key_data: client_key_data
                            )
                          end
    logger.debug("#{self.class.name}: using #{connection_wrapper.class.name} connection wrapper")
  end

  def with_connection(&block)
    connection_wrapper.with_connection(&block)
  end

  private

  attr_reader :logger, :connection_wrapper
end
