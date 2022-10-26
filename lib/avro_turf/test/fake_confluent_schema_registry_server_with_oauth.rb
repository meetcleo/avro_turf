class FakeConfluentSchemaRegistryServerWithOauth < FakeConfluentSchemaRegistryServer
  helpers do
    def global_config
      # Snatch the auth header and return it with the global config so we can check it was provided
      self.class.global_config.merge!(request.env.slice('HTTP_AUTHORIZATION'))
      self.class.global_config
    end
  end

  get "/config/:subject" do
    # Make requesting an unauthorized subject name trigger a 401 response to help us test token refresh
    halt(401, {}) if params[:subject] == 'unauthorized'
    CONFIGS.fetch(params[:subject], global_config).to_json
  end
end
