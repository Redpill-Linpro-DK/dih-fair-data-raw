using DIH.Common.Configuration;
using DIH.Data.Raw.Functions.Helpers;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

[assembly: FunctionsStartup(typeof(DIH.Data.Raw.Functions.Startup))]
namespace DIH.Data.Raw.Functions
{
    public class Startup : FunctionsStartup
    {
        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
            builder.UseDihConfiguration();
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.UseDihDataRawServices();

            // Message handler manager
            builder.Services.AddSingleton<MessageHandlerHelper, MessageHandlerHelper>();

            // Make Azure App Configuration services available
            builder.Services.AddAzureAppConfiguration();
        }
    }
}
