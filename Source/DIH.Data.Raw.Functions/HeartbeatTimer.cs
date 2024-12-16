using DIH.Common;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration.AzureAppConfiguration;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions
{
    public class HeartbeatTimer
    {
        private DihLogger<HeartbeatTimer> Logger { get; set; }
        private IConfigurationRefresherProvider ConfigurationRefresherProvider { get; set; }

        public HeartbeatTimer(ILogger<HeartbeatTimer> logger, IConfigurationRefresherProvider configurationRefresherProvider)
        {
            Logger = logger.AsDihLogger();
            ConfigurationRefresherProvider = configurationRefresherProvider;
        }

        [FunctionName("dataraw-heartbeat-timer")]
        public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer)
        {
            try
            {
                // This timer has two tasks:
                // - Keep app alive at intervals so queues are checked... 
                // - Refresh configuration
                Logger.DihDebug($"TimerTrigger called... ");
                await ConfigurationRefresherProvider.Refreshers.First().TryRefreshAsync();
            }
            catch (Exception e)
            {
                Logger.DihError(e);
                throw;
            }
        }
    }
}

